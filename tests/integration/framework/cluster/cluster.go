//go:build integration

// Package cluster assembles a test topology of hosts and, for the remote runtime, a control-plane runtime, all sharing one provider backend
//
// A scenario picks a Kind (local or remote) and a provider variant, and the cluster wires up the right processes so the same scenario body can run against both runtimes
package cluster

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin"
	"github.com/italypaleale/francis/host/local"
	runtimepkg "github.com/italypaleale/francis/runtime"
	"github.com/italypaleale/francis/tests/integration/framework/process"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/ports"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	frameworkruntime "github.com/italypaleale/francis/tests/integration/framework/process/runtime"
)

// Kind selects the runtime topology
type Kind string

const (
	// Local embeds the provider in each host
	Local Kind = "local"
	// Remote runs a standalone runtime that owns the provider, with stateless hosts connecting to it
	Remote Kind = "remote"
)

// Options configures a cluster
type Options struct {
	// Kind selects the local or remote topology
	Kind Kind
	// Variant selects the provider backend
	Variant provider.Variant
	// Hosts is the number of actor hosts to start, and must be at least one
	Hosts int
	// Actors are registered on every host before it starts
	Actors []frameworkhost.ActorReg
	// BuiltInActors are framework-managed actors registered on every host via WithBuiltInActor
	BuiltInActors []*builtin.BuiltInActor
	// AlarmsPollInterval optionally tunes how frequently alarms are polled, so alarm scenarios fire quickly instead of waiting on the multi-second component defaults
	// On the local topology it is applied to each host, and on the remote topology to the runtime that owns alarm execution, so the same value speeds up either topology
	// Zero leaves the component default in place
	AlarmsPollInterval time.Duration
	// BootstrapJWT, when set, makes the remote topology authenticate joining hosts with a JWT instead of the shared host PSK
	// It only applies to the remote topology, where hosts bootstrap against a runtime (the local topology self-issues from the runtime PSK and ignores it)
	BootstrapJWT *clustersecret.JWTBootstrap
	// RuntimeReplicas runs more than one runtime against the same shared store on the remote topology, so a scenario can stop one and watch hosts roll over to a survivor
	// Zero or one keeps the single-runtime default
	// The value is ignored on the local topology, which has no standalone runtime
	// Replicas require a store the runtimes can share, so a variant whose provider is not shareable across processes is rejected
	RuntimeReplicas int
}

// Cluster is an assembled topology, exposing its processes and host services
type Cluster struct {
	backend  provider.Backend
	runtimes []*frameworkruntime.Runtime
	hosts    []frameworkhost.Instance
	procs    []process.Interface
}

// New assembles a cluster for the given options
// It does not start anything: pass Processes to framework.Run, which starts them in order and tears them down in reverse
func New(t *testing.T, opts Options) *Cluster {
	t.Helper()
	require.GreaterOrEqual(t, opts.Hosts, 1, "a cluster needs at least one host")

	// Standalone providers coordinate nothing across processes, so the local topology cannot share them across hosts
	if opts.Kind == Local && opts.Hosts > 1 {
		require.True(t, opts.Variant.LocalMultiHost(), "variant %q cannot back multiple local hosts", opts.Variant)
	}

	backend := provider.New(opts.Variant)
	c := &Cluster{
		backend: backend,
		hosts:   make([]frameworkhost.Instance, opts.Hosts),
	}

	// The backend is started first so its store is ready before any host or runtime uses it
	c.procs = append(c.procs, backend)

	switch opts.Kind {
	case Local:
		c.buildLocal(t, opts)
	case Remote:
		c.buildRemote(t, opts)
	default:
		t.Fatalf("unknown cluster kind %q", opts.Kind)
	}

	return c
}

// buildLocal wires one provider-embedding host per requested host
func (c *Cluster) buildLocal(t *testing.T, opts Options) {
	t.Helper()

	// Each local host owns alarm polling, so the poll interval is applied per host
	var hostExtra []local.HostOption
	if opts.AlarmsPollInterval > 0 {
		hostExtra = append(hostExtra, local.WithAlarmsPollInterval(opts.AlarmsPollInterval))
	}

	hostPorts := ports.Reserve(t, opts.Hosts)
	for i := range opts.Hosts {
		h := frameworkhost.NewLocal(frameworkhost.LocalOptions{
			Address:       addr(hostPorts[i]),
			Backend:       c.backend,
			Actors:        opts.Actors,
			BuiltInActors: opts.BuiltInActors,
			Extra:         hostExtra,
		})
		c.hosts[i] = h
		c.procs = append(c.procs, h)
	}
}

// buildRemote wires one or more runtimes that own a shared provider, plus stateless hosts that connect to them
func (c *Cluster) buildRemote(t *testing.T, opts Options) {
	t.Helper()

	replicas := max(opts.RuntimeReplicas, 1)
	if replicas > 1 {
		require.True(t, opts.Variant.SharedStore(), "variant %q cannot back multiple runtime replicas", opts.Variant)
	}

	// Reserve a port per runtime replica, ahead of the host ports
	p := ports.Reserve(t, opts.Hosts+replicas)
	runtimeAddrs := make([]string, replicas)
	for i := range replicas {
		runtimeAddrs[i] = addr(p[i])
	}
	hostPorts := p[replicas:]

	// On the remote topology the runtime owns alarm polling, so the poll interval is applied there instead of on the hosts
	var runtimeExtra []runtimepkg.RuntimeOption
	if opts.AlarmsPollInterval > 0 {
		runtimeExtra = append(runtimeExtra, runtimepkg.WithAlarmsPollInterval(opts.AlarmsPollInterval))
	}

	// Each replica owns its own provider against the shared store, so any of them can serve the hosts
	c.runtimes = make([]*frameworkruntime.Runtime, replicas)
	for i := range replicas {
		rt := frameworkruntime.New(frameworkruntime.Options{
			Bind:         runtimeAddrs[i],
			Backend:      c.backend,
			BootstrapJWT: opts.BootstrapJWT,
			Extra:        runtimeExtra,
		})
		c.runtimes[i] = rt
		c.procs = append(c.procs, rt)
	}

	for i := range opts.Hosts {
		// When JWT bootstrap is configured, each host presents a token whose subject identifies it
		var token string
		if opts.BootstrapJWT != nil {
			var err error
			token, err = opts.BootstrapJWT.Token("host-"+strconv.Itoa(i), time.Hour)
			require.NoError(t, err, "failed to mint host bootstrap token")
		}

		// Hosts know every replica address and roll over to a survivor when one goes away
		h := frameworkhost.NewRemote(frameworkhost.RemoteOptions{
			Address:          addr(hostPorts[i]),
			RuntimeAddresses: runtimeAddrs,
			BootstrapToken:   token,
			Actors:           opts.Actors,
			BuiltInActors:    opts.BuiltInActors,
		})
		c.hosts[i] = h
		c.procs = append(c.procs, h)
	}
}

// Processes returns the processes that make up the cluster, in start order
func (c *Cluster) Processes() []process.Interface {
	return c.procs
}

// Runtime returns the i-th runtime replica on the remote topology, or nil on the local topology where each host embeds its own provider
func (c *Cluster) Runtime(i int) *frameworkruntime.Runtime {
	if i < 0 || i >= len(c.runtimes) {
		return nil
	}
	return c.runtimes[i]
}

// Host returns the i-th host
func (c *Cluster) Host(i int) frameworkhost.Instance {
	return c.hosts[i]
}

// Service returns the actor service of the i-th host
func (c *Cluster) Service(i int) *actor.Service {
	return c.hosts[i].Service()
}

// Len returns the number of hosts
func (c *Cluster) Len() int {
	return len(c.hosts)
}

// addr formats a loopback address for the given port
func addr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
