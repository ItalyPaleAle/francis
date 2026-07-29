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
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/host/remote"
	"github.com/italypaleale/francis/internal/builtinactor"
	runtimepkg "github.com/italypaleale/francis/internal/runtime"
	"github.com/italypaleale/francis/tests/integration/framework/process"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/netfault"
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
	// BuiltInActors are framework-managed actors registered on every host via RegisterBuiltInActor
	BuiltInActors []builtinactor.BuiltInActor
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
	// HostHealthCheckDeadline optionally shortens how long a host registration survives without a health check
	// Failure scenarios set it to a few seconds so a host that dies silently is expired quickly instead of after the twenty-second default
	// It is applied to the provider, to each local host, and to the runtime on the remote topology, which must all agree on it
	HostHealthCheckDeadline time.Duration
	// AlarmsLeaseDuration optionally shortens how long a host holds the lease on an alarm it is executing, so a scenario that kills the executing host does not wait out the default lease before another host can take over
	// Like the health check deadline it is applied to the provider and to each local host
	AlarmsLeaseDuration time.Duration
	// ProviderQueryTimeout optionally shortens the timeout the provider applies to a single database query, so a database that has stopped answering surfaces as an error quickly
	ProviderQueryTimeout time.Duration
	// HealthCheckPolicy optionally shortens how hosts retry their health checks, which is what sets the floor under HostHealthCheckDeadline
	// Without it a failure scenario cannot go below the twelve seconds the default policy needs, and every wait on a registration expiring is that much longer
	HealthCheckPolicy components.HealthCheckPolicy
	// HostRequestTimeout optionally shortens how long a host waits on a provider request, a runtime request, or a peer dial, so a severed link fails fast instead of hanging on the default timeouts
	HostRequestTimeout time.Duration
	// StallableProvider makes the backend give each host, or each runtime on the remote topology, its own database handle that StallProvider can choke on demand
	// Only the SQLite variant supports it
	StallableProvider bool
	// RuntimeLinks puts a severable network link in front of the runtime for each host, so a scenario can cut one host off from the control plane while leaving the runtime and the other hosts running
	// It only applies to the remote topology, and because each host gets its own link it requires the single-runtime default
	RuntimeLinks bool
	// PeerLinks puts a severable network link in front of each host's peer server, so a scenario can cut host-to-host traffic to one host while leaving it connected to the provider or the runtime
	PeerLinks bool
}

// Cluster is an assembled topology, exposing its processes and host services
type Cluster struct {
	backend  provider.Backend
	runtimes []*frameworkruntime.Runtime
	hosts    []frameworkhost.Instance
	procs    []process.Interface
	// runtimeLinks and peerLinks are indexed by host, and hold the severable link standing in front of that host's runtime and of its own peer server
	runtimeLinks []*netfault.Link
	peerLinks    []*netfault.Link
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

	backend := provider.New(opts.Variant, provider.Options{
		HostHealthCheckDeadline: opts.HostHealthCheckDeadline,
		AlarmsLeaseDuration:     opts.AlarmsLeaseDuration,
		QueryTimeout:            opts.ProviderQueryTimeout,
		HealthCheck:             opts.HealthCheckPolicy,
		Stallable:               opts.StallableProvider,
	})
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

	// A local host owns its own copy of everything the runtime owns on the remote topology, so alarm polling and every timeout override are applied per host
	var hostExtra []local.HostOption
	if opts.AlarmsPollInterval > 0 {
		hostExtra = append(hostExtra, local.WithAlarmsPollInterval(opts.AlarmsPollInterval))
	}
	if opts.AlarmsLeaseDuration > 0 {
		hostExtra = append(hostExtra, local.WithAlarmsLeaseDuration(opts.AlarmsLeaseDuration))
	}
	if opts.HostHealthCheckDeadline > 0 {
		hostExtra = append(hostExtra, local.WithHostHealthCheckDeadline(opts.HostHealthCheckDeadline))
	}
	if opts.HostRequestTimeout > 0 {
		hostExtra = append(hostExtra, local.WithProviderRequestTimeout(opts.HostRequestTimeout))
	}
	hostExtra = append(hostExtra, local.WithHealthCheckPolicy(&opts.HealthCheckPolicy))

	// A host with a peer link in front of it binds to one port and advertises the link's, so peers reach it only through the link
	hostPorts, linkPorts := c.reserveHostPorts(t, opts)
	for i := range opts.Hosts {
		address, bind := addr(hostPorts[i]), ""
		if opts.PeerLinks {
			address, bind = addr(linkPorts[i]), addr(hostPorts[i])
			c.addPeerLink(address, bind)
		}

		h := frameworkhost.NewLocal(frameworkhost.LocalOptions{
			Address:       address,
			Bind:          bind,
			Backend:       c.backend,
			Actors:        opts.Actors,
			BuiltInActors: opts.BuiltInActors,
			Extra:         hostExtra,
		})
		c.hosts[i] = h
		c.procs = append(c.procs, h)
	}
}

// reserveHostPorts reserves a port per host, plus one per peer link when the scenario asked for them
func (c *Cluster) reserveHostPorts(t *testing.T, opts Options) (hostPorts []int, linkPorts []int) {
	t.Helper()

	if !opts.PeerLinks {
		return ports.Reserve(t, opts.Hosts), nil
	}

	p := ports.Reserve(t, 2*opts.Hosts)
	return p[:opts.Hosts], p[opts.Hosts:]
}

// addPeerLink registers a severable link that carries peer traffic from the advertised address to the host's real bind address
func (c *Cluster) addPeerLink(address string, bind string) {
	l := netfault.New(address, bind)
	c.peerLinks = append(c.peerLinks, l)
	// The link is started before the host so a peer can reach it as soon as the host is ready
	c.procs = append(c.procs, l)
}

// buildRemote wires one or more runtimes that own a shared provider, plus stateless hosts that connect to them
func (c *Cluster) buildRemote(t *testing.T, opts Options) {
	t.Helper()

	replicas := max(opts.RuntimeReplicas, 1)
	if replicas > 1 {
		require.True(t, opts.Variant.SharedStore(), "variant %q cannot back multiple runtime replicas", opts.Variant)
	}

	// Each host gets its own link to the control plane, so cutting one host off leaves the others connected, which only makes sense against a single runtime
	if opts.RuntimeLinks {
		require.Equal(t, 1, replicas, "runtime links require the single-runtime default, since each host reaches the runtime through its own link")
	}

	// Reserve a port per runtime replica, ahead of the host ports, plus one per link the scenario asked for
	extraPorts := 0
	if opts.RuntimeLinks {
		extraPorts += opts.Hosts
	}
	if opts.PeerLinks {
		extraPorts += opts.Hosts
	}
	p := ports.Reserve(t, opts.Hosts+replicas+extraPorts)
	runtimeAddrs := make([]string, replicas)
	for i := range replicas {
		runtimeAddrs[i] = addr(p[i])
	}
	hostPorts := p[replicas : replicas+opts.Hosts]
	extra := p[replicas+opts.Hosts:]

	// On the remote topology the runtime owns alarm polling and host health tracking, so those overrides are applied there instead of on the hosts
	var runtimeExtra []runtimepkg.RuntimeOption
	if opts.AlarmsPollInterval > 0 {
		runtimeExtra = append(runtimeExtra, runtimepkg.WithAlarmsPollInterval(opts.AlarmsPollInterval))
	}
	if opts.HostHealthCheckDeadline > 0 {
		runtimeExtra = append(runtimeExtra, runtimepkg.WithHostHealthCheckDeadline(opts.HostHealthCheckDeadline))
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

	// Hosts get shorter request timeouts when asked, so a request into a black hole fails fast rather than hanging on the default
	var hostExtra []remote.HostOption
	if opts.HostRequestTimeout > 0 {
		hostExtra = append(hostExtra, remote.WithRequestTimeout(opts.HostRequestTimeout))
	}
	hostExtra = append(hostExtra, remote.WithHealthCheckPolicy(&opts.HealthCheckPolicy))

	for i := range opts.Hosts {
		// When JWT bootstrap is configured, each host presents a token whose subject identifies it
		var token string
		if opts.BootstrapJWT != nil {
			var err error
			token, err = opts.BootstrapJWT.Token("host-"+strconv.Itoa(i), time.Hour)
			require.NoError(t, err, "failed to mint host bootstrap token")
		}

		// A host with a runtime link reaches the control plane only through it, so cutting that link isolates this host alone
		hostRuntimeAddrs := runtimeAddrs
		if opts.RuntimeLinks {
			linkAddr := addr(extra[0])
			extra = extra[1:]
			l := netfault.New(linkAddr, runtimeAddrs[0])
			c.runtimeLinks = append(c.runtimeLinks, l)
			c.procs = append(c.procs, l)
			hostRuntimeAddrs = []string{linkAddr}
		}

		// A host with a peer link in front of it binds to one port and advertises the link's, so peers reach it only through the link
		address, bind := addr(hostPorts[i]), ""
		if opts.PeerLinks {
			address, bind = addr(extra[0]), addr(hostPorts[i])
			extra = extra[1:]
			c.addPeerLink(address, bind)
		}

		// Hosts know every replica address and roll over to a survivor when one goes away
		h := frameworkhost.NewRemote(frameworkhost.RemoteOptions{
			Address:          address,
			Bind:             bind,
			RuntimeAddresses: hostRuntimeAddrs,
			BootstrapToken:   token,
			Actors:           opts.Actors,
			BuiltInActors:    opts.BuiltInActors,
			Extra:            hostExtra,
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

// RuntimeLink returns the severable link carrying the i-th host's traffic to the runtime
// It is only present when the cluster was built with RuntimeLinks on the remote topology
func (c *Cluster) RuntimeLink(t *testing.T, i int) *netfault.Link {
	t.Helper()
	require.Less(t, i, len(c.runtimeLinks), "the cluster was not built with a runtime link for host %d", i)
	return c.runtimeLinks[i]
}

// PeerLink returns the severable link standing in front of the i-th host's peer server, which carries every invocation other hosts send it
// It is only present when the cluster was built with PeerLinks
func (c *Cluster) PeerLink(t *testing.T, i int) *netfault.Link {
	t.Helper()
	require.Less(t, i, len(c.peerLinks), "the cluster was not built with a peer link for host %d", i)
	return c.peerLinks[i]
}

// StallProvider makes every provider call the i-th host makes block, simulating a database that has gone unavailable for that host alone
// On the remote topology, where the runtime owns the provider, the index selects a runtime replica instead
// It requires a cluster built with StallableProvider
func (c *Cluster) StallProvider(t *testing.T, i int) {
	t.Helper()
	c.stallable(t).Stall(t, i)
}

// UnstallProvider lets the i-th host's provider calls through again, and is idempotent
func (c *Cluster) UnstallProvider(t *testing.T, i int) {
	t.Helper()
	c.stallable(t).Unstall(t, i)
}

// stallable returns the backend as a Stallable, failing the scenario if it was not built to support stalling
func (c *Cluster) stallable(t *testing.T) provider.Stallable {
	t.Helper()

	s, ok := c.backend.(provider.Stallable)
	require.True(t, ok, "variant %q cannot simulate a database outage", c.backend.Variant())
	return s
}

// addr formats a loopback address for the given port
func addr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
