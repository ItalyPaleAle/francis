//go:build integration

// Package cluster assembles a test topology of hosts and, for the remote runtime, a control-plane runtime, all sharing one provider backend
//
// A scenario picks a Kind (local or remote) and a provider variant, and the cluster wires up the right processes so the same scenario body can run against both runtimes
package cluster

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/tests/integration/framework/process"
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
}

// Cluster is an assembled topology, exposing its processes and host services
type Cluster struct {
	backend provider.Backend
	runtime *frameworkruntime.Runtime
	hosts   []frameworkhost.Instance
	procs   []process.Interface
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

	hostPorts := ports.Reserve(t, opts.Hosts)
	for i := range opts.Hosts {
		h := frameworkhost.NewLocal(frameworkhost.LocalOptions{
			Address: addr(hostPorts[i]),
			Backend: c.backend,
			Actors:  opts.Actors,
		})
		c.hosts[i] = h
		c.procs = append(c.procs, h)
	}
}

// buildRemote wires one runtime that owns the provider, plus stateless hosts that connect to it
func (c *Cluster) buildRemote(t *testing.T, opts Options) {
	t.Helper()

	// Reserve one extra port for the runtime, ahead of the host ports
	p := ports.Reserve(t, opts.Hosts+1)
	runtimeAddr := addr(p[0])
	hostPorts := p[1:]

	c.runtime = frameworkruntime.New(frameworkruntime.Options{
		Bind:    runtimeAddr,
		Backend: c.backend,
	})
	c.procs = append(c.procs, c.runtime)

	for i := range opts.Hosts {
		h := frameworkhost.NewRemote(frameworkhost.RemoteOptions{
			Address:          addr(hostPorts[i]),
			RuntimeAddresses: []string{runtimeAddr},
			Actors:           opts.Actors,
		})
		c.hosts[i] = h
		c.procs = append(c.procs, h)
	}
}

// Processes returns the processes that make up the cluster, in start order
func (c *Cluster) Processes() []process.Interface {
	return c.procs
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
