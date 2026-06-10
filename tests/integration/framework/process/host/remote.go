//go:build integration

package host

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/remote"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	"github.com/italypaleale/francis/tests/integration/framework/process/ports"
)

// RemoteOptions configures a remote host process
type RemoteOptions struct {
	// Address the host binds to and advertises to peers and the runtime, e.g. "127.0.0.1:7571"
	Address string
	// RuntimeAddresses are the runtime replicas this host connects to
	RuntimeAddresses []string
	// Actors to register before the host starts
	Actors []ActorReg
	// Logger is optional and defaults to the host's discarding logger
	Logger *slog.Logger
	// Extra host options applied last, e.g. custom timeouts
	Extra []remote.HostOption
}

// Remote is a remote actor host managed as a framework process
// It connects to a standalone runtime rather than embedding a provider
type Remote struct {
	opts    RemoteOptions
	h       *remote.Host
	runErrC chan error
	cancel  context.CancelFunc
}

// NewRemote returns a remote host process that is started by Run
func NewRemote(opts RemoteOptions) *Remote {
	return &Remote{
		opts:    opts,
		runErrC: make(chan error, 1),
	}
}

func (p *Remote) Service() *actor.Service {
	return p.h.Service()
}

func (p *Remote) HostID() string {
	return p.h.HostID()
}

func (p *Remote) Address() string {
	return p.opts.Address
}

func (p *Remote) Run(t *testing.T) {
	t.Helper()

	// Assemble the host options, pointing the host at the runtime replicas
	hostOpts := []remote.HostOption{
		remote.WithAddress(p.opts.Address),
		remote.WithRuntimeAddresses(p.opts.RuntimeAddresses...),
		// The host bootstraps with the shared host PSK
		// Once registered it holds a workload cert and reconnects over mTLS
		remote.WithHostBootstrapPSK(clustersecret.HostBootstrapPSK),
		// Tests trust the runtime on first connection rather than pinning its CA
		remote.WithUnsafeNoPinnedCA(),
		remote.WithShutdownGracePeriod(ShutdownGrace),
	}
	if p.opts.Logger != nil {
		hostOpts = append(hostOpts, remote.WithLogger(p.opts.Logger))
	}
	hostOpts = append(hostOpts, p.opts.Extra...)

	h, err := remote.NewHost(hostOpts...)
	require.NoError(t, err, "failed to create remote host")
	p.h = h

	// Actors must be registered before Run
	for _, a := range p.opts.Actors {
		require.NoError(t, h.RegisterActor(a.Type, a.Factory, a.Opts), "failed to register actor %q", a.Type)
	}

	// Run the host in the background and wait until it has registered with a runtime
	// The runtime client reconnects with backoff, so the host may start before the runtime is listening
	runCtx, cancel := context.WithCancel(t.Context())
	p.cancel = cancel
	go func() {
		p.runErrC <- h.Run(runCtx)
	}()

	waitReady(t, p.opts.Address, h.Ready(), p.runErrC)
	// The peer server starts concurrently with registration, so confirm it is serving before proceeding
	waitPeerServer(t, p.opts.Address)
}

// Stop gracefully shuts the host down mid-test
// After Stop the host can be restarted with Run, and the end-of-test Cleanup becomes a no-op
func (p *Remote) Stop(t *testing.T) {
	t.Helper()
	waitShutdown(t, p.opts.Address, p.runErrC, p.cancel)
	p.cancel = nil
}

// Rebind moves the host to a freshly reserved port for its next Run, modelling a restart as a new process
func (p *Remote) Rebind(t *testing.T) {
	t.Helper()
	reserved := ports.Reserve(t, 1)
	p.opts.Address = net.JoinHostPort("127.0.0.1", strconv.Itoa(reserved[0]))
}

func (p *Remote) Cleanup(t *testing.T) {
	t.Helper()
	waitShutdown(t, p.opts.Address, p.runErrC, p.cancel)
	p.cancel = nil
}
