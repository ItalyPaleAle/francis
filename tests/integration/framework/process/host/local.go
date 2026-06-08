//go:build integration

package host

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
)

// LocalOptions configures a local host process
type LocalOptions struct {
	// Address the host binds to and is reachable at, e.g. "127.0.0.1:7571"
	Address string
	// Backend supplies the embedded provider option, resolved at Run time
	Backend provider.Backend
	// Actors to register before the host starts
	Actors []ActorReg
	// PeerAuthKey is the shared peer-authentication key, defaulting to DefaultPeerAuthKey
	PeerAuthKey string
	// Logger is optional and defaults to the host's discarding logger
	Logger *slog.Logger
	// Extra host options applied last, e.g. custom timeouts
	Extra []local.HostOption
}

// Local is a local actor host managed as a framework process
type Local struct {
	opts    LocalOptions
	h       *local.Host
	runErrC chan error
	cancel  context.CancelFunc
}

// NewLocal returns a local host process that is started by Run
func NewLocal(opts LocalOptions) *Local {
	return &Local{
		opts:    opts,
		runErrC: make(chan error, 1),
	}
}

func (p *Local) Service() *actor.Service {
	return p.h.Service()
}

func (p *Local) HostID() string {
	return p.h.HostID()
}

func (p *Local) Address() string {
	return p.opts.Address
}

func (p *Local) Run(t *testing.T) {
	t.Helper()

	key := p.opts.PeerAuthKey
	if key == "" {
		key = DefaultPeerAuthKey
	}

	// Assemble the host options, embedding the shared backend's provider
	hostOpts := []local.HostOption{
		local.WithAddress(p.opts.Address),
		local.WithPeerAuthenticationSharedKey(key),
		// Hosts use self-signed certs and must accept each other's certificates for peer invocation
		local.WithServerTLSInsecureSkipTLSValidation(),
		local.WithShutdownGracePeriod(ShutdownGrace),
		p.opts.Backend.LocalHostOption(t),
	}
	if p.opts.Logger != nil {
		hostOpts = append(hostOpts, local.WithLogger(p.opts.Logger))
	}
	hostOpts = append(hostOpts, p.opts.Extra...)

	h, err := local.NewHost(hostOpts...)
	require.NoError(t, err, "failed to create local host")
	p.h = h

	// Actors must be registered before Run
	for _, a := range p.opts.Actors {
		require.NoError(t, h.RegisterActor(a.Type, a.Factory, a.Opts), "failed to register actor %q", a.Type)
	}

	// Run the host in the background and wait until it has registered with the provider
	runCtx, cancel := context.WithCancel(t.Context())
	p.cancel = cancel
	go func() {
		p.runErrC <- h.Run(runCtx)
	}()

	waitReady(t, p.opts.Address, h.Ready(), p.runErrC)
	// The peer server starts concurrently with registration, so confirm it is serving before proceeding
	waitPeerServer(t, p.opts.Address)
}

func (p *Local) Cleanup(t *testing.T) {
	t.Helper()
	waitShutdown(t, p.opts.Address, p.runErrC, p.cancel)
	p.cancel = nil
}
