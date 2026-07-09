//go:build integration

package host

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
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
	// BuiltInActors are framework-managed actors registered via RegisterBuiltInActor
	BuiltInActors []builtinactor.BuiltInActor
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

// ListJobs lists an actor's jobs straight through the host, bypassing the Service guard so tests can inspect built-in actors
func (p *Local) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	return p.h.ListJobs(ctx, actorType, actorID)
}

func (p *Local) Run(t *testing.T) {
	t.Helper()

	// Assemble the host options, embedding the shared backend's provider
	hostOpts := []local.HostOption{
		local.WithAddress(p.opts.Address),
		// Every local host derives the same CA from the shared runtime PSK, so they authenticate each other with mTLS
		local.WithRuntimePSKs(clustersecret.RuntimePSK),
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

	// Built-in and regular actors must be registered before Run
	for _, b := range p.opts.BuiltInActors {
		require.NoError(t, h.RegisterBuiltInActor(b), "failed to register built-in actor")
	}
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

// Stop gracefully shuts the host down mid-test
// After Stop the host can be restarted with Run, and the end-of-test Cleanup becomes a no-op
func (p *Local) Stop(t *testing.T) {
	t.Helper()
	waitShutdown(t, p.opts.Address, p.runErrC, p.cancel)
	p.cancel = nil
}

func (p *Local) Cleanup(t *testing.T) {
	t.Helper()
	waitShutdown(t, p.opts.Address, p.runErrC, p.cancel)
	p.cancel = nil
}
