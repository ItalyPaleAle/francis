//go:build integration

package host

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
)

// LocalOptions configures a local host process
type LocalOptions struct {
	// Address the host is reachable at and advertises to its peers, e.g. "127.0.0.1:7571"
	Address string
	// Bind is the address the peer server actually listens on, when it differs from the advertised address
	// A scenario sets it when peers must reach this host through something else, such as a severable link that stands in front of it
	Bind string
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

// Halt deactivates an actor straight through the host, bypassing the Service guard so tests can halt built-in actors
// It returns actor.ErrActorNotHosted when the actor is not active on this host, which doubles as a placement probe
func (p *Local) Halt(actorType string, actorID string) error {
	return p.h.Halt(actorType, actorID)
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
	// When the peer server listens somewhere other than the advertised address, peers reach it indirectly and the bind must be set explicitly
	if p.opts.Bind != "" {
		bindAddr, bindPort := splitHostPort(t, p.opts.Bind)
		hostOpts = append(hostOpts, local.WithBindAddress(bindAddr), local.WithBindPort(bindPort))
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
		err = h.RegisterBuiltInActor(b)
		require.NoError(t, err, "failed to register built-in actor")
	}
	for _, a := range p.opts.Actors {
		err = h.RegisterActor(a.Type, a.Factory, a.Opts...)
		require.NoError(t, err, "failed to register actor %q", a.Type)
	}

	// A previous run may have left its exit error behind, which would otherwise be mistaken for this one failing immediately
	drainExit(p.runErrC)

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

// WaitExit blocks until the host's Run returns on its own and reports the error it exited with
func (p *Local) WaitExit(t *testing.T, timeout time.Duration) error {
	t.Helper()
	return waitExit(t, p.opts.Address, p.runErrC, timeout)
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
