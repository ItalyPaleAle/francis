//go:build integration

// Package runtime provides the standalone Francis runtime as a framework process
//
// The runtime is the control plane of the remote topology: it owns the provider and coordinates placement, state, and alarms for the remote hosts that connect to it
package runtime

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	runtimepkg "github.com/italypaleale/francis/runtime"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
)

const (
	// startupGrace is how long Run waits to catch an immediate startup failure before assuming the server is listening
	// The runtime exposes no readiness signal, but remote hosts reconnect with backoff, so end-to-end readiness is gated by the hosts' own Ready channels
	startupGrace    = 250 * time.Millisecond
	shutdownTimeout = 45 * time.Second

	// ShutdownGrace keeps runtime teardown snappy in tests
	ShutdownGrace = 5 * time.Second
)

// Options configures a runtime process
type Options struct {
	// Bind is the address and port the runtime's WebTransport server listens on
	Bind string
	// Backend supplies the provider the runtime owns, built at Run time
	Backend provider.Backend
	// Logger is optional and defaults to the runtime's discarding logger
	Logger *slog.Logger
	// Extra runtime options applied last
	Extra []runtimepkg.RuntimeOption
}

// Runtime is a standalone runtime managed as a framework process
type Runtime struct {
	opts    Options
	rt      *runtimepkg.Runtime
	runErrC chan error
	cancel  context.CancelFunc
}

// New returns a runtime process that is started by Run
func New(opts Options) *Runtime {
	return &Runtime{
		opts:    opts,
		runErrC: make(chan error, 1),
	}
}

// Address returns the address the runtime listens on
func (p *Runtime) Address() string {
	return p.opts.Bind
}

func (p *Runtime) Run(t *testing.T) {
	t.Helper()

	logger := p.opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// Build the provider the runtime owns from the shared backend
	prov := p.opts.Backend.NewProvider(t, logger)

	rtOpts := make([]runtimepkg.RuntimeOption, 0, len(p.opts.Extra)+3)
	rtOpts = append(rtOpts,
		runtimepkg.WithBind(p.opts.Bind),
		runtimepkg.WithLogger(logger),
		runtimepkg.WithShutdownGracePeriod(ShutdownGrace),
	)
	rtOpts = append(rtOpts, p.opts.Extra...)

	rt, err := runtimepkg.NewRuntime(prov, rtOpts...)
	require.NoError(t, err, "failed to create runtime")
	p.rt = rt

	// Run the runtime in the background
	runCtx, cancel := context.WithCancel(t.Context())
	p.cancel = cancel
	go func() {
		p.runErrC <- rt.Run(runCtx)
	}()

	// Catch an immediate failure such as a bind error, then return and let hosts reconnect
	select {
	case err := <-p.runErrC:
		p.runErrC <- err
		t.Fatalf("runtime %s exited during startup: %v", p.opts.Bind, err)
	case <-time.After(startupGrace):
		// Assume the server is listening
	}
}

func (p *Runtime) Cleanup(t *testing.T) {
	t.Helper()
	if p.cancel == nil {
		return
	}
	p.cancel()

	select {
	case <-p.runErrC:
		// Run returned, so the runtime has stopped
	case <-time.After(shutdownTimeout):
		t.Fatalf("runtime %s did not shut down within %s", p.opts.Bind, shutdownTimeout)
	}
	p.cancel = nil
}
