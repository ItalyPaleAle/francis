//go:build integration

package framework

import (
	"testing"

	"github.com/italypaleale/francis/tests/integration/framework/process"
)

// Options is the assembled configuration for a test topology
type Options struct {
	procs []process.Interface
}

// Option configures Options
// Use WithProcesses to register the processes that make up a test topology
type Option func(*Options)

// WithProcesses registers one or more processes to be started by Run
//
// Order matters: list the provider backend before the hosts that depend on it, so the backend is prepared before any host connects to it
// Because cleanup runs in reverse, this also tears hosts down before the backend store is dropped
func WithProcesses(procs ...process.Interface) Option {
	return func(o *Options) { o.procs = append(o.procs, procs...) }
}

// Run starts every registered process in order and registers reverse-order cleanup via t.Cleanup
// It returns once all processes are up and ready, handing control back to the caller, which is the Case.Run that follows
func Run(t *testing.T, opts ...Option) {
	t.Helper()

	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	for _, p := range o.procs {
		// Register cleanup before Run so a process that fails partway through Run is still torn down
		// t.Cleanup is LIFO, so processes registered earlier, such as the provider backend, are cleaned up last
		t.Cleanup(func() {
			p.Cleanup(t)
		})
		p.Run(t)
	}
}
