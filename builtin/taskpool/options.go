package taskpool

import (
	"context"
	"log/slog"
	"time"
)

// options holds the configuration for a task pool, built up from the functional Option values passed to New
type options struct {
	// handler runs each submitted task and is required
	handler func(ctx context.Context, task Task) error
	// accept, when set, lets a host decline a task before running it, re-routing it to another host
	accept func(ctx context.Context, task Task) bool
	// concurrency is the strict maximum number of tasks this host runs at once, across every queue of the pool
	concurrency int
	// capabilities are the extra capabilities this host advertises, each backing a queue only capable hosts serve
	capabilities []string
	// maxAttempts is the number of times a failing task is retried before it is dead-lettered
	maxAttempts int
	// initialRetryDelay is the base backoff between retries of a failing task
	initialRetryDelay time.Duration
	// idleTimeout is how long a finished worker lingers before deactivating; it is only a safety net, since workers halt themselves after a task
	idleTimeout time.Duration
	// logger is an optional logger
	logger *slog.Logger
}

// Option configures a task pool built with New
type Option func(*options)

// WithHandler sets the function that runs each submitted task
// It is required
// The handler receives the task and should return nil on success, an error to retry (then dead-letter once attempts are exhausted), actor.ErrJobPermanentFailure to dead-letter immediately, or actor.ErrJobRejected to decline the task so another host runs it
// A task pool does not track results: the handler is responsible for communicating its outcome, for example by writing to a database, calling an API, or invoking another actor
func WithHandler(fn func(ctx context.Context, task Task) error) Option {
	return func(o *options) {
		o.handler = fn
	}
}

// WithConcurrency sets the strict maximum number of tasks this host runs at once, shared across every queue of the pool
// It defaults to 1, and is enforced exactly in-process, so more hosts (or a higher limit) mean more tasks run in parallel
func WithConcurrency(n int) Option {
	return func(o *options) {
		o.concurrency = n
	}
}

// WithCapability advertises a capability on this host, so tasks that require it can run here
// It can be passed more than once to advertise several capabilities
// A host that advertises no capability still serves every task submitted without a required capability, so a plain host needs to declare nothing
func WithCapability(capability string) Option {
	return func(o *options) {
		o.capabilities = append(o.capabilities, capability)
	}
}

// WithAccept sets an optional predicate that runs before a task executes, letting this host decline it
// Returning false re-routes the task to another host without counting an attempt, which is a cheap way to express dynamic, per-task affinity that a static capability cannot (for example, only run where the input file is already local)
func WithAccept(fn func(ctx context.Context, task Task) bool) Option {
	return func(o *options) {
		o.accept = fn
	}
}

// WithMaxAttempts sets how many times a failing task is retried before it is dead-lettered
func WithMaxAttempts(n int) Option {
	return func(o *options) {
		o.maxAttempts = n
	}
}

// WithInitialRetryDelay sets the base backoff between retries of a failing task
func WithInitialRetryDelay(d time.Duration) Option {
	return func(o *options) {
		o.initialRetryDelay = d
	}
}

// WithIdleTimeout sets how long a finished worker lingers before deactivating
// Workers halt themselves after each task, so this is only a safety net for the rare case a self-halt does not run
func WithIdleTimeout(d time.Duration) Option {
	return func(o *options) {
		o.idleTimeout = d
	}
}

// WithLogger sets the logger the pool uses for task lifecycle events
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}
