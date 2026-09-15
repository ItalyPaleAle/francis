package actorcore

import (
	"errors"
	"math"
	"time"
)

const (
	defaultActorIdleTimeout         = 5 * time.Minute
	defaultActorDeactivationTimeout = 5 * time.Second
	defaultAlarmMaxAttempts         = 3
	defaultAlarmInitialRetryDelay   = 2 * time.Second
	// defaultDeadLetteredJobRetention is how long a dead-lettered job's record is kept when the actor type does not say
	// A failure is worth keeping long enough for someone to notice it and replay it, and long enough that a weekly process catches it, without growing without bound
	defaultDeadLetteredJobRetention = 30 * 24 * time.Hour
)

// LockMode selects how the framework serializes the invocations of an actor type
type LockMode uint8

const (
	// LockModeExclusive is the default turn-based model, where one invocation runs at a time and concurrent Peek calls share the read side
	LockModeExclusive LockMode = 0
	// LockModeShared runs every invocation of the type under the shared lock and never takes the exclusive one, so invocations never block each other
	// An actor type registered this way synchronizes itself, including around its own durable state writes, and it rejects Peek because none of its invocations are read-only
	// This is limited to built-in actors only (at least for now)
	LockModeShared LockMode = 1
)

// IsValid reports whether the lock mode is one of the defined modes
func (m LockMode) IsValid() bool {
	switch m {
	case LockModeExclusive, LockModeShared:
		return true
	default:
		return false
	}
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	// Maximum idle time before the actor is deactivated
	// Defaults to 5 minutes
	// A negative value means no timeout
	IdleTimeout time.Duration
	// Timeout for deactivating actors (because they are idle or they are being halted)
	// Defaults to 5s
	DeactivationTimeout time.Duration
	// Maximum number of actors of the same type active on this host
	// Defaults to 0, indicating no limit on the host
	// This must be between 0 (unlimited) and MaxInt32
	ConcurrencyLimit int
	// Maximum number of attempts when invoking the actor or executing alarms
	// Defaults to 3
	MaxAttempts int
	// Initial retry delay after failed invocation attempts
	// Defaults to 2s
	InitialRetryDelay time.Duration
	// CompletedJobRetention is how long a job dispatched to this actor type keeps a record after it completes successfully
	// When set, a completed job leaves a record that ListJobs and GetJob report until the retention elapses
	// Defaults to 0, which keeps no record of a completed job, and a negative value keeps one that never expires
	CompletedJobRetention time.Duration
	// DeadLetteredJobRetention is how long a job dispatched to this actor type keeps its record after it is dead-lettered
	// A dead-lettered job is always recorded, so this only decides for how long
	// Defaults to 30 days, and a negative value keeps the record until something removes it
	DeadLetteredJobRetention time.Duration
	// CapacityGroup, when set, places this actor type into a named host-local capacity group
	// Every actor type registered on this host with the same group name shares a single strict concurrency budget, enforced in-process when their jobs execute
	// It is the exact per-host guarantee that complements the best-effort, cluster-wide ConcurrencyLimit placement hint
	CapacityGroup string
	// CapacityGroupLimit is the maximum number of jobs that may run at once across all actor types in the capacity group, on this host
	// It is required when CapacityGroup is set, must be greater than zero, and every actor type sharing a group must declare the same limit
	CapacityGroupLimit int
	// LockMode selects how the framework serializes the invocations of this actor type
	// It defaults to LockModeExclusive, the turn-based model every application actor uses
	// LockModeShared is currently reserved for built-in actors
	LockMode LockMode
	// BootstrapData is optional data passed to ActorBootstrapper.Bootstrap when the host bootstraps the singleton instance
	// It is delivered as the Bootstrap call's data argument (decoded from the invocation envelope), just like Invokes deliver their data via an Envelope
	// It is nil when not provided
	// This option is ignored when passed to RegisterActor
	BootstrapData any
}

// RegisterActorOption is a functional option for RegisterActor.
type RegisterActorOption func(*RegisterActorOptions)

// WithIdleTimeout sets the maximum idle time before the actor is deactivated
func WithIdleTimeout(d time.Duration) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.IdleTimeout = d
	}
}

// WithDeactivationTimeout sets the timeout for deactivating actors
func WithDeactivationTimeout(d time.Duration) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.DeactivationTimeout = d
	}
}

// WithConcurrencyLimit sets the maximum number of actors of the same type active on this host
func WithConcurrencyLimit(n int) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.ConcurrencyLimit = n
	}
}

// WithMaxAttempts sets the maximum number of attempts when invoking the actor or executing alarms
func WithMaxAttempts(n int) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.MaxAttempts = n
	}
}

// WithCompletedJobRetention sets how long a job dispatched to this actor type keeps a record after it completes successfully
// A completed job leaves no record at all unless this is set, and a negative duration keeps one that never expires
func WithCompletedJobRetention(d time.Duration) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.CompletedJobRetention = d
	}
}

// WithDeadLetteredJobRetention sets how long a job dispatched to this actor type keeps its record after it is dead-lettered
// A dead-lettered job is always recorded, so this only decides for how long: it defaults to 30 days, and a negative duration keeps the record until something removes it
func WithDeadLetteredJobRetention(d time.Duration) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.DeadLetteredJobRetention = d
	}
}

// WithInitialRetryDelay sets the initial retry delay after failed invocation attempts
func WithInitialRetryDelay(d time.Duration) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.InitialRetryDelay = d
	}
}

// WithBootstrapData sets optional data passed to ActorBootstrapper.Bootstrap when the host bootstraps the singleton instance
func WithBootstrapData(data any) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.BootstrapData = data
	}
}

// WithCapacityGroup places the actor type into a named host-local capacity group with a strict per-host limit
// Actor types sharing a group name draw from one budget of at most limit concurrent jobs on this host, enforced exactly in-process
func WithCapacityGroup(group string, limit int) RegisterActorOption {
	return func(o *RegisterActorOptions) {
		o.CapacityGroup = group
		o.CapacityGroupLimit = limit
	}
}

func (o *RegisterActorOptions) Validate() error {
	switch {
	case o.IdleTimeout == 0:
		// Set default idle timeout if empty
		o.IdleTimeout = defaultActorIdleTimeout
	case o.IdleTimeout < 0:
		// A negative number means no timeout
		o.IdleTimeout = -1
	}

	switch {
	case o.ConcurrencyLimit <= 0:
		o.ConcurrencyLimit = 0
	case o.ConcurrencyLimit > math.MaxInt32:
		return errors.New("option ConcurrencyLimit must fit in int32 (2^31-1)")
	}

	switch {
	case o.DeactivationTimeout == 0:
		o.DeactivationTimeout = defaultActorDeactivationTimeout
	case o.DeactivationTimeout < 0:
		return errors.New("option DeactivationTimeout must not be negative")
	}

	if o.MaxAttempts <= 0 {
		o.MaxAttempts = defaultAlarmMaxAttempts
	}

	if o.InitialRetryDelay <= 0 {
		o.InitialRetryDelay = defaultAlarmInitialRetryDelay
	}

	// A dead-lettered job is always recorded, so an unset retention takes the default rather than meaning "keep nothing"
	// A negative value is the way to ask for a record that never expires, and is normalized so every negative spelling behaves the same
	switch {
	case o.DeadLetteredJobRetention == 0:
		o.DeadLetteredJobRetention = defaultDeadLetteredJobRetention
	case o.DeadLetteredJobRetention < 0:
		o.DeadLetteredJobRetention = -1
	}

	// A completed job is recorded only when asked for, so zero stays zero, and a negative value is normalized the same way
	if o.CompletedJobRetention < 0 {
		o.CompletedJobRetention = -1
	}

	// A capacity group is meaningless without a positive limit to enforce
	if o.CapacityGroup != "" && o.CapacityGroupLimit <= 0 {
		return errors.New("option CapacityGroupLimit must be greater than zero when CapacityGroup is set")
	}

	// An unknown lock mode would silently fall back to the turn-based path, so reject it at registration instead
	if !o.LockMode.IsValid() {
		return errors.New("option LockMode is not a valid lock mode")
	}

	return nil
}
