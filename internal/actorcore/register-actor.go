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
)

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

	return nil
}
