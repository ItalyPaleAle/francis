package host

import (
	"time"

	"github.com/italypaleale/francis/internal/actorcore"
)

// The options below mirror the ones exposed by the local and remote host packages, so code that registers actors against a Host does not need to import a topology-specific package

// WithIdleTimeout sets the maximum idle time before the actor is deactivated
func WithIdleTimeout(d time.Duration) RegisterActorOption {
	return actorcore.WithIdleTimeout(d)
}

// WithDeactivationTimeout sets the timeout for deactivating actors
func WithDeactivationTimeout(d time.Duration) RegisterActorOption {
	return actorcore.WithDeactivationTimeout(d)
}

// WithConcurrencyLimit sets the maximum number of actors of the same type active on this host
func WithConcurrencyLimit(n int) RegisterActorOption {
	return actorcore.WithConcurrencyLimit(n)
}

// WithCapacityGroup places the actor type into a named host-local capacity group with a strict per-host limit
// Actor types sharing a group name draw from one budget of at most limit concurrent jobs on this host, enforced exactly in-process
func WithCapacityGroup(group string, limit int) RegisterActorOption {
	return actorcore.WithCapacityGroup(group, limit)
}

// WithMaxAttempts sets the maximum number of attempts when invoking the actor or executing alarms
func WithMaxAttempts(n int) RegisterActorOption {
	return actorcore.WithMaxAttempts(n)
}

// WithInitialRetryDelay sets the initial retry delay after failed invocation attempts
func WithInitialRetryDelay(d time.Duration) RegisterActorOption {
	return actorcore.WithInitialRetryDelay(d)
}

// WithBootstrapData sets optional data passed to ActorBootstrapper.Bootstrap when the host bootstraps the singleton instance
// This option is meant for RegisterSingletonActor and has no effect when passed to RegisterActor
func WithBootstrapData(data any) RegisterActorOption {
	return actorcore.WithBootstrapData(data)
}
