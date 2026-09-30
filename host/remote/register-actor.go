package remote

import (
	"errors"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// RegisterActorOption is a functional option for RegisterActor/RegisterSingletonActor.
type RegisterActorOption = actorcore.RegisterActorOption

// errClientOnly is returned when an actor is registered on a host created with WithClientOnly, which hosts no actor by design
var errClientOnly = errors.New("cannot register an actor on a client-only host")

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

// WithCompletedJobRetention sets how long a job dispatched to this actor type keeps a record after it completes successfully
// A completed job leaves no record at all unless this is set, and a negative duration keeps one that never expires
func WithCompletedJobRetention(d time.Duration) RegisterActorOption {
	return actorcore.WithCompletedJobRetention(d)
}

// WithDeadLetteredJobRetention sets how long a job dispatched to this actor type keeps its record after it is dead-lettered
// Defaults to 30 days
// Set to <= 0 to disable automatic deletion of records
func WithDeadLetteredJobRetention(d time.Duration) RegisterActorOption {
	return actorcore.WithDeadLetteredJobRetention(d)
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

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts ...RegisterActorOption) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}
	if h.clientOnly {
		return errClientOnly
	}

	var o actorcore.RegisterActorOptions
	for _, opt := range opts {
		opt(&o)
	}
	return h.core.RegisterActor(actorType, factory, o)
}

// RegisterSingletonActor registers a singleton actor in the host.
// A singleton actor is reached at the well-known actor.SingletonActorID from every host, and the host bootstraps that instance once ready: if it implements actor.ActorBootstrapper, its Bootstrap hook runs, routed to the single owning host and serialized by its turn lock.
// Use it for cluster-wide setup that must happen once, such as registering a durable recurring job.
// Must be called before Run, and can be called multiple times to register more than one singleton actor.
func (h *Host) RegisterSingletonActor(actorType string, factory actor.Factory, opts ...RegisterActorOption) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterSingletonActor after host has started")
	}
	if h.clientOnly {
		return errClientOnly
	}

	var o actorcore.RegisterActorOptions
	for _, opt := range opts {
		opt(&o)
	}

	err := h.core.RegisterSingletonActor(actorType, factory, o)
	if err != nil {
		return err
	}

	return nil
}

// RegisterBuiltInActor registers a framework-managed built-in actor on the host, such as one created with cronjob.New.
// The host registers it under its reserved type and, when the built-in actor is a singleton, bootstraps its singleton instance once ready.
// Must be called before Run, and can be called multiple times to register more than one built-in actor.
func (h *Host) RegisterBuiltInActor(b builtinactor.BuiltInActor) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterBuiltInActor after host has started")
	}
	if h.clientOnly {
		return errClientOnly
	}

	err := builtinactor.Register(h.core, b)
	if err != nil {
		return err
	}

	// Record the definition the built-in actor serves, if any, so host snapshots can report it
	h.core.RecordManagementDefinition(b)

	return nil
}
