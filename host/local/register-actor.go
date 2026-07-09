package local

import (
	"errors"
	"fmt"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions = actorcore.RegisterActorOptions

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}

	return h.core.RegisterActor(actorType, factory, opts)
}

// RegisterSingletonActor registers a singleton actor in the host.
// A singleton actor is reached at the well-known actor.SingletonActorID from every host, and the host bootstraps that instance once ready: if it implements actor.ActorBootstrapper, its Bootstrap hook runs, routed to the single owning host and serialized by its turn lock.
// Use it for cluster-wide setup that must happen once, such as registering a durable recurring job.
// Must be called before Run, and can be called multiple times to register more than one singleton actor.
func (h *Host) RegisterSingletonActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterSingletonActor after host has started")
	}

	err := h.core.RegisterActor(actorType, factory, opts)
	if err != nil {
		return err
	}

	h.singletonActors = append(h.singletonActors, actorType)
	return nil
}

// RegisterBuiltInActor registers a framework-managed built-in actor on the host, such as one created with cronjob.New.
// The host registers it under its reserved type and, when the built-in actor is a singleton, bootstraps its singleton instance once ready.
// Must be called before Run, and can be called multiple times to register more than one built-in actor.
func (h *Host) RegisterBuiltInActor(b builtinactor.BuiltInActor) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterBuiltInActor after host has started")
	}
	if b == nil {
		return errors.New("built-in actor is nil")
	}

	// Built-in actors carry only their bare type
	// The host adds the reserved prefix when registering
	actorType := builtinactor.FullActorType(b.ActorType())
	err := h.core.RegisterActor(actorType, b.Factory(), b.RegisterOptions())
	if err != nil {
		return fmt.Errorf("failed to register built-in actor %q: %w", actorType, err)
	}

	if b.Singleton() {
		h.singletonActors = append(h.singletonActors, actorType)
	}
	return nil
}
