package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (any, error) {
	// Look up the actor
	ref := actorRef(actorType, actorID)
	res, err := h.actorProvider.LookupActor(ctx, ref, components.LookupActorOpts{})
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor '%s': %w", ref, err)
	}

	// TODO: Check if actor is not local
	if res.Address != h.address {
		return nil, errors.New("invoking remote actors not yet implemented")
	}

	return h.invokeLocal(ctx, ref, method, data)
}

func (h *Host) invokeLocal(ctx context.Context, ref components.ActorRef, method string, data any) (any, error) {
	actor, err := h.getOrCreateActor(ref)
	if err != nil {
		return nil, err
	}

	// Invoke the actor
	return actor.Invoke(ctx, method, data)
}

func actorRef(actorType string, actorID string) components.ActorRef {
	return components.ActorRef{
		ActorType: actorType,
		ActorID:   actorID,
	}
}

func (h *Host) getOrCreateActor(ref components.ActorRef) (actor.Actor, error) {
	// TODO: Idle timeout

	// Get the factory function
	fn, err := h.createActorFn(ref)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(ref.String(), fn)

	return actor, nil
}

func (h *Host) createActorFn(ref components.ActorRef) (func() actor.Actor, error) {
	// We don't need a locking mechanism here as this map is "locked" after the service has started
	factoryFn := h.actorFactories[ref.ActorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	return func() actor.Actor {
		return factoryFn(ref.ActorID, h.service)
	}, nil
}
