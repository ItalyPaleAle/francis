package host

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	act, err := h.getOrCreateActor(ref)
	if err != nil {
		return nil, err
	}

	// Acquire a lock for turn-based concurrency
	err = act.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for actor: %w", err)
	}
	defer act.Unlock()
	err = h.idleActorProcessor.Enqueue(act)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue actor in idle processor: %w", err)
	}

	// Invoke the actor
	res, err := act.instance.Invoke(ctx, method, data)
	if err != nil {
		return nil, fmt.Errorf("error from actor: %w", err)
	}

	return res, nil
}

func actorRef(actorType string, actorID string) components.ActorRef {
	return components.ActorRef{
		ActorType: actorType,
		ActorID:   actorID,
	}
}

func (h *Host) getOrCreateActor(ref components.ActorRef) (*activeActor, error) {
	// Get the factory function
	fn, err := h.createActorFn(ref)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(ref.String(), fn)

	return actor, nil
}

func (h *Host) createActorFn(ref components.ActorRef) (func() *activeActor, error) {
	// We don't need a locking mechanism here as these maps are "locked" after the service has started
	factoryFn := h.actorFactories[ref.ActorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	idleTimeout := time.Duration(h.actorsConfig[ref.ActorType].IdleTimeout) * time.Second

	return func() *activeActor {
		instance := factoryFn(ref.ActorID, h.service)
		return newActiveActor(ref, instance, idleTimeout, h.clock)
	}, nil
}
