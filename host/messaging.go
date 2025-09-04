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

	// TODO: Handle errActorHalted and retry, actor could be on a separate host
	return h.invokeLocal(ctx, ref, method, data)
}

func (h *Host) invokeLocal(ctx context.Context, ref components.ActorRef, method string, data any) (any, error) {
	return h.lockAndInvokeFn(ctx, ref, func(ctx context.Context, act *activeActor) (any, error) {
		obj, ok := act.instance.(actor.ActorInvoke)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Invoke method", act.ActorType())
		}

		// Invoke the actor
		res, err := obj.Invoke(ctx, method, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return res, nil
	})
}

func (h *Host) lockAndInvokeFn(parentCtx context.Context, ref components.ActorRef, fn func(context.Context, *activeActor) (any, error)) (any, error) {
	// Get the actor, which may create it
	act, err := h.getOrCreateActor(ref)
	if err != nil {
		return nil, err
	}

	// Create a context for this request, which allows us to stop it in-flight if needed
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Acquire a lock for turn-based concurrency
	haltCh, err := act.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for actor: %w", err)
	}
	defer act.Unlock()

	go func() {
		select {
		case <-haltCh:
			// The actor is being halted, so we need to cancel the context
			cancel()
		case <-ctx.Done():
			// The method is returning
			return
		}
	}()

	return fn(ctx, act)
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

	idleTimeout := h.actorsConfig[ref.ActorType].IdleTimeout
	return func() *activeActor {
		instance := factoryFn(ref.ActorID, h.service)
		return newActiveActor(ref, instance, idleTimeout, h.idleActorProcessor, h.clock)
	}, nil
}
