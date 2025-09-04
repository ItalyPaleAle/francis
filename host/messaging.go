package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
)

func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (any, error) {
	// Look up the actor
	aRef := ref.NewActorRef(actorType, actorID)
	res, err := h.actorProvider.LookupActor(ctx, aRef, components.LookupActorOpts{})
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor '%s': %w", aRef, err)
	}

	// TODO: Check if actor is not local
	if res.Address != h.address {
		return nil, errors.New("invoking remote actors not yet implemented")
	}

	// TODO: Handle errActorHalted and retry, actor could be on a separate host
	return h.invokeLocal(ctx, aRef, method, data)
}

func (h *Host) invokeLocal(ctx context.Context, ref ref.ActorRef, method string, data any) (any, error) {
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

func (h *Host) lockAndInvokeFn(parentCtx context.Context, ref ref.ActorRef, fn func(context.Context, *activeActor) (any, error)) (any, error) {
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
			t := h.clock.NewTimer(h.shutdownGracePeriod)
			select {
			case <-t.C():
				// Graceful timeout has passed: forcefully cancel the context
				cancel()
				return
			case <-ctx.Done():
				// The method is returning (either fn() is done, or context was canceled)
				if !t.Stop() {
					<-t.C()
				}
				return
			}
		case <-ctx.Done():
			// The method is returning (either fn() is done, or context was canceled)
			return
		}
	}()

	return fn(ctx, act)
}

func (h *Host) executeAlarm(ctx context.Context, ref ref.AlarmRef, data any) error {
	_, err := h.lockAndInvokeFn(ctx, ref.ActorRef(), func(ctx context.Context, act *activeActor) (any, error) {
		obj, ok := act.instance.(actor.ActorAlarm)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		// Invoke the actor
		err := obj.Alarm(ctx, ref.Name, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return nil, nil
	})
	return err
}

func (h *Host) getOrCreateActor(ref ref.ActorRef) (*activeActor, error) {
	// Get the factory function
	fn, err := h.createActorFn(ref)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(ref.String(), fn)

	return actor, nil
}

func (h *Host) createActorFn(ref ref.ActorRef) (func() *activeActor, error) {
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
