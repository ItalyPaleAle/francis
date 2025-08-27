package host

import (
	"context"
	"fmt"

	"github.com/italypaleale/actors/components"
)

func (h *Host) executeAlarm(ctx context.Context, ref components.ActorRef, name string, data any) error {
	act, err := h.getOrCreateActor(ref)
	if err != nil {
		return err
	}

	// Acquire a lock for turn-based concurrency
	err = act.Lock(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire lock for actor: %w", err)
	}
	defer act.Unlock()
	err = h.idleActorProcessor.Enqueue(act)
	if err != nil {
		return fmt.Errorf("failed to enqueue actor in idle processor: %w", err)
	}

	// Invoke the actor
	err = act.instance.Alarm(ctx, name, data)
	if err != nil {
		return fmt.Errorf("error from actor: %w", err)
	}

	return nil
}
