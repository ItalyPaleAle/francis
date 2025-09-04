package host

import (
	"context"
	"fmt"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

func (h *Host) executeAlarm(ctx context.Context, ref components.ActorRef, name string, data any) error {
	_, err := h.lockAndInvokeFn(ctx, ref, func(ctx context.Context, act *activeActor) (any, error) {
		obj, ok := act.instance.(actor.ActorAlarm)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		// Invoke the actor
		err := obj.Alarm(ctx, name, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return nil, nil
	})
	return err
}
