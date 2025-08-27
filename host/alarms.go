package host

import (
	"context"
	"fmt"

	"github.com/italypaleale/actors/components"
)

func (h *Host) executeAlarm(ctx context.Context, ref components.ActorRef, name string, data any) error {
	_, err := h.lockAndInvokeFn(ctx, ref, func(ctx context.Context, act *activeActor) (any, error) {
		// Invoke the actor
		err := act.instance.Alarm(ctx, name, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return nil, nil
	})
	return err
}
