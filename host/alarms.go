package host

import (
	"context"

	"github.com/italypaleale/actors/components"
)

func (h *Host) executeAlarm(ctx context.Context, ref components.ActorRef, name string, data any) error {
	actor, err := h.getOrCreateActor(ref)
	if err != nil {
		return err
	}

	// Invoke the actor
	return actor.Alarm(ctx, name, data)
}
