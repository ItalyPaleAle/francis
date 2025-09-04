package host

import (
	"context"
	"fmt"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
)

// TODO: Make data "any" and use msgpak
type SetAlarmOpts = ref.AlarmProperties

func (h *Host) SetAlarm(ctx context.Context, actorType string, actorID string, name string, opts *SetAlarmOpts) error {
	req := components.SetAlarmReq{}
	if opts != nil {
		req.AlarmProperties = *opts
	}

	err := h.actorProvider.SetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name), req)
	if err != nil {
		return fmt.Errorf("failed to set alarm: %w", err)
	}

	return nil
}
