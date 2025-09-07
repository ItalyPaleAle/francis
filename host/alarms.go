package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
)

func (h *Host) runAlarmFetcher(parentCtx context.Context) error {
	h.log.DebugContext(parentCtx, "Starting background alarm fetching", slog.Any("interval", h.alarmsPollInterval))

	t := h.clock.NewTicker(h.alarmsPollInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			// TODO
		case <-parentCtx.Done():
			// Stop when the context is canceled
			return parentCtx.Err()
		}
	}
}

func (h *Host) GetAlarm(ctx context.Context, actorType string, actorID string, name string) (actor.AlarmProperties, error) {
	res, err := h.actorProvider.GetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
	if errors.Is(err, components.ErrNoAlarm) {
		return actor.AlarmProperties{}, actor.ErrAlarmNotFound
	} else if err != nil {
		return actor.AlarmProperties{}, fmt.Errorf("failed to get alarm: %w", err)
	}

	properties, err := alarmPropertiesFromAlarmRes(res)
	if err != nil {
		return actor.AlarmProperties{}, err
	}

	return properties, nil
}

func (h *Host) SetAlarm(ctx context.Context, actorType string, actorID string, name string, properties actor.AlarmProperties) error {
	req, err := alarmPropertiesToAlarmReq(properties)
	if err != nil {
		return err
	}

	err = h.actorProvider.SetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name), req)
	if err != nil {
		return fmt.Errorf("failed to set alarm: %w", err)
	}

	return nil
}

func (h *Host) DeleteAlarm(ctx context.Context, actorType string, actorID string, name string) error {
	err := h.actorProvider.DeleteAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
	if errors.Is(err, components.ErrNoAlarm) {
		return actor.ErrAlarmNotFound
	} else if err != nil {
		return fmt.Errorf("failed to delete alarm: %w", err)
	}

	return nil
}

func alarmPropertiesFromAlarmRes(res components.GetAlarmRes) (actor.AlarmProperties, error) {
	o := actor.AlarmProperties{
		DueTime:  res.DueTime,
		Interval: res.Interval,
		Data:     res.Data,
	}

	if res.TTL != nil {
		o.TTL = *res.TTL
	}

	return o, nil
}

func alarmPropertiesToAlarmReq(o actor.AlarmProperties) (components.SetAlarmReq, error) {
	req := components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  o.DueTime,
			Interval: o.Interval,
			Data:     o.Data,
		},
	}

	if !o.TTL.IsZero() {
		req.TTL = &o.TTL
	}

	return req, nil
}
