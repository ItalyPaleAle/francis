package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/eventqueue"
	"github.com/italypaleale/actors/internal/ref"
)

func (h *Host) runAlarmFetcher(ctx context.Context) error {
	h.log.DebugContext(ctx, "Starting background alarm fetcher", slog.Any("interval", h.alarmsPollInterval))
	defer h.log.Debug("Stopped background alarm fetcher")

	// Start the processor
	h.alarmProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		ExecuteFn: func(r *ref.AlarmLease) {},
	})
	defer func() {
		apErr := h.alarmProcessor.Close()
		if apErr != nil {
			h.log.Error("Failed to close alarm processor", slog.Any("error", apErr))
		}
		h.alarmProcessor = nil
	}()

	t := h.clock.NewTicker(h.alarmsPollInterval)
	defer t.Stop()

	var err error
	hostList := []string{h.hostID}
	for {
		select {
		case <-t.C():
			err = h.fetchAndEnqueueAlarms(ctx, hostList)
			if err != nil {
				// Log the error only
				h.log.ErrorContext(ctx, "Failed to fetch alarms", slog.Any("error", err))
			}
		case <-ctx.Done():
			// Stop when the context is canceled
			return ctx.Err()
		}
	}
}

func (h *Host) fetchAndEnqueueAlarms(ctx context.Context, hostList []string) error {
	// Fetch all upcoming alarms
	res, err := h.actorProvider.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: hostList,
	})
	if err != nil {
		return fmt.Errorf("error fetching alarms: %w", err)
	}

	// Enqueue all alarms
	if len(res) == 0 {
		return nil
	}

	err = h.alarmProcessor.Enqueue(res...)
	if err != nil {
		return fmt.Errorf("error enqueueing alarms: %w", err)
	}

	return nil
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
