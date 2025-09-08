package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
		ExecuteFn: h.executeAlarm,
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
	var i int
	for _, a := range res {
		// If the alarm is to be executed immediately (within the next 0.1ms), skip enqueuing it and execute it right away
		if a.DueTime().Sub(h.clock.Now()) < 100*time.Microsecond {
			go h.executeAlarm(a)
			continue
		}

		res[i] = a
		i++
	}

	// Enqueue the alarms all at once as this is more efficient (we lock the processor only once)
	err = h.alarmProcessor.Enqueue(res[:i]...)
	if err != nil {
		return fmt.Errorf("error enqueueing alarms: %w", err)
	}

	return nil
}

// Callback for the alarm processor
func (h *Host) executeAlarm(lease *ref.AlarmLease) {
	h.log.Debug("Executing alarm", slog.String("id", lease.Key()), slog.Any("due", lease.DueTime()))

	// Get and lock the actor
	// TODO: Need to remove the completed alarm
	_, err := h.lockAndInvokeFn(context.Background(), lease.ActorRef(), func(parentCtx context.Context, act *activeActor) (any, error) {
		// Before we execute an alarm we need to fetch it again using the lease
		// This is because alarms we have in-memory could have been here for a few seconds, and they may not represent the accurate
		// state of the data in the provider. For example, it could have been edited or deleted, or the lease could have been broken.
		// Note we do this after we acquired the lock on the actor, since that operation can take time.
		ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
		defer cancel()
		a, err := h.actorProvider.GetLeasedAlarm(ctx, lease)
		if errors.Is(err, components.ErrNoAlarm) {
			// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
			// We log this, and return
			h.log.Warn("Lease was lost for alarm - skipping execution", slog.String("id", lease.Key()), slog.Any("due", lease.DueTime()))
			return nil, nil
		} else if err != nil {
			return nil, fmt.Errorf("error retrieving alarm from provider: %w", err)
		}

		obj, ok := act.instance.(actor.ActorAlarm)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		// Invoke the actor
		err = obj.Alarm(parentCtx, a.Name, a.Data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return nil, nil
	})

	if err != nil {
		// Log the error - we are in a background task
		h.log.Error("Error executing alarm", slog.String("id", lease.Key()), slog.Any("due", lease.DueTime()), slog.Any("error", err))
		return
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
