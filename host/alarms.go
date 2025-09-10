package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	return h.enqueueAlarms(res)
}

func (h *Host) enqueueAlarms(leases []*ref.AlarmLease) (err error) {
	// Get the lock
	h.activeAlarmsLock.Lock()

	var (
		i  int
		ok bool
	)
	for _, a := range leases {
		// If the alarm is already ok, skip it
		// We also skip alarms that are being retried, otherwise their attempt counter and delay would be overwritten
		// (retries are not persisted and are managed in-memory only)
		_, ok = h.activeAlarms[a.Key()]
		if ok {
			continue
		}
		_, ok = h.retryingAlarms[a.Key()]
		if ok {
			continue
		}

		// If the alarm is to be executed immediately (within the next 0.1ms), skip enqueuing it and execute it right away
		if a.DueTime().Sub(h.clock.Now()) < 100*time.Microsecond {
			h.activeAlarms[a.Key()] = struct{}{}
			go h.executeActiveAlarm(a)
			continue
		}

		leases[i] = a
		i++
	}

	// Unlock
	h.activeAlarmsLock.Unlock()

	if i > 0 {
		// Enqueue the alarms all at once as this is more efficient (we lock the processor only once)
		err = h.alarmProcessor.Enqueue(leases[:i]...)
		if err != nil {
			return fmt.Errorf("error enqueueing alarms: %w", err)
		}
	}

	return nil
}

type executeAlarmStatus int

const (
	executeAlarmStatusCompleted = executeAlarmStatus(iota)
	executeAlarmStatusFatal
	executeAlarmStatusRetryable
	executeAlarmStatusAbandoned
)

// Callback for the alarm processor
func (h *Host) executeAlarm(lease *ref.AlarmLease) {
	// Mark the alarm as active
	h.activeAlarmsLock.Lock()
	_, active := h.activeAlarms[lease.Key()]
	if active {
		// Already active, so nothing to do
		h.activeAlarmsLock.Unlock()
		return
	}
	h.activeAlarms[lease.Key()] = struct{}{}
	h.activeAlarmsLock.Unlock()

	// Now we can execute the alarm
	h.executeActiveAlarm(lease)
}

// Executes an active alarm, that is already in the activeAlarms map
func (h *Host) executeActiveAlarm(lease *ref.AlarmLease) {
	ctx := context.Background()

	key := lease.Key()
	log := h.log.With(
		slog.String("id", lease.AlarmRef().String()),
		slog.Any("due", lease.DueTime()),
	)
	log.Debug("Executing alarm")

	// Get and lock the actor
	ref := lease.ActorRef()
	statusAny, err := h.lockAndInvokeFn(ctx, ref, func(parentCtx context.Context, act *activeActor) (any, error) {
		// Before we execute an alarm we need to fetch it again using the lease
		// This is because alarms we have in-memory could have been here for a few seconds, and they may not represent the accurate
		// state of the data in the provider. For example, it could have been edited or deleted, or the lease could have been broken.
		// Note we do this after we acquired the lock on the actor, since that operation can take time.
		ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
		defer cancel()
		a, err := h.actorProvider.GetLeasedAlarm(ctx, lease)
		if errors.Is(err, components.ErrNoAlarm) {
			// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
			// We return executeAlarmAbandoned, indicating there's nothing else left to do
			return executeAlarmStatusAbandoned, nil
		} else if err != nil {
			// This is a retryable error, possibly something transient with the component, and it could be retried later
			return executeAlarmStatusRetryable, fmt.Errorf("error retrieving alarm from provider: %w", err)
		}

		// Ensure the actor implements the Alarm method
		obj, ok := act.instance.(actor.ActorAlarm)
		if !ok {
			// This is a fatal error, which causes us to drop the alarm since there's no way it can be completed
			return executeAlarmStatusFatal, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		// Mark the alarm as executed now
		lease.SetExecutionTime(h.clock.Now())

		// Invoke the actor
		err = obj.Alarm(parentCtx, a.Name, a.Data)
		if err != nil {
			// Consider this as a retryable condition unless we've exceeded the max attempts
			code := executeAlarmStatusRetryable
			maxAttempts := h.actorsConfig[ref.ActorType].MaxAttempts
			if lease.Attempts() > maxAttempts {
				code = executeAlarmStatusFatal
			}
			return code, fmt.Errorf("error from actor (attempt %d of %d): %w", lease.Attempts(), maxAttempts, err)
		}

		return executeAlarmStatusCompleted, nil
	})

	// Remove from the list of active alarms upon returning
	isRetrying := false
	defer func() {
		h.activeAlarmsLock.Lock()
		delete(h.activeAlarms, key)

		// If it's retrying, we add it to the list of retrying alarms
		if isRetrying {
			h.retryingAlarms[key] = struct{}{}
		} else {
			delete(h.retryingAlarms, key)
		}

		h.activeAlarmsLock.Unlock()
	}()

	status, ok := statusAny.(executeAlarmStatus)
	if !ok {
		// If result was not executeAlarmStatus, it means that something failed getting the actor
		// We'll retry
		status = executeAlarmStatusRetryable
	}
	switch status {
	case executeAlarmStatusAbandoned:
		// Nothing to do here - the alarm was abandoned (either edited, or we lost the lease)
		// Whatever the case, we don't need to do anything else
		log.Warn("Lease was lost for alarm - skipping execution")
		return

	case executeAlarmStatusFatal:
		// Fatal error - we delete the alarm
		log.Error("Fatal error executing alarm - alarm will be removed", slog.Any("error", err))
		err = h.actorProvider.DeleteLeasedAlarm(ctx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			// Log the error only - we are in background goroutine
			// Note we ignore ErrNoAlarm since that means the lease was lost or the alarm was deleted in the meanwhile
			log.Error("Error deleting leased alarm after fatal error", slog.Any("error", err))
		}
		return

	case executeAlarmStatusRetryable:
		// We can retry this
		h.log.Warn("Error executing alarm - will retry", slog.Any("error", err))
		// We still hold the lease, so just increment the due time and add re-add it to the queue
		// We increment it by taking the initial retry delay and multiplying it by 1.5^attempts, with a max of 10
		multiplier := min(math.Pow(1.5, float64(lease.Attempts())), 10)
		delay := h.actorsConfig[ref.ActorType].InitialRetryDelay * time.Duration(multiplier)
		lease.IncreaseAttempts(h.clock.Now().Add(delay))
		err = h.alarmProcessor.Enqueue(lease)
		if err != nil {
			// Log the error only - we are in background goroutine
			log.Error("Error re-enqueueing alarm", slog.Any("error", err))
		}

		// Here, we add it to the list of alarms that we're retrying
		// Otherwise, if the lease renewal happens in background, it will clear the attempt counter
		isRetrying = true
		return

	case executeAlarmStatusCompleted:
		// Complete the alarm
		err = h.completeAlarm(ctx, lease, log)
		if err != nil {
			// Log the error only - we are in background goroutine
			log.Error("Error completing alarm", slog.Any("error", err))
		}
		return

	default:
		// Indicates a development-time error
		panic(fmt.Errorf("unknown alarm completion status: %v", statusAny))
	}
}

func (h *Host) completeAlarm(parentCtx context.Context, lease *ref.AlarmLease, log *slog.Logger) error {
	// First, retrieve the alarm again
	// We do this again first to check that the alarm is repeating, and second to make sure our lease is still valid
	// In fact, the actor itself may have modified the alarm after executing it
	ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	defer cancel()
	alarm, err := h.actorProvider.GetLeasedAlarm(ctx, lease)
	if errors.Is(err, components.ErrNoAlarm) {
		// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
		// Let's just return no error
		return nil
	} else if err != nil {
		// Something went wrong
		return fmt.Errorf("error retrieving alarm from provider: %w", err)
	}

	// Check if the alarm repeats
	next := alarm.NextExecution(lease.ExecutionTime())
	if next.IsZero() {
		log.Debug("Removing completed alarm")

		// Alarm doesn't repeat, delete it as it's completed
		ctx, cancel = context.WithTimeout(parentCtx, h.providerRequestTimeout)
		defer cancel()
		err = h.actorProvider.DeleteLeasedAlarm(ctx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
			// We can ignore that error
			return fmt.Errorf("error removing completed alarm in provider: %w", err)
		}

		// We're done!
		return nil
	}

	// If we're here, the alarm repeats, so we need to update it instead
	updateReq := components.UpdateLeasedAlarmReq{
		DueTime: next,
	}

	// If the due time is within alarmsPollInterval, we can preserve the lease
	if next.Sub(h.clock.Now()) <= h.alarmsPollInterval {
		updateReq.RefreshLease = true
	}

	log.Debug("Re-scheduling alarm for next iteration", slog.Any("due", next), slog.Bool("leased", updateReq.RefreshLease))

	// Do the update
	ctx, cancel = context.WithTimeout(parentCtx, h.providerRequestTimeout)
	defer cancel()
	err = h.actorProvider.UpdateLeasedAlarm(ctx, lease, updateReq)
	if errors.Is(err, components.ErrNoAlarm) {
		// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
		// Let's just return no error
		return nil
	} else if err != nil {
		// Something went wrong
		return fmt.Errorf("error updating alarm in provider: %w", err)
	}

	// If we kept the lease, re-enqueue the alarm
	if updateReq.RefreshLease {
		err = h.enqueueAlarms([]*ref.AlarmLease{lease})
		if err != nil {
			return fmt.Errorf("error re-enqueueing leased alarm: %w", err)
		}
	}

	// All done here too
	return nil
}

func (h *Host) runLeaseRenewal(parentCtx context.Context) (err error) {
	var res components.RenewAlarmLeasesRes

	// Renew the alarm leases on a loop
	interval := h.actorProvider.RenewLeaseInterval()
	h.log.DebugContext(parentCtx, "Starting background alarm lease renewal", slog.Any("interval", interval))
	defer h.log.Debug("Stopped background lease renewal")

	t := h.clock.NewTicker(interval)
	defer t.Stop()

	hostList := []string{h.hostID}
	for {
		select {
		case <-t.C():
			ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
			defer cancel()
			res, err = h.actorProvider.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{
				Hosts: hostList,
			})
			if err != nil {
				// Log the error only
				h.log.ErrorContext(parentCtx, "Error while renewing leases for alarms", slog.Any("error", err))
			} else if len(res.Leases) > 0 {
				h.log.DebugContext(parentCtx, "Renewed alarm leases", slog.Int("count", len(res.Leases)))

				// Re-enqueue all leases
				// The method is safe for concurrent access
				err = h.enqueueAlarms(res.Leases)
				if err != nil {
					h.log.ErrorContext(parentCtx, "Error while re-enqueueing alarms", slog.Any("error", err))
				}
			}
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
