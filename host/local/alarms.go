package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
)

func (h *Host) runAlarmFetcher(ctx context.Context) error {
	h.log.DebugContext(ctx, "Starting background alarm fetcher", slog.Any("interval", h.alarmsPollInterval))
	defer h.log.Debug("Stopped background alarm fetcher")

	// Close the processor when the fetcher exits, then wait for all in-flight alarm goroutines to finish
	// The processor is intentionally left non-nil after close so in-flight re-enqueues receive ErrProcessorStopped instead of panicking
	defer func() {
		apErr := h.alarmProcessor.Close()
		if apErr != nil {
			h.log.Error("Failed to close alarm processor", slog.Any("error", apErr))
		}

		// Drain goroutines spawned by both the processor callback and the immediate-fire path
		h.alarmWg.Wait()
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
			h.alarmWg.Add(1)
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
	// Mark the alarm as active before spawning the goroutine so concurrent processor firings cannot start duplicate executions
	h.activeAlarmsLock.Lock()
	_, active := h.activeAlarms[lease.Key()]
	if active {
		// Already active, so nothing to do
		h.activeAlarmsLock.Unlock()
		return
	}

	h.activeAlarms[lease.Key()] = struct{}{}
	h.alarmWg.Add(1)
	h.activeAlarmsLock.Unlock()

	// Now we can execute the alarm
	// Each execution runs on its own goroutine so the processor loop can continue without blocking
	go h.executeActiveAlarm(lease)
}

// Executes an active alarm, that is already in the activeAlarms map
func (h *Host) executeActiveAlarm(lease *ref.AlarmLease) {
	// Release the shutdown drain barrier when this execution finishes
	defer h.alarmWg.Done()

	// Use a background context so an in-flight execution can finish during the shutdown grace period
	// Individual operations are bounded by providerRequestTimeout
	ctx := context.Background()

	// A fired alarm is not triggered by a caller request, so it begins its own trace
	ctx, span := tracing.Start(ctx, "alarm.execute",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			tracing.ActorRef(lease.ActorRef().String()),
			tracing.AlarmName(lease.AlarmRef().Name),
		),
	)
	defer span.End()

	key := lease.Key()
	log := h.log.With(
		slog.String("id", lease.AlarmRef().String()),
		slog.Any("due", lease.DueTime()),
	)
	log.Debug("Executing alarm")

	// Job-specific state captured inside the closure so the terminal-failure path can dead-letter the job and fire its JobFailed hook
	var (
		isJob     bool
		jobMethod string
		jobData   []byte
		jobProps  ref.AlarmProperties
	)

	// Get and lock the actor
	aRef := lease.ActorRef()
	statusAny, err := h.core.LockAndInvoke(ctx, aRef, func(parentCtx context.Context, act *actorcore.ActiveActor) (any, error) {
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

		// Jobs ride on the alarm engine but differ in delivery (Job vs Alarm) and terminal-failure handling (dead-letter vs delete)
		if a.Kind == components.AlarmKindJob {
			return h.executeJob(parentCtx, lease, act, a, &isJob, &jobMethod, &jobData, &jobProps)
		}

		// Ensure the actor implements the Alarm method
		obj, ok := act.Instance.(actor.ActorAlarm)
		if !ok {
			// This is a fatal error, which causes us to drop the alarm since there's no way it can be completed
			return executeAlarmStatusFatal, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		var data actor.Envelope
		if len(a.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(a.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		// Mark the alarm as executed now
		lease.SetExecutionTime(h.clock.Now())

		// Stamp a per-occurrence key (alarm ID + due-time ms) into the context so the actor can detect
		// duplicate deliveries of the same occurrence without confusing them with legitimate subsequent
		// firings of a repeating alarm (which have a different due time)
		alarmCtx := actor.WithRequestID(parentCtx, alarmRequestID(lease))

		// Invoke the actor
		err = obj.Alarm(alarmCtx, a.Name, data)
		if err != nil {
			// Consider this as a retryable condition unless we've exhausted the configured max attempts
			code := executeAlarmStatusRetryable
			maxAttempts := h.core.ActorsConfig[aRef.ActorType].MaxAttempts
			if lease.Attempts() >= maxAttempts {
				code = executeAlarmStatusFatal
			}
			return code, fmt.Errorf("error from actor (attempt %d of %d): %w", lease.Attempts(), maxAttempts, err)
		}

		return executeAlarmStatusCompleted, nil
	})

	// Remove from the list of active alarms upon returning
	isRetrying := false
	reEnqueue := false
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

		// Re-enqueue a repeating alarm whose lease we kept, only after the active flag is cleared above
		// Otherwise enqueueAlarms would skip it as already active and the alarm would never re-fire on schedule
		if reEnqueue {
			enqErr := h.enqueueAlarms([]*ref.AlarmLease{lease})
			if enqErr != nil {
				log.Error("Error re-enqueueing leased alarm", slog.Any("error", enqErr))
			}
		}
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
		// For jobs, a terminal failure dead-letters the occurrence rather than silently dropping it
		if isJob {
			h.deadLetterJob(ctx, lease, jobProps, jobMethod, jobData, err, log)
			return
		}

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
		// We increment it by taking the initial retry delay and multiplying it by 1.5^attempts, with a max of 10, and some jitter
		// Disable the "G404: Use of weak random number generator " gosec warning, since this is not used for anything security-related
		// #nosec G404
		jitter := rand.Float64()*0.2 + 0.9
		multiplier := min(math.Pow(1.5, float64(lease.Attempts())), 10) * jitter
		delay := h.core.ActorsConfig[aRef.ActorType].InitialRetryDelay * time.Duration(multiplier)
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
		// Complete the alarm, re-enqueuing it from the deferred cleanup when its lease was kept for the next occurrence
		reEnqueue, err = h.completeAlarm(ctx, lease, log)
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

// executeJob delivers a job occurrence to the actor's Job method, classifying the outcome like an alarm execution
// It records the job's method, data, and schedule into the caller's variables so the terminal-failure path can dead-letter the occurrence and fire the JobFailed hook
func (h *Host) executeJob(parentCtx context.Context, lease *ref.AlarmLease, act *actorcore.ActiveActor, a components.GetLeasedAlarmRes, isJob *bool, jobMethod *string, jobData *[]byte, jobProps *ref.AlarmProperties) (executeAlarmStatus, error) {
	aRef := lease.ActorRef()

	// Capture the job details for the terminal-failure path, which runs after this closure returns
	*isJob = true
	*jobMethod = a.JobMethod
	*jobData = a.Data
	*jobProps = a.AlarmProperties

	// Mark the occurrence as executed now, so the dead-letter path can compute the next occurrence even on a hard failure
	lease.SetExecutionTime(h.clock.Now())

	// The actor must implement the Job method to receive jobs
	obj, ok := act.Instance.(actor.ActorJob)
	if !ok {
		// A hard-fatal error: the actor type can never service this job, so it is dead-lettered immediately
		return executeAlarmStatusFatal, fmt.Errorf("actor of type '%s' does not implement the Job method", act.ActorType())
	}

	var data actor.Envelope
	if len(a.Data) > 0 {
		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(a.Data))
		defer msgpack.PutDecoder(dec)
		data = dec
	}

	// Stamp a per-occurrence key (job ID + due-time ms) so the actor can detect duplicate deliveries of the same occurrence
	ctx := actor.WithRequestID(parentCtx, alarmRequestID(lease))

	// Invoke the actor's Job method
	err := obj.Job(ctx, a.JobMethod, data)
	if err != nil {
		// ErrJobPermanentFailure skips the remaining retries and dead-letters the job immediately
		if errors.Is(err, actor.ErrJobPermanentFailure) {
			return executeAlarmStatusFatal, fmt.Errorf("job failed permanently: %w", err)
		}

		// Otherwise retry until the actor type's max attempts are exhausted, then dead-letter
		code := executeAlarmStatusRetryable
		maxAttempts := h.core.ActorsConfig[aRef.ActorType].MaxAttempts
		if lease.Attempts() >= maxAttempts {
			code = executeAlarmStatusFatal
		}

		return code, fmt.Errorf("error from actor job (attempt %d of %d): %w", lease.Attempts(), maxAttempts, err)
	}

	return executeAlarmStatusCompleted, nil
}

// deadLetterJob moves a terminally-failed job occurrence to the dead-letter store, then fires the optional JobFailed hook best-effort
// For a repeating job the recurrence is rescheduled in the same provider transaction, so a single failed occurrence does not stop the recurrence
func (h *Host) deadLetterJob(ctx context.Context, lease *ref.AlarmLease, props ref.AlarmProperties, method string, data []byte, jobErr error, log *slog.Logger) {
	req := components.DeadLetterAlarmReq{
		Reason:   jobErr.Error(),
		Attempts: lease.Attempts(),
	}

	// Keep a repeating job's recurrence alive by rescheduling its next occurrence as part of the dead-letter move
	next, err := props.NextExecution(lease.ExecutionTime())
	if err != nil {
		log.Error("Failed to compute next execution for repeating job; recurrence will not continue", slog.Any("error", err))
	} else if !next.IsZero() {
		req.Reschedule = true
		req.NextDueTime = next
	}

	log.Error("Job failed permanently - dead-lettering", slog.Any("error", jobErr))
	err = h.actorProvider.DeadLetterAlarm(ctx, lease, req)
	if errors.Is(err, components.ErrNoAlarm) {
		// The lease was lost or the occurrence was already moved, so there is nothing else to do
		return
	} else if err != nil {
		// Log the error only - we are in a background goroutine
		log.Error("Error dead-lettering job", slog.Any("error", err))
		return
	}

	// The dead-letter record is now the source of truth, so the JobFailed hook is best-effort
	h.fireJobFailed(ctx, lease, method, data, jobErr)
}

// fireJobFailed delivers the optional JobFailed reaction hook on the actor's turn-based lock, best-effort
// It is never itself dead-lettered: an error here is only logged, leaving the dead_jobs record in place
func (h *Host) fireJobFailed(parentCtx context.Context, lease *ref.AlarmLease, method string, data []byte, jobErr error) {
	aRef := lease.ActorRef()
	jobID := lease.Key()

	_, err := h.core.LockAndInvoke(parentCtx, aRef, func(ctx context.Context, act *actorcore.ActiveActor) (any, error) {
		// The hook is optional, so an actor that does not implement it is a no-op
		obj, ok := act.Instance.(actor.ActorJobFailed)
		if !ok {
			return nil, nil
		}

		var env actor.Envelope
		if len(data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(data))
			defer msgpack.PutDecoder(dec)
			env = dec
		}

		return nil, obj.JobFailed(ctx, jobID, method, env, jobErr)
	})
	if err != nil {
		h.log.Warn("JobFailed hook returned an error; dead-letter record is kept", slog.String("jobId", jobID), slog.Any("error", err))
	}
}

// completeAlarm reschedules a repeating alarm or deletes a one-shot alarm after a successful execution
// It returns true when the lease was kept for the next occurrence and must be re-enqueued by the caller once the active flag is cleared
func (h *Host) completeAlarm(parentCtx context.Context, lease *ref.AlarmLease, log *slog.Logger) (bool, error) {
	// First, retrieve the alarm again
	// We do this again first to check that the alarm is repeating, and second to make sure our lease is still valid
	// In fact, the actor itself may have modified the alarm after executing it
	ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	defer cancel()
	alarm, err := h.actorProvider.GetLeasedAlarm(ctx, lease)
	if errors.Is(err, components.ErrNoAlarm) {
		// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
		// Let's just return no error
		return false, nil
	} else if err != nil {
		// Something went wrong
		return false, fmt.Errorf("error retrieving alarm from provider: %w", err)
	}

	// Check if the alarm repeats
	next, err := alarm.NextExecution(lease.ExecutionTime())
	if err != nil {
		// The stored interval is corrupt
		// Keep the alarm rather than silently deleting it
		log.Error("Failed to compute next execution time for alarm; alarm will be kept", slog.Any("error", err))
		return false, nil
	}
	if next.IsZero() {
		log.Debug("Removing completed alarm")

		// Alarm doesn't repeat, delete it as it's completed
		ctx, cancel = context.WithTimeout(parentCtx, h.providerRequestTimeout)
		defer cancel()
		err = h.actorProvider.DeleteLeasedAlarm(ctx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
			// We can ignore that error
			return false, fmt.Errorf("error removing completed alarm in provider: %w", err)
		}

		// We're done!
		return false, nil
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
		return false, nil
	} else if err != nil {
		// Something went wrong
		return false, fmt.Errorf("error updating alarm in provider: %w", err)
	}

	// If we kept the lease, advance the in-memory due time to the next occurrence and signal the caller to re-enqueue it
	// The actual re-enqueue happens after the active flag is cleared, otherwise enqueueAlarms would skip it as already active
	if updateReq.RefreshLease {
		lease.ResetForNextExecution(next)
		return true, nil
	}

	// The next occurrence is too far out to keep the lease, so a later fetch will pick it up
	return false, nil
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
	err := ref.ValidateComponents(actorType, actorID, name)
	if err != nil {
		return actor.AlarmProperties{}, err
	}

	res, err := h.actorProvider.GetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
	if errors.Is(err, components.ErrNoAlarm) {
		return actor.AlarmProperties{}, actor.ErrAlarmNotFound
	} else if err != nil {
		return actor.AlarmProperties{}, fmt.Errorf("failed to get alarm: %w", err)
	}

	return alarmPropertiesFromAlarmRes(res)
}

func (h *Host) SetAlarm(ctx context.Context, actorType string, actorID string, name string, properties actor.AlarmProperties) error {
	err := ref.ValidateComponents(actorType, actorID, name)
	if err != nil {
		return err
	}

	err = properties.Validate()
	if err != nil {
		return err
	}

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
	err := ref.ValidateComponents(actorType, actorID, name)
	if err != nil {
		return err
	}

	err = h.actorProvider.DeleteAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
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
	}

	if len(res.Data) > 0 {
		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(res.Data))
		defer msgpack.PutDecoder(dec)
		err := dec.Decode(&o.Data)
		if err != nil {
			return actor.AlarmProperties{}, fmt.Errorf("failed to deserialize data using msgpack: %w", err)
		}
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
		},
	}

	if o.Data != nil {
		var data bytes.Buffer
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(&data)

		err := enc.Encode(o.Data)
		if err != nil {
			return components.SetAlarmReq{}, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}

		req.Data = data.Bytes()
	}

	if !o.TTL.IsZero() {
		req.TTL = &o.TTL
	}

	return req, nil
}

func alarmRequestID(lease *ref.AlarmLease) string {
	return lease.Key() + "|" + strconv.FormatInt(lease.DueTime().UnixMilli(), 10)
}
