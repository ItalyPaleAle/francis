package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/italypaleale/go-kit/eventqueue"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
	"github.com/italypaleale/francis/protocol"
)

// runAlarmFetcher periodically fetches and leases upcoming alarms for the hosts connected to this runtime
// It dispatches each due alarm to the host that owns its actor
func (rt *Runtime) runAlarmFetcher(ctx context.Context) error {
	rt.log.DebugContext(ctx, "Starting background alarm fetcher", slog.Any("interval", rt.alarmsPollInterval))
	defer rt.log.Debug("Stopped background alarm fetcher")

	// Clear any draining state from a previous run
	rt.activeAlarmsLock.Lock()
	rt.alarmsDraining = false
	rt.activeAlarmsLock.Unlock()

	// Start the processor that fires alarms at their due time
	rt.alarmProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		ExecuteFn: rt.executeAlarm,
	})
	defer func() {
		apErr := rt.alarmProcessor.Close()
		if apErr != nil {
			rt.log.Error("Failed to close alarm processor", slog.Any("error", apErr))
		}

		// Intentionally leave the field set to the closed processor rather than setting it to nil, to prevent race conditions
		// An in-flight execution that outlives the grace-period drain may still reach the re-enqueue path, where a closed processor returns ErrProcessorStopped from Enqueue while a nil one would panic
	}()

	t := rt.clock.NewTicker(rt.alarmsPollInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			err := rt.fetchAndEnqueueAlarms(ctx)
			if err != nil {
				// Log errors
				rt.log.ErrorContext(ctx, "Failed to fetch alarms", slog.Any("error", err))
			}
		case <-ctx.Done():
			// Stop accepting new alarms and let in-flight executions finish before tearing down the processor
			rt.drainActiveAlarms()
			return ctx.Err()
		}
	}
}

// drainActiveAlarms stops new alarm executions from starting and waits for in-flight ones to finish
// The wait is bounded by the shutdown grace period, after which remaining alarms are left to their leases expiring
func (rt *Runtime) drainActiveAlarms() {
	rt.activeAlarmsLock.Lock()
	rt.alarmsDraining = true
	inflight := len(rt.activeAlarms)
	rt.activeAlarmsLock.Unlock()

	if inflight == 0 {
		return
	}

	rt.log.Info("Waiting for in-flight alarms to finish before shutting down",
		slog.Int("count", inflight),
		slog.Duration("gracePeriod", rt.shutdownGracePeriod),
	)

	done := make(chan struct{})
	go func() {
		rt.alarmWg.Wait()
		close(done)
	}()

	timer := rt.clock.NewTimer(rt.shutdownGracePeriod)
	defer timer.Stop()
	select {
	case <-done:
		rt.log.Debug("All in-flight alarms finished")
	case <-timer.C():
		rt.log.Warn("Timed out waiting for in-flight alarms to finish before shutting down")
	}
}

// fetchAndEnqueueAlarms leases upcoming alarms scoped to the currently connected hosts and enqueues them
func (rt *Runtime) fetchAndEnqueueAlarms(ctx context.Context) error {
	// Scope the fetch to the hosts connected to this runtime, excluding draining ones
	hosts := rt.hosts.ConnectedHostIDs()
	if len(hosts) == 0 {
		return nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
	defer cancel()
	leases, err := rt.provider.FetchAndLeaseUpcomingAlarms(fetchCtx, components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: hosts,
	})
	if err != nil {
		return fmt.Errorf("error fetching alarms: %w", err)
	}

	return rt.enqueueAlarms(leases...)
}

// enqueueAlarms schedules leases for dispatch, executing immediately any that are already due
// Alarms already active or retrying are skipped so their in-memory retry state is not clobbered
func (rt *Runtime) enqueueAlarms(leases ...*ref.AlarmLease) error {
	rt.activeAlarmsLock.Lock()

	// Do not schedule new work once the runtime is draining for shutdown
	if rt.alarmsDraining {
		rt.activeAlarmsLock.Unlock()
		return nil
	}

	var i int
	for _, a := range leases {
		_, ok := rt.activeAlarms[a.Key()]
		if ok {
			continue
		}
		_, ok = rt.retryingAlarms[a.Key()]
		if ok {
			continue
		}

		// If the alarm is due within the next 0.1ms, execute it right away rather than enqueueing it
		if a.DueTime().Sub(rt.clock.Now()) < 100*time.Microsecond {
			rt.activeAlarms[a.Key()] = struct{}{}
			rt.alarmWg.Add(1)
			go rt.executeActiveAlarm(a)
			continue
		}

		leases[i] = a
		i++
	}

	rt.activeAlarmsLock.Unlock()

	if i > 0 {
		err := rt.alarmProcessor.Enqueue(leases[:i]...)
		if err != nil {
			return fmt.Errorf("error enqueueing alarms: %w", err)
		}
	}

	return nil
}

type executeAlarmStatus int

const (
	// executeAlarmStatusCompleted means the alarm or job ran successfully
	executeAlarmStatusCompleted = executeAlarmStatus(iota)
	// executeAlarmStatusFatal means the occurrence failed terminally, so an alarm is deleted and a job is dead-lettered
	executeAlarmStatusFatal
	// executeAlarmStatusRetryable means the occurrence failed but may succeed later, so it is retried with backoff
	executeAlarmStatusRetryable
	// executeAlarmStatusAbandoned means the lease was lost or the occurrence no longer exists, so there is nothing to do
	executeAlarmStatusAbandoned
	// executeAlarmStatusReleased means the owning host declined a job occurrence (its capacity group was full, or the handler returned ErrJobRejected) and it must be handed back for another host to run
	executeAlarmStatusReleased
)

// executeAlarm is the alarm processor callback invoked when an alarm is due
func (rt *Runtime) executeAlarm(lease *ref.AlarmLease) {
	rt.activeAlarmsLock.Lock()
	// Do not start a new execution once the runtime is draining for shutdown
	if rt.alarmsDraining {
		rt.activeAlarmsLock.Unlock()
		return
	}
	_, active := rt.activeAlarms[lease.Key()]
	if active {
		rt.activeAlarmsLock.Unlock()
		return
	}
	rt.activeAlarms[lease.Key()] = struct{}{}
	rt.alarmWg.Add(1)
	rt.activeAlarmsLock.Unlock()

	// Run each execution on its own goroutine
	go rt.executeActiveAlarm(lease)
}

// executeActiveAlarm dispatches an alarm to its owning host and handles the outcome
func (rt *Runtime) executeActiveAlarm(lease *ref.AlarmLease) {
	// Release the shutdown drain barrier when this execution finishes
	defer rt.alarmWg.Done()

	// Use a background context rather than the runtime context so an in-flight execution is allowed to finish during the shutdown grace-period drain
	// The host round-trip, which is the only unbounded step, is bounded separately in dispatchAlarm
	ctx := context.Background()

	// Alarm dispatch is driven by the runtime poller, not a caller request, so it begins its own trace that the owning host continues
	ctx, span := tracing.Start(ctx, "alarm.dispatch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			tracing.ActorRef(lease.ActorRef().String()),
			tracing.AlarmName(lease.AlarmRef().Name),
		),
	)
	defer span.End()

	key := lease.Key()
	log := rt.log.With(
		slog.String("id", lease.AlarmRef().String()),
		slog.Any("due", lease.DueTime()),
	)
	log.Debug("Dispatching alarm")

	// Job-specific state captured during dispatch so the terminal-failure path can dead-letter the job and fire its JobFailed hook
	var jobInfo jobExecInfo
	status, err := rt.dispatchAlarm(ctx, lease, &jobInfo)

	// Remove from the active set on return
	// We track retrying alarms so lease renewal does not reset their attempts
	// A repeating alarm whose lease we kept is re-enqueued here, only after the active flag is cleared, otherwise enqueueAlarms would skip it as already active
	isRetrying := false
	reEnqueue := false
	defer func() {
		rt.activeAlarmsLock.Lock()
		delete(rt.activeAlarms, key)
		if isRetrying {
			rt.retryingAlarms[key] = struct{}{}
		} else {
			delete(rt.retryingAlarms, key)
		}
		rt.activeAlarmsLock.Unlock()

		if reEnqueue {
			enqErr := rt.enqueueAlarms(lease)
			if enqErr != nil {
				log.Error("Error re-enqueueing leased alarm", slog.Any("error", enqErr))
			}
		}
	}()

	switch status {
	case executeAlarmStatusAbandoned:
		// The lease was lost, the actor is no longer active, or its host is gone
		// Nothing to do: a lease we no longer renew expires and another runtime can pick it up
		log.Warn("Alarm abandoned - skipping execution", slog.Any("error", err))
		return

	case executeAlarmStatusFatal:
		// For jobs, a terminal failure dead-letters the occurrence rather than silently dropping it
		if jobInfo.isJob {
			rt.deadLetterJob(ctx, lease, jobInfo, err, log)
			return
		}

		log.Error("Fatal error executing alarm - alarm will be removed", slog.Any("error", err))
		delErr := rt.provider.DeleteLeasedAlarm(ctx, lease)
		if delErr != nil && !errors.Is(delErr, components.ErrNoAlarm) {
			log.Error("Error deleting leased alarm after fatal error", slog.Any("error", delErr))
		}
		return

	case executeAlarmStatusRetryable:
		log.Warn("Error executing alarm - will retry", slog.Any("error", err))
		// We still hold the lease, so push the due time out with exponential backoff and re-enqueue
		// #nosec G404 -- not security-sensitive
		jitter := rand.Float64()*0.2 + 0.9
		multiplier := min(math.Pow(1.5, float64(lease.Attempts())), 10) * jitter
		delay := rt.initialRetryDelay(lease.ActorRef().ActorType) * time.Duration(multiplier)
		lease.IncreaseAttempts(rt.clock.Now().Add(delay))

		// Do not re-enqueue once the runtime is draining: the processor is being torn down, so let the lease expire and another replica pick the alarm up
		rt.activeAlarmsLock.Lock()
		draining := rt.alarmsDraining
		rt.activeAlarmsLock.Unlock()
		if !draining {
			enqErr := rt.alarmProcessor.Enqueue(lease)
			if enqErr != nil {
				log.Error("Error re-enqueueing alarm", slog.Any("error", enqErr))
			}
			isRetrying = true
		}
		return

	case executeAlarmStatusReleased:
		// The owning host declined this occurrence (capacity group full, or the handler returned ErrJobRejected)
		// Hand it back so another host runs it, without counting an attempt or dead-lettering
		log.Debug("Job occurrence released for re-routing", slog.Any("error", err))
		rt.releaseForReroute(ctx, lease, log)
		return

	case executeAlarmStatusCompleted:
		rt.metrics.alarmsExecuted.Add(ctx, 1)
		var compErr error
		reEnqueue, compErr = rt.completeAlarm(ctx, lease, log)
		if compErr != nil {
			log.Error("Error completing alarm", slog.Any("error", compErr))
		}
		return

	default:
		panic(fmt.Errorf("unknown alarm completion status: %v", status))
	}
}

// jobExecInfo carries the job details captured during dispatch, used by the terminal-failure path to dead-letter the job and fire its JobFailed hook
type jobExecInfo struct {
	isJob  bool
	method string
	data   []byte
	props  ref.AlarmProperties
}

// dispatchAlarm resolves the owning host, rechecks the lease, sends ExecuteAlarm, and classifies the response
func (rt *Runtime) dispatchAlarm(parentCtx context.Context, lease *ref.AlarmLease, jobInfo *jobExecInfo) (executeAlarmStatus, error) {
	aRef := lease.ActorRef()

	// Find the host the actor is active on
	// The fetch that produced this lease placed it on a connected host
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	lar, err := rt.provider.LookupActor(ctx, aRef, components.LookupActorOpts{ActiveOnly: true})
	cancel()
	if errors.Is(err, components.ErrNoActor) {
		return executeAlarmStatusAbandoned, nil
	} else if err != nil {
		return executeAlarmStatusRetryable, fmt.Errorf("error looking up actor host: %w", err)
	}

	// The owning host must be connected to this runtime and not draining
	conn, ok := rt.hosts.Get(lar.HostID)
	if !ok || conn.IsDraining() {
		// Proactively release the lease so another runtime replica can pick up the alarm on the next poll
		// Without this, the lease must expire naturally (up to one full lease duration) before another replica can claim the alarm
		releaseCtx, releaseCancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
		_ = rt.provider.ReleaseAlarmLease(releaseCtx, lease)
		releaseCancel()
		return executeAlarmStatusAbandoned, nil
	}

	// Recheck the leased alarm before dispatch, both to validate the lease and to read the current data
	ctx, cancel = context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	alarm, err := rt.provider.GetLeasedAlarm(ctx, lease)
	cancel()
	if errors.Is(err, components.ErrNoAlarm) {
		return executeAlarmStatusAbandoned, nil
	} else if err != nil {
		return executeAlarmStatusRetryable, fmt.Errorf("error retrieving leased alarm: %w", err)
	}

	// Record the execution time before dispatch,  a successful response may refine it
	lease.SetExecutionTime(rt.clock.Now())

	// Capture the job details so the terminal-failure path can dead-letter the job
	if alarm.Kind == components.AlarmKindJob {
		jobInfo.isJob = true
		jobInfo.method = alarm.JobMethod
		jobInfo.data = alarm.Data
		jobInfo.props = alarm.AlarmProperties
	}

	req, err := protocol.NewRequest(protocol.KindExecuteAlarm, protocol.ExecuteAlarmRequest{
		ActorType:     aRef.ActorType,
		ActorID:       aRef.ActorID,
		Name:          alarm.Name,
		AlarmID:       lease.Key(),
		DueTimeUnixMs: lease.DueTime().UnixMilli(),
		Attempts:      lease.Attempts(),
		Data:          alarm.Data,
		Kind:          string(alarm.Kind),
		JobMethod:     alarm.JobMethod,
	})
	if err != nil {
		return executeAlarmStatusRetryable, fmt.Errorf("error encoding execute alarm request: %w", err)
	}

	// Bound the host round-trip so a hung or half-open host cannot block the dispatch forever
	// This is the call most likely to hang, since the host runs the actor's alarm handler before replying
	sendCtx, sendCancel := context.WithTimeout(parentCtx, rt.alarmExecutionTimeout)
	resp, err := rt.sendToHost(sendCtx, conn, req)
	sendCancel()
	if err != nil {
		// The stream or session broke, or the round-trip timed out
		// Retry later, and if the host has truly gone the next attempt abandons it
		return executeAlarmStatusRetryable, fmt.Errorf("error dispatching alarm to host: %w", err)
	}

	// An error response is retryable up to the actor type's max attempts, unless it is definitively fatal
	perr, isErr := resp.AsError()
	if isErr {
		return rt.classifyAlarmError(conn, aRef.ActorType, lease, perr), fmt.Errorf("host returned an error executing alarm: %w", perr)
	}

	var out protocol.ExecuteAlarmResponse
	err = resp.DecodePayload(&out)
	if err == nil && out.ExecutionTimeUnixMs > 0 {
		lease.SetExecutionTime(time.UnixMilli(out.ExecutionTimeUnixMs))
	}

	return executeAlarmStatusCompleted, nil
}

// classifyAlarmError decides whether a host's alarm error should be retried or is fatal
func (rt *Runtime) classifyAlarmError(conn *hostConn, actorType string, lease *ref.AlarmLease, perr *protocol.Error) executeAlarmStatus {
	switch perr.Code {
	case protocol.ErrCodeCapacityExhausted, protocol.ErrCodeJobRejected:
		// The host declined the occurrence: re-route it to another host rather than retrying here or dead-lettering
		return executeAlarmStatusReleased
	case protocol.ErrCodeActorTypeUnsupported, protocol.ErrCodeInvokeModeUnsupported, protocol.ErrCodeJobPermanentFailure:
		// These errors can never succeed on retry
		// For jobs, a permanent-failure also skips the remaining retries
		return executeAlarmStatusFatal
	default:
		// Otherwise retry until the actor type's max attempts are exhausted
		maxAttempts := 0
		at, ok := conn.actorTypeConfig(actorType)
		if ok {
			maxAttempts = at.MaxAttempts
		}
		if lease.Attempts() >= maxAttempts {
			return executeAlarmStatusFatal
		}
		return executeAlarmStatusRetryable
	}
}

// deadLetterJob moves a terminally-failed job occurrence to the dead-letter store, then asks the owning host to run its JobFailed hook best-effort
// For a repeating job the recurrence is rescheduled in the same provider transaction, so a single failed occurrence does not stop the recurrence
func (rt *Runtime) deadLetterJob(parentCtx context.Context, lease *ref.AlarmLease, info jobExecInfo, jobErr error, log *slog.Logger) {
	req := components.DeadLetterAlarmReq{
		Reason:   jobErr.Error(),
		Attempts: lease.Attempts(),
	}

	// Keep a repeating job's recurrence alive by rescheduling its next occurrence as part of the dead-letter move
	next, err := info.props.NextExecution(lease.ExecutionTime())
	if err != nil {
		log.Error("Failed to compute next execution for repeating job; recurrence will not continue", slog.Any("error", err))
	} else if !next.IsZero() {
		req.Reschedule = true
		req.NextDueTime = next
	}

	log.Error("Job failed permanently - dead-lettering", slog.Any("error", jobErr))
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	err = rt.provider.DeadLetterAlarm(ctx, lease, req)
	cancel()
	if errors.Is(err, components.ErrNoAlarm) {
		// The lease was lost or the occurrence was already moved, so there is nothing else to do
		return
	} else if err != nil {
		log.Error("Error dead-lettering job", slog.Any("error", err))
		return
	}

	// The dead-letter record is now the source of truth, so the JobFailed hook is best-effort
	rt.pushJobFailed(parentCtx, lease, info, jobErr, log)
}

// releaseForReroute hands a declined job occurrence back to the pool so another host can run it
// It clears the actor's placement so the re-fetch reroutes instead of returning to the same host, then drops the lease with a short backoff so a cluster with no free capacity does not spin re-leasing it
// It never counts an attempt or dead-letters, since a reject is not a failure
func (rt *Runtime) releaseForReroute(parentCtx context.Context, lease *ref.AlarmLease, log *slog.Logger) {
	aRef := lease.ActorRef()

	// Clear the actor's placement so the occurrence is re-allocated to a host with free capacity rather than back to the one that declined it
	// The declining host also halts the actor, which removes it too, so this is idempotent belt-and-suspenders against the two racing
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	err := rt.provider.RemoveActor(ctx, aRef)
	cancel()
	if err != nil && !errors.Is(err, components.ErrNoActor) {
		log.Warn("Failed to clear actor placement while releasing job for re-route", slog.Any("error", err))
	}

	// Push the due time out by a jittered backoff and drop the lease
	// The lease is still held until this update, so no other host claims the occurrence in between
	ctx, cancel = context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	next := rt.clock.Now().Add(rerouteBackoff(rt.alarmsPollInterval))
	err = rt.provider.UpdateLeasedAlarm(ctx, lease, components.UpdateLeasedAlarmReq{DueTime: next})
	cancel()
	if err != nil && !errors.Is(err, components.ErrNoAlarm) {
		// Log only: if this fails the lease simply expires on its own and the occurrence is re-fetched later
		log.Warn("Failed to release lease for re-route", slog.Any("error", err))
	}
}

// rerouteBackoff returns the delay before a released occurrence becomes due again
// It scales with the configured alarm poll interval so a re-route reaches another host on its next poll rather than immediately, with jitter to spread re-fetches across hosts and avoid a full cluster spinning on the same occurrence
func rerouteBackoff(pollInterval time.Duration) time.Duration {
	// #nosec G404 -- not security-sensitive, only used to spread re-route timing
	jitter := 1.0 + rand.Float64()
	return time.Duration(float64(pollInterval) * jitter)
}

// pushJobFailed asks the host currently owning the actor to run the actor's JobFailed hook, best-effort
func (rt *Runtime) pushJobFailed(parentCtx context.Context, lease *ref.AlarmLease, info jobExecInfo, jobErr error, log *slog.Logger) {
	aRef := lease.ActorRef()

	// Resolve the host the actor is active on, without activating it
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	lar, err := rt.provider.LookupActor(ctx, aRef, components.LookupActorOpts{
		ActiveOnly: true,
	})
	cancel()
	if err != nil {
		// The actor is no longer active, so there is no host to run the hook
		log.Debug("Skipping JobFailed hook: actor is not active", slog.Any("error", err))
		return
	}

	conn, ok := rt.hosts.Get(lar.HostID)
	if !ok || conn.IsDraining() {
		return
	}

	// Create the request
	req, err := protocol.NewRequest(protocol.KindJobFailed, protocol.JobFailedRequest{
		ActorType:    aRef.ActorType,
		ActorID:      aRef.ActorID,
		JobID:        lease.Key(),
		Method:       info.method,
		Data:         info.data,
		ErrorMessage: jobErr.Error(),
	})
	if err != nil {
		return
	}

	// Send to the actor host
	ctx, cancel = context.WithTimeout(parentCtx, rt.alarmExecutionTimeout)
	_, err = rt.sendToHost(ctx, conn, req)
	cancel()
	if err != nil {
		log.Warn("Failed to deliver JobFailed hook to host", slog.Any("error", err))
	}
}

// initialRetryDelay returns the initial retry delay advertised for an actor type by any connected host
func (rt *Runtime) initialRetryDelay(actorType string) time.Duration {
	for _, hostID := range rt.hosts.ConnectedHostIDs() {
		conn, ok := rt.hosts.Get(hostID)
		if !ok {
			continue
		}

		at, ok := conn.actorTypeConfig(actorType)
		if ok && at.InitialRetryDelayMs > 0 {
			return time.Duration(at.InitialRetryDelayMs) * time.Millisecond
		}
	}
	return time.Second
}

// completeAlarm reschedules a repeating alarm or deletes a one-shot alarm after a successful execution
// It returns true when the lease was kept for its next occurrence and must be re-enqueued by the caller once the active flag is cleared
func (rt *Runtime) completeAlarm(parentCtx context.Context, lease *ref.AlarmLease, log *slog.Logger) (bool, error) {
	// Re-read the alarm to confirm the lease is still valid and to observe any edits the actor made
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	alarm, err := rt.provider.GetLeasedAlarm(ctx, lease)
	cancel()
	if errors.Is(err, components.ErrNoAlarm) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error retrieving alarm from provider: %w", err)
	}

	// A non-repeating alarm is deleted once executed
	next, err := alarm.NextExecution(lease.ExecutionTime())
	if err != nil {
		// The stored interval is corrupt
		// Keep the alarm rather than silently deleting it
		log.Error("Failed to compute next execution time for alarm; alarm will be kept", slog.Any("error", err))
		return false, nil
	}
	if next.IsZero() {
		log.Debug("Removing completed alarm")
		ctx, cancel = context.WithTimeout(parentCtx, rt.providerRequestTimeout)
		defer cancel()
		err = rt.provider.DeleteLeasedAlarm(ctx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			return false, fmt.Errorf("error removing completed alarm in provider: %w", err)
		}
		return false, nil
	}

	// A repeating alarm is rescheduled for its next occurrence
	updateReq := components.UpdateLeasedAlarmReq{
		DueTime: next,
	}

	// Keep the lease if the next occurrence is within one poll interval, so we can dispatch it without a re-fetch
	if next.Sub(rt.clock.Now()) <= rt.alarmsPollInterval {
		updateReq.RefreshLease = true
	}

	log.Debug("Re-scheduling alarm for next iteration", slog.Any("due", next), slog.Bool("leased", updateReq.RefreshLease))

	ctx, cancel = context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.UpdateLeasedAlarm(ctx, lease, updateReq)
	if errors.Is(err, components.ErrNoAlarm) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error updating alarm in provider: %w", err)
	}

	// If we kept the lease, advance the in-memory due time to the next occurrence and signal the caller to re-enqueue it
	// The actual re-enqueue happens after the active flag is cleared, otherwise enqueueAlarms would skip it as already active
	if updateReq.RefreshLease {
		lease.ResetForNextExecution(next)
		return true, nil
	}

	return false, nil
}

// runLeaseRenewal periodically renews the leases for alarms owned by connected hosts
// When a host disconnects it leaves the connected set, so its leases are no longer renewed and naturally expire
func (rt *Runtime) runLeaseRenewal(parentCtx context.Context) error {
	interval := rt.provider.RenewLeaseInterval()
	rt.log.DebugContext(parentCtx, "Starting background alarm lease renewal", slog.Any("interval", interval))
	defer rt.log.Debug("Stopped background lease renewal")

	t := rt.clock.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			hosts := rt.hosts.ConnectedHostIDs()
			if len(hosts) == 0 {
				continue
			}

			ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
			res, err := rt.provider.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{
				Hosts: hosts,
			})
			cancel()
			if err != nil {
				rt.log.ErrorContext(parentCtx, "Error while renewing leases for alarms", slog.Any("error", err))
				continue
			}

			if len(res.Leases) > 0 {
				rt.log.DebugContext(parentCtx, "Renewed alarm leases", slog.Int("count", len(res.Leases)))
				err = rt.enqueueAlarms(res.Leases...)
				if err != nil {
					rt.log.ErrorContext(parentCtx, "Error while re-enqueueing alarms", slog.Any("error", err))
				}
			}
		case <-parentCtx.Done():
			return parentCtx.Err()
		}
	}
}
