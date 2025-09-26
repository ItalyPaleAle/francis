package alarmprocessor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/activeactor"
	"github.com/italypaleale/francis/internal/eventqueue"
	"github.com/italypaleale/francis/internal/ref"
)

type LockAndInvokeFn func(parentCtx context.Context, ref ref.ActorRef, fn func(context.Context, *activeactor.Instance) (any, error)) (any, error)

type Processor struct {
	pollInterval           time.Duration
	providerRequestTimeout time.Duration
	actorProvider          components.ActorProvider
	hostIDs                func() []string
	lockAndInvokeFn        LockAndInvokeFn

	// Map of actor configuration objects; key is actor type
	actorsConfig map[string]components.ActorHostType

	queueProcessor *eventqueue.Processor[string, *ref.AlarmLease]

	// List of currently-active alarms
	activeAlarmsLock sync.Mutex
	activeAlarms     map[string]struct{}
	retryingAlarms   map[string]struct{}

	log   *slog.Logger
	clock clock.WithTicker
}

type AlarmProcessorOpts struct {
	// Interval for polling for alarms
	PollInterval time.Duration
	// Timeout for component requests
	ProviderRequestTimeout time.Duration
	// Actor provider component
	ActorProvider components.ActorProvider
	// Function that returns the IDs of active hosts
	HostIDs func() []string
	// Function that locks and invokes an actor
	LockAndInvokeFn LockAndInvokeFn
	// Map of actor configuration objects; key is actor type
	ActorsConfig map[string]components.ActorHostType
	// Logger
	Log *slog.Logger
	// Clock
	Clock clock.WithTicker
}

func NewProcessor(opts AlarmProcessorOpts) *Processor {
	return &Processor{
		pollInterval:           opts.PollInterval,
		providerRequestTimeout: opts.ProviderRequestTimeout,
		actorProvider:          opts.ActorProvider,
		hostIDs:                opts.HostIDs,
		lockAndInvokeFn:        opts.LockAndInvokeFn,
		actorsConfig:           opts.ActorsConfig,
		log:                    opts.Log,
		clock:                  opts.Clock,

		activeAlarms:   map[string]struct{}{},
		retryingAlarms: map[string]struct{}{},
	}
}

func (p *Processor) RunAlarmFetcher(ctx context.Context) error {
	p.log.DebugContext(ctx, "Starting background alarm fetcher", slog.Any("interval", p.pollInterval))
	defer p.log.Debug("Stopped background alarm fetcher")

	// Start the processor
	p.queueProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		ExecuteFn: p.executeAlarm,
	})
	defer func() {
		apErr := p.queueProcessor.Close()
		if apErr != nil {
			p.log.Error("Failed to close alarm processor", slog.Any("error", apErr))
		}
		p.queueProcessor = nil
	}()

	t := p.clock.NewTicker(p.pollInterval)
	defer t.Stop()

	var err error
	for {
		select {
		case <-t.C():
			err = p.fetchAndEnqueueAlarms(ctx, p.hostIDs())
			if err != nil {
				// Log the error only
				p.log.ErrorContext(ctx, "Failed to fetch alarms", slog.Any("error", err))
			}
		case <-ctx.Done():
			// Stop when the context is canceled
			return ctx.Err()
		}
	}
}

func (p *Processor) fetchAndEnqueueAlarms(ctx context.Context, hostList []string) error {
	// Fetch all upcoming alarms
	res, err := p.actorProvider.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: hostList,
	})
	if err != nil {
		return fmt.Errorf("error fetching alarms: %w", err)
	}

	// Enqueue all alarms
	return p.enqueueAlarms(res)
}

func (p *Processor) enqueueAlarms(leases []*ref.AlarmLease) (err error) {
	// Get the lock
	p.activeAlarmsLock.Lock()

	var (
		i  int
		ok bool
	)
	for _, a := range leases {
		// If the alarm is already ok, skip it
		// We also skip alarms that are being retried, otherwise their attempt counter and delay would be overwritten
		// (retries are not persisted and are managed in-memory only)
		_, ok = p.activeAlarms[a.Key()]
		if ok {
			continue
		}
		_, ok = p.retryingAlarms[a.Key()]
		if ok {
			continue
		}

		// If the alarm is to be executed immediately (within the next 0.1ms), skip enqueuing it and execute it right away
		if a.DueTime().Sub(p.clock.Now()) < 100*time.Microsecond {
			p.activeAlarms[a.Key()] = struct{}{}
			go p.executeActiveAlarm(a)
			continue
		}

		leases[i] = a
		i++
	}

	// Unlock
	p.activeAlarmsLock.Unlock()

	if i > 0 {
		// Enqueue the alarms all at once as this is more efficient (we lock the processor only once)
		err = p.queueProcessor.Enqueue(leases[:i]...)
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
func (p *Processor) executeAlarm(lease *ref.AlarmLease) {
	// Mark the alarm as active
	p.activeAlarmsLock.Lock()
	_, active := p.activeAlarms[lease.Key()]
	if active {
		// Already active, so nothing to do
		p.activeAlarmsLock.Unlock()
		return
	}
	p.activeAlarms[lease.Key()] = struct{}{}
	p.activeAlarmsLock.Unlock()

	// Now we can execute the alarm
	p.executeActiveAlarm(lease)
}

// Executes an active alarm, that is already in the activeAlarms map
func (p *Processor) executeActiveAlarm(lease *ref.AlarmLease) {
	ctx := context.Background()

	key := lease.Key()
	log := p.log.With(
		slog.String("id", lease.AlarmRef().String()),
		slog.Any("due", lease.DueTime()),
	)
	log.Debug("Executing alarm")

	// Get and lock the actor
	ref := lease.ActorRef()
	statusAny, err := p.lockAndInvokeFn(ctx, ref, func(parentCtx context.Context, act *activeactor.Instance) (any, error) {
		// Before we execute an alarm we need to fetch it again using the lease
		// This is because alarms we have in-memory could have been here for a few seconds, and they may not represent the accurate
		// state of the data in the provider. For example, it could have been edited or deleted, or the lease could have been broken.
		// Note we do this after we acquired the lock on the actor, since that operation can take time.
		ctx, cancel := context.WithTimeout(parentCtx, p.providerRequestTimeout)
		defer cancel()
		a, err := p.actorProvider.GetLeasedAlarm(ctx, lease)
		if errors.Is(err, components.ErrNoAlarm) {
			// If we get ErrNoAlarm, the alarm was modified/deleted, or the lease was canceled for other reasons
			// We return executeAlarmAbandoned, indicating there's nothing else left to do
			return executeAlarmStatusAbandoned, nil
		} else if err != nil {
			// This is a retryable error, possibly something transient with the component, and it could be retried later
			return executeAlarmStatusRetryable, fmt.Errorf("error retrieving alarm from provider: %w", err)
		}

		// Ensure the actor implements the Alarm method
		obj, ok := act.Instance().(actor.ActorAlarm)
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
		lease.SetExecutionTime(p.clock.Now())

		// Invoke the actor
		err = obj.Alarm(parentCtx, a.Name, data)
		if err != nil {
			// Consider this as a retryable condition unless we've exceeded the max attempts
			code := executeAlarmStatusRetryable
			maxAttempts := p.actorsConfig[ref.ActorType].MaxAttempts
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
		p.activeAlarmsLock.Lock()
		delete(p.activeAlarms, key)

		// If it's retrying, we add it to the list of retrying alarms
		if isRetrying {
			p.retryingAlarms[key] = struct{}{}
		} else {
			delete(p.retryingAlarms, key)
		}

		p.activeAlarmsLock.Unlock()
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
		err = p.actorProvider.DeleteLeasedAlarm(ctx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			// Log the error only - we are in background goroutine
			// Note we ignore ErrNoAlarm since that means the lease was lost or the alarm was deleted in the meanwhile
			log.Error("Error deleting leased alarm after fatal error", slog.Any("error", err))
		}
		return

	case executeAlarmStatusRetryable:
		// We can retry this
		p.log.Warn("Error executing alarm - will retry", slog.Any("error", err))
		// We still hold the lease, so just increment the due time and add re-add it to the queue
		// We increment it by taking the initial retry delay and multiplying it by 1.5^attempts, with a max of 10, and some jitter
		// Disable the "G404: Use of weak random number generator " gosec warning, since this is not used for anything security-related
		// #nosec G404
		jitter := rand.Float64()*0.2 + 0.9
		multiplier := min(math.Pow(1.5, float64(lease.Attempts())), 10) * jitter
		delay := p.actorsConfig[ref.ActorType].InitialRetryDelay * time.Duration(multiplier)
		lease.IncreaseAttempts(p.clock.Now().Add(delay))
		err = p.queueProcessor.Enqueue(lease)
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
		err = p.completeAlarm(ctx, lease, log)
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

func (p *Processor) completeAlarm(parentCtx context.Context, lease *ref.AlarmLease, log *slog.Logger) error {
	// First, retrieve the alarm again
	// We do this again first to check that the alarm is repeating, and second to make sure our lease is still valid
	// In fact, the actor itself may have modified the alarm after executing it
	ctx, cancel := context.WithTimeout(parentCtx, p.providerRequestTimeout)
	defer cancel()
	alarm, err := p.actorProvider.GetLeasedAlarm(ctx, lease)
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
		ctx, cancel = context.WithTimeout(parentCtx, p.providerRequestTimeout)
		defer cancel()
		err = p.actorProvider.DeleteLeasedAlarm(ctx, lease)
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
	if next.Sub(p.clock.Now()) <= p.pollInterval {
		updateReq.RefreshLease = true
	}

	log.Debug("Re-scheduling alarm for next iteration", slog.Any("due", next), slog.Bool("leased", updateReq.RefreshLease))

	// Do the update
	ctx, cancel = context.WithTimeout(parentCtx, p.providerRequestTimeout)
	defer cancel()
	err = p.actorProvider.UpdateLeasedAlarm(ctx, lease, updateReq)
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
		err = p.enqueueAlarms([]*ref.AlarmLease{lease})
		if err != nil {
			return fmt.Errorf("error re-enqueueing leased alarm: %w", err)
		}
	}

	// All done here too
	return nil
}

func (p *Processor) RunLeaseRenewal(parentCtx context.Context) (err error) {
	var res components.RenewAlarmLeasesRes

	// Renew the alarm leases on a loop
	interval := p.actorProvider.RenewLeaseInterval()
	p.log.DebugContext(parentCtx, "Starting background alarm lease renewal", slog.Any("interval", interval))
	defer p.log.Debug("Stopped background lease renewal")

	t := p.clock.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			ctx, cancel := context.WithTimeout(parentCtx, p.providerRequestTimeout)
			defer cancel()
			res, err = p.actorProvider.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{
				Hosts: p.hostIDs(),
			})
			if err != nil {
				// Log the error only
				p.log.ErrorContext(parentCtx, "Error while renewing leases for alarms", slog.Any("error", err))
			} else if len(res.Leases) > 0 {
				p.log.DebugContext(parentCtx, "Renewed alarm leases", slog.Int("count", len(res.Leases)))

				// Re-enqueue all leases
				// The method is safe for concurrent access
				err = p.enqueueAlarms(res.Leases)
				if err != nil {
					p.log.ErrorContext(parentCtx, "Error while re-enqueueing alarms", slog.Any("error", err))
				}
			}
		case <-parentCtx.Done():
			// Stop when the context is canceled
			return parentCtx.Err()
		}
	}
}
