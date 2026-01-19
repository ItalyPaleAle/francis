package francis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/italypaleale/go-kit/eventqueue"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// AlarmRunner handles fetching and dispatching alarms to clients
type AlarmRunner struct {
	provider       components.ActorProvider
	clientManager  *ClientManager
	pollInterval   time.Duration
	requestTimeout time.Duration
	clock          clock.WithTicker
	log            *slog.Logger

	processor    *eventqueue.Processor[string, *ref.AlarmLease]
	activeAlarms sync.Map // map[string]struct{} - tracks active alarm keys
}

// NewAlarmRunner creates a new AlarmRunner
func NewAlarmRunner(
	provider components.ActorProvider,
	clientManager *ClientManager,
	pollInterval time.Duration,
	requestTimeout time.Duration,
	clk clock.WithTicker,
	log *slog.Logger,
) *AlarmRunner {
	return &AlarmRunner{
		provider:       provider,
		clientManager:  clientManager,
		pollInterval:   pollInterval,
		requestTimeout: requestTimeout,
		clock:          clk,
		log:            log.With(slog.String("component", "alarm-runner")),
	}
}

// Run starts the alarm runner
func (r *AlarmRunner) Run(ctx context.Context) error {
	r.log.DebugContext(ctx, "Starting alarm runner", slog.Any("interval", r.pollInterval))
	defer r.log.Debug("Stopped alarm runner")

	// Start the event processor
	r.processor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		Clock:     r.clock,
		ExecuteFn: r.dispatchAlarm,
	})
	defer func() {
		err := r.processor.Close()
		if err != nil {
			r.log.Error("Error closing alarm processor", slog.Any("error", err))
		}
	}()

	t := r.clock.NewTicker(r.pollInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			err := r.fetchAndEnqueueAlarms(ctx)
			if err != nil {
				r.log.ErrorContext(ctx, "Error fetching alarms", slog.Any("error", err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *AlarmRunner) fetchAndEnqueueAlarms(ctx context.Context) error {
	// Get all connected clients
	clients := r.clientManager.GetAllClients()
	if len(clients) == 0 {
		return nil
	}

	// Collect all host IDs
	hostIDs := make([]string, len(clients))
	for i, client := range clients {
		hostIDs[i] = client.ID
	}

	// Fetch alarms for all hosts
	fetchCtx, fetchCancel := context.WithTimeout(ctx, r.requestTimeout)
	defer fetchCancel()

	leases, err := r.provider.FetchAndLeaseUpcomingAlarms(fetchCtx, components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: hostIDs,
	})
	if err != nil {
		return fmt.Errorf("error fetching alarms: %w", err)
	}

	// Enqueue alarms
	for _, lease := range leases {
		key := lease.Key()

		// Skip if already active
		if _, loaded := r.activeAlarms.LoadOrStore(key, struct{}{}); loaded {
			continue
		}

		// If due immediately, dispatch now
		if lease.DueTime().Sub(r.clock.Now()) < 100*time.Microsecond {
			go r.dispatchAlarm(lease)
			continue
		}

		// Enqueue for later
		err = r.processor.Enqueue(lease)
		if err != nil {
			r.activeAlarms.Delete(key)
			r.log.Error("Error enqueueing alarm", slog.String("key", key), slog.Any("error", err))
		}
	}

	return nil
}

func (r *AlarmRunner) dispatchAlarm(lease *ref.AlarmLease) {
	ctx := context.Background()
	key := lease.Key()
	aRef := lease.AlarmRef()

	log := r.log.With(
		slog.String("alarm", aRef.String()),
		slog.Any("due", lease.DueTime()),
	)

	defer func() {
		r.activeAlarms.Delete(key)
	}()

	log.Debug("Dispatching alarm to client")

	// First, verify the lease is still valid and get fresh alarm data
	getCtx, getCancel := context.WithTimeout(ctx, r.requestTimeout)
	alarm, err := r.provider.GetLeasedAlarm(getCtx, lease)
	getCancel()
	if errors.Is(err, components.ErrNoAlarm) {
		log.Debug("Alarm no longer exists or lease expired, skipping")
		return
	}
	if err != nil {
		log.Error("Error getting leased alarm", slog.Any("error", err))
		return
	}

	// Look up which client should execute this alarm
	lookupCtx, lookupCancel := context.WithTimeout(ctx, r.requestTimeout)
	location, err := r.provider.LookupActor(lookupCtx, aRef.ActorRef(), components.LookupActorOpts{
		// Get clients that support this actor type
	})
	lookupCancel()
	if err != nil {
		log.Error("Error looking up actor for alarm", slog.Any("error", err))
		// Release the lease since we can't execute the alarm
		r.releaseAlarmLease(ctx, lease)
		return
	}

	// Get the client
	client, ok := r.clientManager.GetClient(location.HostID)
	if !ok {
		log.Warn("Client not connected for alarm execution", slog.String("hostId", location.HostID))
		// Release the lease
		r.releaseAlarmLease(ctx, lease)
		return
	}

	// Send notification to client
	notification := &protocol.ExecuteAlarmNotification{
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		AlarmName: aRef.Name,
		LeaseID:   uuid.NewString(), // Generate a unique lease ID for tracking
		DueTime:   lease.DueTime(),
		Data:      alarm.Data,
		Attempt:   lease.Attempts(),
	}

	notifyPayload, err := protocol.EncodePayload(notification)
	if err != nil {
		log.Error("Error encoding alarm notification", slog.Any("error", err))
		r.releaseAlarmLease(ctx, lease)
		return
	}

	msg := &protocol.Message{
		Type:    protocol.MsgTypeExecuteAlarm,
		ID:      notification.LeaseID,
		Payload: notifyPayload,
	}

	err = client.SendNotification(ctx, msg)
	if err != nil {
		log.Error("Error sending alarm notification to client", slog.Any("error", err))
		// Release the lease so another host can pick it up
		r.releaseAlarmLease(ctx, lease)
		return
	}

	log.Debug("Alarm dispatched to client", slog.String("clientId", client.ID))
}

func (r *AlarmRunner) releaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) {
	releaseCtx, releaseCancel := context.WithTimeout(ctx, r.requestTimeout)
	defer releaseCancel()

	err := r.provider.ReleaseAlarmLease(releaseCtx, lease)
	if err != nil && !errors.Is(err, components.ErrNoAlarm) {
		r.log.Error("Error releasing alarm lease", slog.Any("error", err))
	}
}

// HandleAlarmCompleted processes an alarm completion message from a client
func (r *AlarmRunner) HandleAlarmCompleted(ctx context.Context, req *protocol.AlarmCompletedReq) (*protocol.AlarmCompletedRes, error) {
	aRef := ref.NewAlarmRef(req.ActorType, req.ActorID, req.AlarmName)
	log := r.log.With(slog.String("alarm", aRef.String()))

	// Create a lease object for operations
	// Note: In a real implementation, we'd need to track the actual lease
	// For now, we create a placeholder
	lease := ref.NewAlarmLease(aRef, req.ExecutionTime, req.Attempt)

	switch req.Status {
	case protocol.AlarmStatusCompleted:
		return r.completeAlarm(ctx, lease, log)

	case protocol.AlarmStatusFailed:
		// Fatal error - delete the alarm
		log.Warn("Alarm execution failed fatally", slog.String("error", req.ErrorMessage))
		delCtx, delCancel := context.WithTimeout(ctx, r.requestTimeout)
		defer delCancel()
		err := r.provider.DeleteLeasedAlarm(delCtx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			log.Error("Error deleting failed alarm", slog.Any("error", err))
		}
		return &protocol.AlarmCompletedRes{}, nil

	case protocol.AlarmStatusRetryable:
		// Retryable error - schedule retry
		log.Warn("Alarm execution failed, will retry", slog.String("error", req.ErrorMessage))
		// The client will handle retry timing
		return &protocol.AlarmCompletedRes{}, nil

	case protocol.AlarmStatusAbandoned:
		// Alarm was abandoned (lease lost, etc.)
		log.Debug("Alarm was abandoned")
		return &protocol.AlarmCompletedRes{}, nil

	default:
		return nil, fmt.Errorf("unknown alarm status: %s", req.Status)
	}
}

func (r *AlarmRunner) completeAlarm(ctx context.Context, lease *ref.AlarmLease, log *slog.Logger) (*protocol.AlarmCompletedRes, error) {
	// Get the alarm to check if it repeats
	getCtx, getCancel := context.WithTimeout(ctx, r.requestTimeout)
	alarm, err := r.provider.GetLeasedAlarm(getCtx, lease)
	getCancel()
	if errors.Is(err, components.ErrNoAlarm) {
		// Alarm was deleted or lease expired
		return &protocol.AlarmCompletedRes{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error getting alarm: %w", err)
	}

	// Check if the alarm repeats
	next := alarm.NextExecution(lease.ExecutionTime())
	if next.IsZero() {
		// Non-repeating alarm, delete it
		log.Debug("Deleting completed alarm")
		delCtx, delCancel := context.WithTimeout(ctx, r.requestTimeout)
		defer delCancel()
		err = r.provider.DeleteLeasedAlarm(delCtx, lease)
		if err != nil && !errors.Is(err, components.ErrNoAlarm) {
			return nil, fmt.Errorf("error deleting completed alarm: %w", err)
		}
		return &protocol.AlarmCompletedRes{}, nil
	}

	// Repeating alarm, update the due time
	log.Debug("Re-scheduling repeating alarm", slog.Any("nextDue", next))
	updateCtx, updateCancel := context.WithTimeout(ctx, r.requestTimeout)
	defer updateCancel()

	keepLease := next.Sub(r.clock.Now()) <= r.pollInterval
	err = r.provider.UpdateLeasedAlarm(updateCtx, lease, components.UpdateLeasedAlarmReq{
		DueTime:      next,
		RefreshLease: keepLease,
	})
	if errors.Is(err, components.ErrNoAlarm) {
		return &protocol.AlarmCompletedRes{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error updating repeating alarm: %w", err)
	}

	return &protocol.AlarmCompletedRes{
		NextDueTime: &next,
		KeepLease:   keepLease,
	}, nil
}
