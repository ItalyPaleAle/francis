package remote

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
	"github.com/italypaleale/francis/protocol"
)

func (h *Host) GetAlarm(ctx context.Context, actorType string, actorID string, name string) (actor.AlarmProperties, error) {
	err := ref.ValidateComponents(actorType, actorID, name)
	if err != nil {
		return actor.AlarmProperties{}, err
	}

	// Retrieve the alarm through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.GetAlarm(reqCtx, protocol.GetAlarmRequest{
		ActorType: actorType,
		ActorID:   actorID,
		Name:      name,
	})
	if isProtocolErrorCode(err, protocol.ErrCodeAlarmNotFound) {
		// A missing alarm is reported as the public ErrAlarmNotFound
		return actor.AlarmProperties{}, actor.ErrAlarmNotFound
	} else if err != nil {
		return actor.AlarmProperties{}, fmt.Errorf("failed to get alarm: %w", err)
	}

	return protocolAlarmPropsToActor(res.AlarmProperties)
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

	// Encode the alarm properties for the wire
	props, err := actorAlarmPropsToProtocol(properties)
	if err != nil {
		return err
	}

	// Create or replace the alarm through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.SetAlarm(reqCtx, protocol.SetAlarmRequest{
		ActorType:       actorType,
		ActorID:         actorID,
		Name:            name,
		AlarmProperties: props,
	})
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

	// Delete the alarm through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.DeleteAlarm(reqCtx, protocol.DeleteAlarmRequest{
		ActorType: actorType,
		ActorID:   actorID,
		Name:      name,
	})
	if isProtocolErrorCode(err, protocol.ErrCodeAlarmNotFound) {
		// A missing alarm is reported as the public ErrAlarmNotFound
		return actor.ErrAlarmNotFound
	} else if err != nil {
		return fmt.Errorf("failed to delete alarm: %w", err)
	}

	return nil
}

// executeAlarm runs an alarm for an actor owned by this host
// It is invoked by the runtime, which owns the alarm lease and schedule
// This host only activates the actor and runs its Alarm method
func (h *Host) executeAlarm(ctx context.Context, req protocol.ExecuteAlarmRequest) (protocol.ExecuteAlarmResponse, *protocol.Error) {
	// Continue the runtime's trace as a server span for the alarm run on this host
	ctx, span := tracing.Start(ctx, "alarm.execute",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			tracing.ActorType(req.ActorType),
			tracing.ActorID(req.ActorID),
			tracing.AlarmName(req.Name),
		),
	)
	defer span.End()

	aRef := ref.NewActorRef(req.ActorType, req.ActorID)

	// Track when the alarm executed so the runtime can record it on the lease
	var executionTime int64

	// Acquire the actor's turn-based lock and run its Alarm or Job method
	_, err := h.core.LockAndInvoke(ctx, aRef, func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		// Record the execution time before invoking the actor
		executionTime = h.clock.Now().UnixMilli()

		// Jobs are delivered to the Job method, plain alarms to the Alarm method
		// A job whose capacity group is full on this host is declined, which the runtime re-routes to another host without counting an attempt
		return nil, h.core.RunOccurrence(invokeCtx, act, actorcore.Occurrence{
			Job:       req.Kind == string(components.AlarmKindJob),
			Name:      req.Name,
			JobMethod: req.JobMethod,
			Data:      req.Data,
			RequestID: alarmRequestID(req),
		})
	})
	if err != nil {
		tracing.Fail(ctx, err.Error())

		// The host declined this job occurrence, either because its capacity group is full or the handler returned ErrJobRejected
		// It is signaled with a distinct code so the runtime re-routes it to another host without counting an attempt, and the actor is halted here to clear its placement so the re-route does not return to this host
		if errors.Is(err, actorcore.ErrCapacityExhausted) {
			h.core.HaltDeferred(req.ActorType, req.ActorID)
			return protocol.ExecuteAlarmResponse{}, protocol.NewError(protocol.ErrCodeCapacityExhausted, err.Error())
		}
		if errors.Is(err, actor.ErrJobRejected) {
			h.core.HaltDeferred(req.ActorType, req.ActorID)
			return protocol.ExecuteAlarmResponse{}, protocol.NewError(protocol.ErrCodeJobRejected, err.Error())
		}

		// A permanent job failure is signaled with a distinct code so the runtime dead-letters it immediately instead of retrying
		if errors.Is(err, actor.ErrJobPermanentFailure) {
			return protocol.ExecuteAlarmResponse{}, protocol.NewError(protocol.ErrCodeJobPermanentFailure, err.Error())
		}

		return protocol.ExecuteAlarmResponse{}, actorcore.InvokeErrorToProtocol(err)
	}

	return protocol.ExecuteAlarmResponse{ExecutionTimeUnixMs: executionTime}, nil
}

// terminateActor halts an actor active on this host, at the runtime's request
func (h *Host) terminateActor(_ context.Context, req protocol.TerminateActorRequest) (protocol.TerminateActorResponse, *protocol.Error) {
	err := h.core.Halt(req.ActorType, req.ActorID)
	if errors.Is(err, actor.ErrActorNotHosted) {
		// An actor that is not active here is already in the desired state, which the caller may want to know
		return protocol.TerminateActorResponse{NotActive: true}, nil
	} else if err != nil {
		return protocol.TerminateActorResponse{}, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to terminate actor: %v", err)
	}

	return protocol.TerminateActorResponse{}, nil
}

func alarmRequestID(req protocol.ExecuteAlarmRequest) string {
	return req.AlarmID + "|" + strconv.FormatInt(req.DueTimeUnixMs, 10)
}
