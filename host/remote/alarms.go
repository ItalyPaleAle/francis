package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	msgpack "github.com/vmihailenco/msgpack/v5"
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
		AlarmRef: protocol.AlarmRef{ActorType: actorType, ActorID: actorID, Name: name},
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
		AlarmRef:        protocol.AlarmRef{ActorType: actorType, ActorID: actorID, Name: name},
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
		AlarmRef: protocol.AlarmRef{ActorType: actorType, ActorID: actorID, Name: name},
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
		// Wrap the occurrence data so the actor can decode it into a custom object
		var data actor.Envelope
		if len(req.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(req.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		// Stamp a per-occurrence key (ID + due-time ms) into the context so the actor can detect duplicate deliveries of the same occurrence without confusing them with legitimate subsequent firings of a repeating occurrence (which have a different due time)
		invokeCtx = actor.WithRequestID(invokeCtx, alarmRequestID(req))

		// Record the execution time before invoking the actor
		executionTime = h.clock.Now().UnixMilli()

		// Jobs are delivered to the Job method, plain alarms to the Alarm method
		if req.Kind == string(components.AlarmKindJob) {
			obj, ok := act.Instance.(actor.ActorJob)
			if !ok {
				return nil, actorcore.ErrActorMethodUnsupported
			}
			rErr := obj.Job(invokeCtx, req.JobMethod, data)
			if rErr != nil {
				return nil, rErr
			}
			return nil, nil
		}

		// The actor must implement the Alarm method to receive alarms
		obj, ok := act.Instance.(actor.ActorAlarm)
		if !ok {
			return nil, actorcore.ErrActorMethodUnsupported
		}
		rErr := obj.Alarm(invokeCtx, req.Name, data)
		if rErr != nil {
			return nil, fmt.Errorf("error from actor: %w", rErr)
		}

		return nil, nil
	})
	if err != nil {
		tracing.Fail(ctx, err.Error())

		// A permanent job failure is signaled with a distinct code so the runtime dead-letters it immediately instead of retrying
		if errors.Is(err, actor.ErrJobPermanentFailure) {
			return protocol.ExecuteAlarmResponse{}, protocol.NewError(protocol.ErrCodeJobPermanentFailure, err.Error())
		}

		return protocol.ExecuteAlarmResponse{}, actorcore.InvokeErrorToProtocol(err)
	}

	return protocol.ExecuteAlarmResponse{ExecutionTimeUnixMs: executionTime}, nil
}

// terminateActor halts an actor active on this host, at the runtime's request
func (h *Host) terminateActor(_ context.Context, req protocol.TerminateActorRequest) *protocol.Error {
	err := h.core.Halt(req.ActorType, req.ActorID)
	if errors.Is(err, actor.ErrActorNotHosted) {
		// An actor that is not active here is already in the desired state
		return nil
	} else if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to terminate actor: %v", err)
	}

	return nil
}

func alarmRequestID(req protocol.ExecuteAlarmRequest) string {
	return req.AlarmID + "|" + strconv.FormatInt(req.DueTimeUnixMs, 10)
}
