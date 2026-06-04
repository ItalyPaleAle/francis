package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

func (h *Host) GetAlarm(ctx context.Context, actorType string, actorID string, name string) (actor.AlarmProperties, error) {
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
	// Delete the alarm through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err := h.runtimeClient.DeleteAlarm(reqCtx, protocol.DeleteAlarmRequest{
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
	aRef := ref.NewActorRef(req.ActorType, req.ActorID)

	// Track when the alarm executed so the runtime can record it on the lease
	var executionTime int64

	// Acquire the actor's turn-based lock and run its Alarm method
	_, err := h.core.LockAndInvoke(ctx, aRef, func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		// The actor must implement the Alarm method to receive alarms
		obj, ok := act.Instance.(actor.ActorAlarm)
		if !ok {
			return nil, actorcore.ErrActorMethodUnsupported
		}

		// Wrap the alarm data so the actor can decode it into a custom object
		var data actor.Envelope
		if len(req.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(req.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		// Record the execution time, then invoke the alarm
		executionTime = h.clock.Now().UnixMilli()
		alarmErr := obj.Alarm(invokeCtx, req.Name, data)
		if alarmErr != nil {
			return nil, fmt.Errorf("error from actor: %w", alarmErr)
		}

		return nil, nil
	})
	if err != nil {
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
