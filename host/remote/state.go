package remote

import (
	"context"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
	"github.com/italypaleale/francis/internal/types"
	"github.com/italypaleale/francis/protocol"
)

func (h *Host) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) (err error) {
	ctx, span := tracing.Start(ctx, "state.set",
		trace.WithAttributes(
			tracing.ActorType(actorType),
			tracing.ActorID(actorID),
		),
	)
	defer func() {
		tracing.End(span, err)
	}()

	err = ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	var ttl time.Duration
	if opts != nil {
		ttl = opts.TTL
	}

	// Encode the state using msgpack
	data, err := msgpack.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to serialize state using msgpack: %w", err)
	}

	// Persist the state through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.SetState(reqCtx, protocol.SetStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
		Data:      data,
		TTLMs:     ttl.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("failed saving state: %w", err)
	}

	return nil
}

func (h *Host) GetState(ctx context.Context, actorType string, actorID string, dest any) (err error) {
	ctx, span := tracing.Start(ctx, "state.get",
		trace.WithAttributes(
			tracing.ActorType(actorType),
			tracing.ActorID(actorID),
		),
	)

	defer func() {
		// A missing state is an expected outcome (an actor reading state it has not written yet)
		tracing.EndExpected(span, err, actor.ErrStateNotFound)
	}()

	err = ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	// Retrieve the state through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.GetState(reqCtx, protocol.GetStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
	})
	if isProtocolErrorCode(err, protocol.ErrCodeStateNotFound) {
		// A missing state is reported as the public ErrStateNotFound
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed retrieving state: %w", err)
	}

	// Decode the state into the caller's destination
	err = msgpack.Unmarshal(res.Data, dest)
	if err != nil {
		return fmt.Errorf("failed to deserialize state using msgpack: %w", err)
	}

	return nil
}

func (h *Host) ListStates(ctx context.Context, actorType string, opts *actor.ListStatesOpts) (res actor.StateList, err error) {
	ctx, span := tracing.Start(ctx, "state.list",
		trace.WithAttributes(
			tracing.ActorType(actorType),
		),
	)
	defer func() {
		tracing.End(span, err)
	}()

	err = ref.ValidateComponents(actorType)
	if err != nil {
		return res, err
	}

	req := protocol.ListStatesRequest{
		ActorType: actorType,
	}
	if opts != nil {
		req.IncludeData = opts.IncludeData
		req.After = opts.After
		req.Limit = opts.Limit
	}

	// Retrieve the page through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	protoRes, err := h.runtimeClient.ListStates(reqCtx, req)
	if err != nil {
		return res, fmt.Errorf("failed listing states: %w", err)
	}

	// Map the wire DTOs to the public types
	res = actor.StateList{
		States:  make([]actor.StateInfo, len(protoRes.States)),
		HasMore: protoRes.HasMore,
	}
	for i := range protoRes.States {
		res.States[i] = actor.StateInfo{
			ActorID: protoRes.States[i].ActorID,
		}

		// An empty payload is left as a nil envelope, since there is nothing to decode from it
		if len(protoRes.States[i].Data) > 0 {
			res.States[i].Data = types.MsgpackEnvelope(protoRes.States[i].Data)
		}
	}

	return res, nil
}

func (h *Host) DeleteState(ctx context.Context, actorType string, actorID string) (err error) {
	ctx, span := tracing.Start(ctx, "state.delete",
		trace.WithAttributes(
			tracing.ActorType(actorType),
			tracing.ActorID(actorID),
		),
	)

	defer func() {
		// Deleting a state that does not exist already reaches the desired end state
		tracing.EndExpected(span, err, actor.ErrStateNotFound)
	}()

	err = ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	// Delete the state through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.DeleteState(reqCtx, protocol.DeleteStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
	})
	if isProtocolErrorCode(err, protocol.ErrCodeStateNotFound) {
		// A missing state is reported as the public ErrStateNotFound
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed deleting state: %w", err)
	}

	return nil
}
