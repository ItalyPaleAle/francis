package local

import (
	"context"
	"errors"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
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

	err = h.actorProvider.SetState(ctx, ref.NewActorRef(actorType, actorID), data, components.SetStateOpts{
		TTL: ttl,
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
		// A missing state is an expected outcome (an actor reading state it has not written yet), not a span error
		tracing.EndExpected(span, err, actor.ErrStateNotFound)
	}()

	err = ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	data, err := h.actorProvider.GetState(ctx, ref.NewActorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed retrieving state: %w", err)
	}

	err = msgpack.Unmarshal(data, dest)
	if err != nil {
		return fmt.Errorf("failed to deserialize state using msgpack: %w", err)
	}

	return nil
}

func (h *Host) DeleteState(ctx context.Context, actorType string, actorID string) (err error) {
	ctx, span := tracing.Start(ctx, "state.delete",
		trace.WithAttributes(
			tracing.ActorType(actorType),
			tracing.ActorID(actorID),
		),
	)

	defer func() {
		// Deleting a state that does not exist already reaches the desired end state, so it is not a span error
		tracing.EndExpected(span, err, actor.ErrStateNotFound)
	}()

	err = ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	err = h.actorProvider.DeleteState(ctx, ref.NewActorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed deleting state: %w", err)
	}

	return nil
}
