package host

import (
	"context"
	"errors"
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

func (h *Host) SetState(ctx context.Context, actorType string, actorID string, state any) error {
	// Encode the state using msgpack
	data, err := msgpack.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed serializing state using msgpack: %w", err)
	}

	err = h.actorProvider.SetState(ctx, actorRef(actorType, actorID), data, components.SetStateOpts{
		// TODO: support TTL
		TTL: 0,
	})
	if err != nil {
		return fmt.Errorf("failed saving state: %w", err)
	}

	return nil
}

func (h *Host) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	data, err := h.actorProvider.GetState(ctx, actorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed retrieving state: %w", err)
	}

	err = msgpack.Unmarshal(data, dest)
	if err != nil {
		return fmt.Errorf("failed unserializing state using msgpack: %w", err)
	}

	return nil
}

func (h *Host) DeleteState(ctx context.Context, actorType string, actorID string) error {
	err := h.actorProvider.DeleteState(ctx, actorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed deleting state: %w", err)
	}

	return nil
}
