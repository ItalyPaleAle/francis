package remote

import (
	"context"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

func (h *Host) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	err := ref.ValidateComponents(actorType, actorID)
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
		ActorRef: protocol.ActorRef{ActorType: actorType, ActorID: actorID},
		Data:     data,
		TTLMs:    ttl.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("failed saving state: %w", err)
	}

	return nil
}

func (h *Host) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	// Retrieve the state through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.GetState(reqCtx, protocol.GetStateRequest{
		ActorRef: protocol.ActorRef{ActorType: actorType, ActorID: actorID},
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

func (h *Host) DeleteState(ctx context.Context, actorType string, actorID string) error {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	// Delete the state through the runtime
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.DeleteState(reqCtx, protocol.DeleteStateRequest{
		ActorRef: protocol.ActorRef{ActorType: actorType, ActorID: actorID},
	})
	if isProtocolErrorCode(err, protocol.ErrCodeStateNotFound) {
		// A missing state is reported as the public ErrStateNotFound
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed deleting state: %w", err)
	}

	return nil
}
