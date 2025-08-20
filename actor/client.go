package actor

import (
	"context"
	"errors"
)

// Client allows interacting with the actor service, and it's pre-configured for the active actor
type Client[T any] interface {
	// SaveState saves the actor's state.
	SaveState(ctx context.Context, state T) error
	// GetState retrieves the actor's state.
	GetState(ctx context.Context) (state T, err error)
	// DeleteState deletes the actor's state.
	DeleteState(ctx context.Context) error
	// SetAlarm creates or replaces an alarm.
	SetAlarm(ctx context.Context, alarmName string, opts AlarmOptions) error
	// DeleteAlarm deletes an alarm.
	DeleteAlarm(ctx context.Context, alarmName string) error
}

type client[T any] struct {
	actorID   string
	actorType string
	service   *Service
	hasState  bool
	state     T
}

// NewActorClient returns a new ActorClient object.
func NewActorClient[T any](actorType string, actorID string, service *Service) Client[T] {
	return &client[T]{
		actorType: actorType,
		actorID:   actorID,
		service:   service,
	}
}

// SaveState saves the actor's state.
func (c *client[T]) SaveState(ctx context.Context, state T) error {
	err := c.service.saveActorState(ctx, c.actorType, c.actorID, state)
	if err != nil {
		return err
	}

	// Store the state in the local object
	c.hasState = true
	c.state = state
	return nil
}

// GetState retrieves the actor's state.
func (c *client[T]) GetState(ctx context.Context) (state T, err error) {
	if c.hasState {
		// Return from the local object
		return c.state, nil
	}

	err = c.service.getActorState(ctx, c.actorType, c.actorID, &state)
	// Ignore the error indicating the state can't be found, and return a zero state
	if err != nil && !errors.Is(err, ErrStateNotFound) {
		return state, err
	}

	// Store the state in the local object
	c.hasState = true
	c.state = state

	return state, nil
}

// DeleteState deletes the actor's state.
func (c *client[T]) DeleteState(ctx context.Context) error {
	// We set "hasState" to indicate we have the cached state
	var zero T
	c.hasState = true
	c.state = zero

	return c.service.deleteActorState(ctx, c.actorType, c.actorID)
}

// SetAlarm creates or replaces an alarm.
func (c *client[T]) SetAlarm(ctx context.Context, alarmName string, opts AlarmOptions) error {
	return c.service.setActorAlarm(ctx, c.actorType, c.actorID, alarmName, opts)
}

// DeleteAlarm deletes an alarm.
func (c *client[T]) DeleteAlarm(ctx context.Context, alarmName string) error {
	return c.service.deleteActorAlarm(ctx, c.actorType, c.actorID, alarmName)
}
