package actor

import (
	"context"
	"errors"
)

// Client allows interacting with the actor service, and it's pre-configured for the active actor
type Client[T any] interface {
	// SetState saves the actor's state.
	SetState(ctx context.Context, state T, opts *SetStateOpts) error
	// GetState retrieves the actor's state.
	GetState(ctx context.Context) (state T, err error)
	// DeleteState deletes the actor's state.
	DeleteState(ctx context.Context) error
	// SetAlarm creates or replaces an alarm.
	SetAlarm(ctx context.Context, alarmName string, properties AlarmProperties) error
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

// SetState saves the actor's state.
func (c *client[T]) SetState(ctx context.Context, state T, opts *SetStateOpts) error {
	err := c.service.SetState(ctx, c.actorType, c.actorID, state, opts)
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

	err = c.service.GetState(ctx, c.actorType, c.actorID, &state)
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

	return c.service.DeleteState(ctx, c.actorType, c.actorID)
}

// SetAlarm creates or replaces an alarm.
func (c *client[T]) SetAlarm(ctx context.Context, alarmName string, properties AlarmProperties) error {
	return c.service.SetAlarm(ctx, c.actorType, c.actorID, alarmName, properties)
}

// DeleteAlarm deletes an alarm.
func (c *client[T]) DeleteAlarm(ctx context.Context, alarmName string) error {
	return c.service.DeleteAlarm(ctx, c.actorType, c.actorID, alarmName)
}
