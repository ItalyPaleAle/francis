package host

import (
	"context"
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
	SetAlarm(ctx context.Context, alarmName string) error
	// DeleteAlarm deletes an alarm.
	DeleteAlarm(ctx context.Context, alarmName string) error
}

type actorClient[T any] struct {
	actorID   string
	actorType string
	hasState  bool
	state     T
}

// NewActorClient returns a new ActorClient object.
func NewActorClient[T any](actorType string, actorID string) Client[T] {
	return &actorClient[T]{
		actorType: actorType,
		actorID:   actorID,
	}
}

// SaveState saves the actor's state.
func (c *actorClient[T]) SaveState(ctx context.Context, state T) error {
	/*err := c.daprClient.SaveActorState(ctx, c.actorType, c.actorID, state)
	if err != nil {
		return err
	}*/

	// Store the state in the local object
	c.hasState = true
	c.state = state
	return nil
}

// GetState retrieves the actor's state.
func (c *actorClient[T]) GetState(ctx context.Context) (state T, err error) {
	if c.hasState {
		// Return from the local object
		return c.state, nil
	}

	/*err = c.daprClient.GetActorState(ctx, c.actorType, c.actorID, &state)
	// Ignore the error indicating the state can't be found, and return a zero state
	if err != nil && !errors.Is(err, client.ErrStateNotFound) {
		return state, err
	}*/

	// Store the state in the local object
	c.hasState = true
	c.state = state

	return state, nil
}

// DeleteState deletes the actor's state.
func (c *actorClient[T]) DeleteState(ctx context.Context) error {
	// We set "hasState" to indicate we have the cached state
	var zero T
	c.hasState = true
	c.state = zero

	//return c.daprClient.DeleteActorState(ctx, c.actorType, c.actorID)
	return nil
}

// SetAlarm creates or replaces an alarm.
func (c *actorClient[T]) SetAlarm(ctx context.Context, alarmName string) error {
	//return c.daprClient.SetActorAlarm(ctx, c.actorType, c.actorID, alarmName, opts)
	return nil
}

// DeleteAlarm deletes an alarm.
func (c *actorClient[T]) DeleteAlarm(ctx context.Context, alarmName string) error {
	//return c.daprClient.DeleteActorAlarm(ctx, c.actorType, c.actorID, alarmName)
	return nil
}
