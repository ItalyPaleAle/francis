package actor

import (
	"context"
)

// Actor is the interface that all actor objects must implement.
// It allows the actor host to invoke and execute alarms on each actor.
type Actor interface {
	// Invoke is called when an actor is invoked.
	Invoke(ctx context.Context, method string, data any) (any, error)
	// Alarm is invoked upon execution of an alarm.
	Alarm(ctx context.Context, name string, data any) error
	// Deactivate is invoked upon actor deactivation
	Deactivate(ctx context.Context) error
}

// Factory is a function that initializes a new actor.
type Factory func(actorID string, service *Service) Actor
