package host

import (
	"context"
)

type Actor interface {
	// Invoke is called when an actor is invoked.
	Invoke(ctx context.Context, method string, data []byte) error
	// Alarm is invoked upon execution of an alarm.
	Alarm(ctx context.Context, name string, data []byte) error
	// Deactivate is invoked upon actor deactivation
	Deactivate(ctx context.Context) error
}

// ActorFactory is a function that initializes a new actor
type ActorFactory func(actorType string, actorID string) Actor
