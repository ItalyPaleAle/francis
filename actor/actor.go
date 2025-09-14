package actor

import (
	"context"
)

// Actor is an alias for "any".
// It's included for convenience.
type Actor = any

// ActorInvoke can be implemented by actors that offer the Invoke method.
type ActorInvoke interface {
	// Invoke is called when an actor is invoked.
	Invoke(ctx context.Context, method string, data Envelope) (any, error)
}

// ActorAlarm can be implemented by actors that offer the Alarm method.
type ActorAlarm interface {
	// Alarm is invoked upon execution of an alarm.
	// The parameter "data" is an envelope that allows decoding the associated data into a custom object; it could be nil if there's no data
	Alarm(ctx context.Context, name string, data Envelope) error
}

// ActorDeactivate can be implemented by actors that offer the Deactivate method.
type ActorDeactivate interface {
	// Deactivate is invoked upon actor deactivation
	Deactivate(ctx context.Context) error
}

// Factory is a function that initializes a new actor.
type Factory func(actorID string, service *Service) Actor

// Envelope allows retrieving data and decoding it into a custom object
type Envelope interface {
	Decode(into any) error
}
