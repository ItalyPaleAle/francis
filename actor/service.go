package actor

import (
	"context"
	"errors"
)

var (
	// ErrStateNotFound is returned by GetState and DeleteState when the object cannot be found.
	ErrStateNotFound = errors.New("state not found for actor")
	// ErrAlarmNotFound is returned by DeleteAlarm when the alarm cannot be found.
	ErrAlarmNotFound = errors.New("alarm not found")
	// ErrActorNotHosted is returned by Halt when the actor is not active on the current host.
	ErrActorNotHosted = errors.New("actor is not active on the current host")
	// ErrActorHalted is returned by methods that perform invocation when the actor is halted on the host where it was previously active.
	// Callers should retry after a delay.
	ErrActorHalted = errors.New("actor is halted")
	// ErrActorHalted is returned by methods that perform invocation when the actor type is not supported for this cluster.
	ErrActorTypeUnsupported = errors.New("actor type is not supported in the cluster")
)

// Service allows interacting with the actor host, to invoke actors and perform operations on state and alarms.
type Service struct {
	host Host
}

// NewService returns a new Service configured to interact with specific Host.
func NewService(host Host) *Service {
	return &Service{
		host: host,
	}
}

// Invoke an actor
func (s Service) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (Envelope, error) {
	return s.host.Invoke(ctx, actorType, actorID, method, data)
}

// SetState saves the state for an actor.
func (s Service) SetState(ctx context.Context, actorType string, actorID string, state any) error {
	return s.host.SetState(ctx, actorType, actorID, state)
}

// GetState retrieves the state for an actor.
// The state is JSON-decoded into dest.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	return s.host.GetState(ctx, actorType, actorID, dest)
}

// DeleteState deletes the state for an actor.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) DeleteState(ctx context.Context, actorType string, actorID string) error {
	return s.host.DeleteState(ctx, actorType, actorID)
}

// SetAlarm creates or replaces an alarm for an actor.
func (s Service) SetAlarm(ctx context.Context, actorType string, actorID string, alarmName string, properties AlarmProperties) error {
	return s.host.SetAlarm(ctx, actorType, actorID, alarmName, properties)
}

// DeleteAlarm deletes an alarm for an actor.
// Returns ErrAlarmNotFound if the alarm cannot be found.
func (s Service) DeleteAlarm(ctx context.Context, actorType string, actorID string, alarmName string) error {
	return s.host.DeleteAlarm(ctx, actorType, actorID, alarmName)
}

// HaltAll halts all actors currently active on the host.
func (s Service) HaltAll() error {
	return s.host.HaltAll()
}

// Halt halts one actor currently active on the host.
func (s Service) Halt(actorType string, actorID string) error {
	return s.host.Halt(actorType, actorID)
}
