package actor

import (
	"context"
	"encoding/json"
	"errors"
)

// ErrStateNotFound is returned by getActorState when the object cannot be found.
var ErrStateNotFound = errors.New("not found")

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

func (s Service) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (any, error) {
	return s.host.Invoke(ctx, actorType, actorID, method, data)
}

func (s Service) HaltAll() error {
	return s.host.HaltAll()
}

func (s Service) Halt(actorType string, actorID string) error {
	return s.host.Halt(actorType, actorID)
}

// setState saves the state for an actor.
func (s Service) setState(ctx context.Context, actorType string, actorID string, state any) error {
	return s.host.SetState(ctx, actorType, actorID, state)
}

// getState retrieves the state for an actor.
// The state is JSON-decoded into dest.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) getState(ctx context.Context, actorType string, actorID string, dest any) error {
	return s.host.GetState(ctx, actorType, actorID, dest)
}

// deleteState deletes the state for an actor.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) deleteState(ctx context.Context, actorType string, actorID string) error {
	return s.host.DeleteState(ctx, actorType, actorID)
}

// setAlarm creates or replaces an alarm for an actor.
func (s Service) setAlarm(ctx context.Context, actorType string, actorID string, alarmName string, opts AlarmOptions) error {
	// TODO
	return nil
}

// AlarmOptions contains the options for a new alarm.
type AlarmOptions struct {
	Data    json.RawMessage `json:"data"`
	DueTime string          `json:"dueTime"`
	Period  string          `json:"period"`
	TTL     string          `json:"ttl"`
}

// deleteAlarm deletes an alarm for an actor.
func (s Service) deleteAlarm(ctx context.Context, actorType string, actorID string, alarmName string) error {
	// TODO
	return nil
}

type Host interface {
	Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (any, error)

	HaltAll() error
	Halt(actorType string, actorID string) error

	SetState(ctx context.Context, actorType string, actorID string, state any) error
	GetState(ctx context.Context, actorType string, actorID string, dest any) error
	DeleteState(ctx context.Context, actorType string, actorID string) error
}
