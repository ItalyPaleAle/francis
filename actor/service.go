package actor

import (
	"context"
	"encoding/json"
	"errors"
)

// ErrStateNotFound is returned by getActorState when the object cannot be found
var ErrStateNotFound = errors.New("not found")

// AlarmOptions contains the options for a new alarm.
type AlarmOptions struct {
	Data    json.RawMessage `json:"data"`
	DueTime string          `json:"dueTime"`
	Period  string          `json:"period"`
	TTL     string          `json:"ttl"`
}

// Service allows interacting with the actor host, to invoke
type Service struct {
}

func (s Service) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) ([]byte, error) {
	// TODO
	return nil, nil
}

// saveActorState saves the state for an actor.
func (s Service) saveActorState(ctx context.Context, actorType string, actorID string, state any) error {
	// TODO
	return nil
}

// GetActorState retrieves the state for an actor.
// The state is JSON-decoded into dest.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) getActorState(ctx context.Context, actorType string, actorID string, dest any) error {
	// TODO
	return nil
}

// deleteActorState deletes the state for an actor.
// Returns ErrStateNotFound if the state cannot be found.
func (s Service) deleteActorState(ctx context.Context, actorType string, actorID string) error {
	// TODO
	return nil
}

// setActorAlarm creates or replaces an alarm for an actor.
func (s Service) setActorAlarm(ctx context.Context, actorType string, actorID string, alarmName string, opts AlarmOptions) error {
	// TODO
	return nil
}

// deleteActorAlarm deletes an alarm for an actor.
func (s Service) deleteActorAlarm(ctx context.Context, actorType string, actorID string, alarmName string) error {
	// TODO
	return nil
}
