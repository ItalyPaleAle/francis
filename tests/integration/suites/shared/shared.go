//go:build integration

// Package shared holds small helpers reused across integration scenarios
package shared

import (
	"context"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
)

// CounterActorType is the registered type name for the counter actor
const CounterActorType = "counter"

// CounterState is the persisted state of the counter actor
type CounterState struct {
	N int64 `json:"n"`
}

// CounterResult is returned by the counter actor's increment method
type CounterResult struct {
	N int64 `json:"n"`
}

// CounterActor is a trivial actor that persists a monotonically increasing counter
// It exercises state read and write through whichever provider and runtime are in use
type CounterActor struct {
	client actor.Client[CounterState]
}

// NewCounterActor is the actor.Factory for CounterActor
func NewCounterActor(actorID string, service *actor.Service) actor.Actor {
	return &CounterActor{
		client: actor.NewActorClient[CounterState](CounterActorType, actorID, service),
	}
}

// Invoke handles the increment method by loading state, incrementing, persisting, and returning the new value
func (a *CounterActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	state, err := a.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	if method == "increment" {
		state.N++
	}

	err = a.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, err
	}

	return CounterResult(state), nil
}

// CounterReg returns the registration for the counter actor with the given idle timeout
func CounterReg(idle time.Duration) frameworkhost.ActorReg {
	return frameworkhost.ActorReg{
		Type:    CounterActorType,
		Factory: NewCounterActor,
		Opts:    actorcore.RegisterActorOptions{IdleTimeout: idle},
	}
}
