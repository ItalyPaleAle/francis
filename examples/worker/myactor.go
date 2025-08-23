package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/italypaleale/actors/actor"
)

type MyActor struct {
	invocations uint64
	log         *slog.Logger
	client      actor.Client[myActorState]
}

type myActorState struct {
	Counter int64
}

func NewMyActor(actorID string, service *actor.Service) actor.Actor {
	log := slog.
		Default().
		With(
			slog.String("actorType", "myactor"),
			slog.String("actorID", actorID),
		)

	log.Info("Actor Created")

	return &MyActor{
		log:    log,
		client: actor.NewActorClient[myActorState]("myactor", actorID, service),
	}
}

func (m *MyActor) Invoke(ctx context.Context, method string, data any) (any, error) {
	m.invocations++

	state, err := m.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("error retrieving state: %w", err)
	}

	switch method {
	case "increment":
		state.Counter++
	case "reset":
		state.Counter = 0
	}

	err = m.client.SetState(ctx, state)
	if err != nil {
		return nil, fmt.Errorf("error saving state: %w", err)
	}

	m.log.InfoContext(ctx, "Actor Invoked", "method", method, "counter", state.Counter, "invocations", m.invocations)

	return m.invocations, nil
}

func (m *MyActor) Alarm(ctx context.Context, name string, data any) error {
	m.log.InfoContext(ctx, "Actor Alarm", "name", name)
	return nil
}

func (m *MyActor) Deactivate(ctx context.Context) error {
	m.log.InfoContext(ctx, "Actor Deactivate", "invocations", m.invocations)
	return nil
}
