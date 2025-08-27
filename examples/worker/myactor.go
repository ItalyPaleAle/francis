package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
	log := log.
		With(
			slog.String("scope", "actor"),
			slog.String("actorType", "myactor"),
			slog.String("actorID", actorID),
		)

	log.Info("Actor created")

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

	if strings.HasSuffix(method, "-wait") {
		method = strings.TrimSuffix(method, "-wait")

		const waitTime = 2500 * time.Millisecond
		select {
		case <-time.After(waitTime):
			// All good
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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

	m.log.InfoContext(ctx, "Actor invoked", "method", method, "counter", state.Counter, "invocations", m.invocations)

	return state.Counter, nil
}

func (m *MyActor) Alarm(ctx context.Context, name string, data any) error {
	m.log.InfoContext(ctx, "Actor received alarm", "name", name)
	return nil
}

func (m *MyActor) Deactivate(ctx context.Context) error {
	m.log.InfoContext(ctx, "Actor deactivated", "invocations", m.invocations)
	return nil
}
