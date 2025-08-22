package main

import (
	"context"
	"log/slog"

	"github.com/italypaleale/actors/actor"
)

type MyActor struct {
	count uint64
	log   *slog.Logger
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
		log: log,
	}
}

func (m *MyActor) Invoke(ctx context.Context, method string, data any) (any, error) {
	m.count++

	m.log.InfoContext(ctx, "Actor Invoked", "method", method, "count", m.count)

	return m.count, nil
}

func (m *MyActor) Alarm(ctx context.Context, name string, data any) error {
	m.log.InfoContext(ctx, "Actor Alarm", "name", name)
	return nil
}

func (m *MyActor) Deactivate(ctx context.Context) error {
	m.log.InfoContext(ctx, "Actor Deactivate")
	return nil
}
