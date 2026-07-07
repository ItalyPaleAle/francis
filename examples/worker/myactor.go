package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/italypaleale/francis/actor"
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

func (m *MyActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	var d struct {
		In int64
	}
	if data != nil {
		err := data.Decode(&d)
		if err != nil {
			return nil, fmt.Errorf("failed to decode data: %w", err)
		}
	}

	m.invocations++

	state, err := m.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("error retrieving state: %w", err)
	}

	before, ok := strings.CutSuffix(method, "-wait")
	if ok {
		method = before

		const waitTime = 25000 * time.Millisecond
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
	case "schedule-job":
		// Demonstrate self-dispatch: the actor enqueues a durable background job to itself
		// The job runs immediately on whatever host serves this actor, delivered to the Job method below
		jobID, err := m.client.Dispatch(ctx, "report", map[string]any{
			"counter": state.Counter,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to dispatch job: %w", err)
		}
		m.log.InfoContext(ctx, "Dispatched background job to self", slog.String("jobID", jobID))
	}

	err = m.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, fmt.Errorf("error saving state: %w", err)
	}

	m.log.InfoContext(ctx, "Actor invoked", "method", method, "counter", state.Counter, "invocations", m.invocations, "in", d.In)

	var res struct {
		Out int64
	}
	res.Out = state.Counter

	return res, nil
}

// Peek returns the counter without touching it, so many concurrent Peek calls can run at once
// Unlike Invoke, Peek must never mutate the actor's persisted state or in-memory fields, since other Peek calls could be reading them concurrently
func (m *MyActor) Peek(ctx context.Context, method string, data actor.Envelope) (any, error) {
	state, err := m.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("error retrieving state: %w", err)
	}

	// A "-wait" suffix simulates a slow read, demonstrating that concurrent Peek calls overlap instead of queuing behind each other like Invoke does
	before, ok := strings.CutSuffix(method, "-wait")
	if ok {
		method = before

		const waitTime = 10000 * time.Millisecond
		select {
		case <-time.After(waitTime):
			// All good
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.log.InfoContext(ctx, "Actor peeked", "method", method, "counter", state.Counter)

	var res struct {
		Out int64
	}
	res.Out = state.Counter

	return res, nil
}

func (m *MyActor) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	var d struct {
		Hello string
	}
	if data != nil {
		err := data.Decode(&d)
		if err != nil {
			return fmt.Errorf("failed to decode data: %w", err)
		}
	}

	m.log.InfoContext(ctx, "Actor received alarm", slog.String("name", name), slog.String("data.hello", d.Hello))

	return nil
}

func (m *MyActor) Job(ctx context.Context, method string, data actor.Envelope) error {
	var d struct {
		Counter int64
	}
	if data != nil {
		err := data.Decode(&d)
		if err != nil {
			return fmt.Errorf("failed to decode data: %w", err)
		}
	}

	m.log.InfoContext(ctx, "Actor ran background job", slog.String("method", method), slog.Int64("data.counter", d.Counter))

	return nil
}

// JobFailed is the optional reaction hook, called best-effort after one of this actor's jobs is dead-lettered.
func (m *MyActor) JobFailed(ctx context.Context, jobID string, method string, data actor.Envelope, jobErr error) error {
	m.log.WarnContext(ctx, "Background job was dead-lettered",
		slog.String("jobID", jobID),
		slog.String("method", method),
		slog.Any("error", jobErr),
	)
	return nil
}

func (m *MyActor) Deactivate(ctx context.Context) error {
	time.Sleep(time.Second)
	m.log.InfoContext(ctx, "Actor deactivated", "invocations", m.invocations)
	return nil
}
