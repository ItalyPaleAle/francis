package actorcore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
)

// occurrenceActor records the alarm, job and JobFailed deliveries it receives
type occurrenceActor struct {
	err error

	name      string
	method    string
	data      string
	requestID string
	jobID     string
	jobErr    error
}

func (a *occurrenceActor) record(ctx context.Context, data actor.Envelope) {
	a.requestID = actor.RequestIDFromContext(ctx)
	if data != nil {
		_ = data.Decode(&a.data)
	}
}

func (a *occurrenceActor) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	a.name = name
	a.record(ctx, data)
	return a.err
}

func (a *occurrenceActor) Job(ctx context.Context, method string, data actor.Envelope) error {
	a.method = method
	a.record(ctx, data)
	return a.err
}

func (a *occurrenceActor) JobFailed(ctx context.Context, jobID string, method string, data actor.Envelope, jobErr error) error {
	a.jobID = jobID
	a.method = method
	a.jobErr = jobErr
	a.record(ctx, data)
	return a.err
}

// runOccurrence delivers an occurrence to the "testactor" actor on its turn lock, the way the hosts do
func runOccurrence(t *testing.T, m *Manager, o Occurrence) error {
	t.Helper()
	_, err := m.LockAndInvoke(t.Context(), ref.NewActorRef("testactor", "a1"), func(ctx context.Context, act *ActiveActor) (any, error) {
		return nil, m.RunOccurrence(ctx, act, o)
	})
	return err
}

func TestRunOccurrence(t *testing.T) {
	data, err := msgpack.Marshal("payload")
	require.NoError(t, err)

	t.Run("an alarm is delivered to the Alarm method", func(t *testing.T) {
		a := &occurrenceActor{}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })

		err := runOccurrence(t, m, Occurrence{Name: "tick", Data: data, RequestID: "req-1"})
		require.NoError(t, err)
		assert.Equal(t, "tick", a.name)
		assert.Empty(t, a.method)
		assert.Equal(t, "payload", a.data)
		assert.Equal(t, "req-1", a.requestID)
	})

	t.Run("a job is delivered to the Job method", func(t *testing.T) {
		a := &occurrenceActor{}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })

		err := runOccurrence(t, m, Occurrence{Job: true, JobMethod: "work", Data: data, RequestID: "req-2"})
		require.NoError(t, err)
		assert.Equal(t, "work", a.method)
		assert.Empty(t, a.name)
		assert.Equal(t, "payload", a.data)
		assert.Equal(t, "req-2", a.requestID)
	})

	t.Run("the actor's own error is returned unwrapped", func(t *testing.T) {
		a := &occurrenceActor{err: actor.ErrJobRejected}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })

		err := runOccurrence(t, m, Occurrence{Job: true, JobMethod: "work"})
		require.ErrorIs(t, err, actor.ErrJobRejected)
		assert.NotErrorIs(t, err, ErrActorMethodUnsupported)
	})

	t.Run("an actor without the method is reported as unsupported", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)

		err := runOccurrence(t, m, Occurrence{Name: "tick"})
		require.ErrorIs(t, err, ErrActorMethodUnsupported)

		err = runOccurrence(t, m, Occurrence{Job: true, JobMethod: "work"})
		require.ErrorIs(t, err, ErrActorMethodUnsupported)
	})

	t.Run("a job whose capacity group is full is declined without running", func(t *testing.T) {
		a := &occurrenceActor{}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })
		m.capacityGroups = map[string]*capacitySemaphore{"g": newCapacitySemaphore(1)}
		m.actorTypeCapacityGroup = map[string]string{"testactor": "g"}

		// Hold the group's only slot
		release, admitted := m.TryAcquireCapacity("testactor")
		require.True(t, admitted)

		err := runOccurrence(t, m, Occurrence{Job: true, JobMethod: "work"})
		require.ErrorIs(t, err, ErrCapacityExhausted)
		assert.Empty(t, a.method)

		// Alarms are not gated by the capacity group
		err = runOccurrence(t, m, Occurrence{Name: "tick"})
		require.NoError(t, err)

		// Once the slot is free, the job runs and gives the slot back
		release()
		err = runOccurrence(t, m, Occurrence{Job: true, JobMethod: "work"})
		require.NoError(t, err)
		assert.Equal(t, "work", a.method)
		release, admitted = m.TryAcquireCapacity("testactor")
		require.True(t, admitted)
		release()
	})
}

func TestRunJobFailed(t *testing.T) {
	data, err := msgpack.Marshal("payload")
	require.NoError(t, err)
	jobErr := errors.New("boom")

	t.Run("the hook receives the job's details", func(t *testing.T) {
		a := &occurrenceActor{}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })

		err := m.RunJobFailed(t.Context(), ref.NewActorRef("testactor", "a1"), "job-1", "work", data, jobErr)
		require.NoError(t, err)
		assert.Equal(t, "job-1", a.jobID)
		assert.Equal(t, "work", a.method)
		assert.Equal(t, "payload", a.data)
		assert.Equal(t, jobErr, a.jobErr)
	})

	t.Run("the hook's error is returned", func(t *testing.T) {
		a := &occurrenceActor{err: errors.New("hook failed")}
		m := newMessagingManager(t, func(string, *actor.Service) actor.Actor { return a })

		err := m.RunJobFailed(t.Context(), ref.NewActorRef("testactor", "a1"), "job-1", "work", nil, jobErr)
		require.ErrorContains(t, err, "hook failed")
	})

	t.Run("an actor without the hook is a no-op", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)

		err := m.RunJobFailed(t.Context(), ref.NewActorRef("testactor", "a1"), "job-1", "work", nil, jobErr)
		require.NoError(t, err)
	})
}

func TestRegisterSingletonActor(t *testing.T) {
	factory := func(string, *actor.Service) actor.Actor { return struct{}{} }

	t.Run("singleton types are recorded in registration order with their bootstrap data", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterSingletonActor("b", factory, RegisterActorOptions{BootstrapData: "data"})
		require.NoError(t, err)
		err = m.RegisterActor("plain", factory, RegisterActorOptions{})
		require.NoError(t, err)
		err = m.RegisterSingletonActor("a", factory, RegisterActorOptions{})
		require.NoError(t, err)

		assert.Equal(t, []string{"b", "a"}, m.SingletonActorTypes())
		assert.Equal(t, "data", m.singletons[0].bootstrapData)
		assert.Nil(t, m.singletons[1].bootstrapData)
		assert.Len(t, m.RegisteredActorTypes(), 3)
	})

	t.Run("a rejected registration records no singleton", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterActor("dup", factory, RegisterActorOptions{})
		require.NoError(t, err)

		err = m.RegisterSingletonActor("dup", factory, RegisterActorOptions{IdleTimeout: time.Minute})
		require.ErrorIs(t, err, ErrActorTypeAlreadyRegistered)
		assert.Empty(t, m.SingletonActorTypes())
	})
}
