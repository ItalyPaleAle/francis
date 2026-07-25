package actor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/ref"
)

// TestServiceNotInitialized verifies that a zero-value Service surfaces ErrServiceNotInitialized instead of panicking on a nil host
func TestServiceNotInitialized(t *testing.T) {
	ctx := context.Background()

	// A zero-value Service has no host
	var s Service

	// Methods that return an error report it rather than dereferencing the nil host
	_, err := s.Invoke(ctx, "type", "id", "method", nil)
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	_, _, err = s.InvokeStream(ctx, "type", "id", "method", "", nil)
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.SetState(ctx, "type", "id", nil, nil)
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.GetState(ctx, "type", "id", nil)
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.DeleteState(ctx, "type", "id")
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	_, err = s.ListStates(ctx, "type", nil)
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.SetAlarm(ctx, "type", "id", "alarm", AlarmProperties{})
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.DeleteAlarm(ctx, "type", "id", "alarm")
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.HaltAll()
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	err = s.Halt("type", "id")
	require.ErrorIs(t, err, ErrServiceNotInitialized)

	// HaltDeferred has no error channel, so it must simply not panic on an uninitialized Service
	assert.NotPanics(t, func() {
		s.HaltDeferred("type", "id")
	})
}

// TestServiceListStates verifies the Service forwards a listing to the host and refuses to list a built-in actor type
func TestServiceListStates(t *testing.T) {
	host := &fakeHost{
		listStatesRes: StateList{
			States:  []StateInfo{{ActorID: "w1"}, {ActorID: "w2"}},
			HasMore: true,
		},
	}
	s := NewService(host)

	opts := &ListStatesOpts{After: "w0", Limit: 2}
	res, err := s.ListStates(t.Context(), "widget", opts)
	require.NoError(t, err)
	assert.Equal(t, "widget", host.listStatesType)
	assert.Same(t, opts, host.listStatesOpts)
	assert.True(t, res.HasMore)
	assert.Len(t, res.States, 2)

	// Built-in actor types are not addressable by clients, so the guard returns before the host is reached
	_, err = s.ListStates(t.Context(), ref.BuiltInActorTypePrefix+"cronjob.test", nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)
}
