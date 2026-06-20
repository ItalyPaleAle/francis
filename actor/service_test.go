package actor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
