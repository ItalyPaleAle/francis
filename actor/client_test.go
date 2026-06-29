package actor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
)

func TestClientCanTarget(t *testing.T) {
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	// A normal client cannot target built-in actors, but can target ordinary ones
	pub := &client[any]{}
	assert.False(t, pub.canTarget(builtInType))
	assert.True(t, pub.canTarget("ordinary"))

	// The privileged built-in client can target anything
	priv := &client[any]{privileged: true}
	assert.True(t, priv.canTarget(builtInType))
	assert.True(t, priv.canTarget("ordinary"))
}

// TestClientRejectsBuiltInTarget verifies the public client refuses to operate on a built-in actor, returning before it reaches the host
func TestClientRejectsBuiltInTarget(t *testing.T) {
	ctx := context.Background()
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	// The guard returns before the nil host is dereferenced, so a zero-value Service is enough
	c := NewActorClient[map[string]any](builtInType, "singleton", &Service{})

	err := c.SetState(ctx, nil, nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)

	_, getErr := c.GetState(ctx)
	require.ErrorIs(t, getErr, ErrActorTypeReserved)

	err = c.DeleteState(ctx)
	require.ErrorIs(t, err, ErrActorTypeReserved)
	err = c.SetAlarm(ctx, "a", AlarmProperties{})
	require.ErrorIs(t, err, ErrActorTypeReserved)
	err = c.DeleteAlarm(ctx, "a")
	require.ErrorIs(t, err, ErrActorTypeReserved)

	_, dispatchErr := c.Dispatch(ctx, "run", nil)
	require.ErrorIs(t, dispatchErr, ErrActorTypeReserved)

	_, listErr := c.ListJobs(ctx)
	require.ErrorIs(t, listErr, ErrActorTypeReserved)

	err = c.CancelJob(ctx, "job")
	require.ErrorIs(t, err, ErrActorTypeReserved)

	// Invoking a built-in target is rejected regardless of which actor the client is bound to
	_, err = c.Invoke(ctx, builtInType, "singleton", "run", nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)
}

// TestNewBuiltInActorClientIsPrivileged verifies the framework's client skips the built-in guard
func TestNewBuiltInActorClientIsPrivileged(t *testing.T) {
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	c := NewBuiltInActorClient[map[string]any](builtinkey.Key{}, builtInType, "singleton", &Service{})
	cc, ok := c.(*client[map[string]any])
	require.True(t, ok)
	assert.True(t, cc.privileged)
	assert.True(t, cc.canTarget(builtInType))
}
