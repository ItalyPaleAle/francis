//go:build integration

// Package state holds single-host scenarios that exercise actor state and invocation against every supported provider variant, on both the local and remote runtimes
package state

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// Register all single-host scenarios
func init() {
	for i, v := range provider.All() {
		// The basic round-trip is cheap, so it runs across the full provider × topology matrix as a broad smoke test
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&roundTrip{kind: k, variant: v})
		}

		// The CRUD scenario spends real time on second-granular TTL-expiry waits, so it runs each provider on a single topology rather than the full cross product
		// Every variant is a distinct storage backend that must be covered: local vs remote only changes whether state goes direct to the embedded provider or over the runtime RPC, so alternating the topology per variant exercises both paths without doubling this slower suite
		k := cluster.Local
		if i%2 == 1 {
			k = cluster.Remote
		}
		suite.Register(&crud{kind: k, variant: v})
	}
}

// roundTrip runs one host on a given runtime and provider and checks a state round-trip and an actor invocation
type roundTrip struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *roundTrip) Name() string {
	return "state/" + string(s.kind) + "/" + string(s.variant)
}

func (s *roundTrip) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.CounterReg(time.Minute)},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *roundTrip) Run(t *testing.T) {
	svc := s.cluster.Service(0)

	// Direct state round-trip through the service
	err := svc.SetState(t.Context(), shared.CounterActorType, "x", shared.CounterState{N: 7}, nil)
	require.NoError(t, err)

	var got shared.CounterState
	err = svc.GetState(t.Context(), shared.CounterActorType, "x", &got)
	require.NoError(t, err)
	require.Equal(t, int64(7), got.N)

	// Actor invocation persists and returns the incremented value, twice
	var want int64
	for want = 1; want <= 2; want++ {
		env, err := svc.Invoke(t.Context(), shared.CounterActorType, "y", "increment", nil)
		require.NoError(t, err)
		var out shared.CounterResult
		err = env.Decode(&out)
		require.NoError(t, err)
		require.Equal(t, want, out.N)
	}
}

// crud exercises the full set of actor state operations through the service against every supported provider variant, on both the local and remote runtimes
type crud struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *crud) Name() string {
	return "statecrud/" + string(s.kind) + "/" + string(s.variant)
}

func (s *crud) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *crud) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// Reading state that was never written reports the public not-found error
	t.Run("get missing returns not found", func(t *testing.T) {
		var got shared.ProbeState
		err := svc.GetState(ctx, shared.ProbeActorType, "missing-1", &got)
		require.ErrorIs(t, err, actor.ErrStateNotFound)
	})

	// A written value round-trips back unchanged
	t.Run("set then get", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "set-1", shared.ProbeState{N: 42}, nil)
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "set-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(42), got.N)
	})

	// Writing again replaces the previous value rather than appending
	t.Run("update overwrites", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "update-1", shared.ProbeState{N: 1}, nil)
		require.NoError(t, err)
		err = svc.SetState(ctx, shared.ProbeActorType, "update-1", shared.ProbeState{N: 2}, nil)
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "update-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(2), got.N)
	})

	// Deleting removes the value, so a later read reports not found
	t.Run("delete removes", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "delete-1", shared.ProbeState{N: 7}, nil)
		require.NoError(t, err)
		err = svc.DeleteState(ctx, shared.ProbeActorType, "delete-1")
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "delete-1", &got)
		require.ErrorIs(t, err, actor.ErrStateNotFound)
	})

	// Deleting state that does not exist reports the public not-found error
	t.Run("delete missing returns not found", func(t *testing.T) {
		err := svc.DeleteState(ctx, shared.ProbeActorType, "missing-2")
		require.ErrorIs(t, err, actor.ErrStateNotFound)
	})

	// State can be written again after a delete, recreating the row
	t.Run("recreate after delete", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "recreate-1", shared.ProbeState{N: 5}, nil)
		require.NoError(t, err)
		err = svc.DeleteState(ctx, shared.ProbeActorType, "recreate-1")
		require.NoError(t, err)
		err = svc.SetState(ctx, shared.ProbeActorType, "recreate-1", shared.ProbeState{N: 9}, nil)
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "recreate-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(9), got.N)
	})

	// Each actor has its own isolated state, so writing one does not surface under another
	t.Run("state is isolated per actor", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "iso-a", shared.ProbeState{N: 100}, nil)
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "iso-b", &got)
		require.ErrorIs(t, err, actor.ErrStateNotFound)

		err = svc.GetState(ctx, shared.ProbeActorType, "iso-a", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(100), got.N)
	})

	// State written with a TTL is readable until it expires, then reads as not found
	t.Run("ttl expires state", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "ttl-1", shared.ProbeState{N: 3}, &actor.SetStateOpts{TTL: time.Second})
		require.NoError(t, err)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "ttl-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(3), got.N)

		// The TTL is enforced on read by comparing against the provider clock, so the value disappears once it elapses
		require.Eventually(t, func() bool {
			var v shared.ProbeState
			err := svc.GetState(ctx, shared.ProbeActorType, "ttl-1", &v)
			return errors.Is(err, actor.ErrStateNotFound)
		}, 10*time.Second, 200*time.Millisecond, "state with TTL should expire")
	})

	// Overwriting state without a TTL clears a previously set expiry, so the value stops expiring
	t.Run("overwrite without ttl clears expiry", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "ttl-clear-1", shared.ProbeState{N: 1}, &actor.SetStateOpts{TTL: time.Second})
		require.NoError(t, err)

		// Replace it with a TTL-less write before the first deadline passes, which upserts the row with no expiration
		err = svc.SetState(ctx, shared.ProbeActorType, "ttl-clear-1", shared.ProbeState{N: 2}, nil)
		require.NoError(t, err)

		// Across a window that spans the original one-second deadline, the value must never read as not found
		assert.Never(t, func() bool {
			var v shared.ProbeState
			return errors.Is(svc.GetState(ctx, shared.ProbeActorType, "ttl-clear-1", &v), actor.ErrStateNotFound)
		}, 1500*time.Millisecond, 200*time.Millisecond, "clearing the TTL should stop the state from expiring")

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "ttl-clear-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(2), got.N, "the overwrite value should remain")
	})

	// Overwriting state with a fresh TTL replaces the previous one, so an extended deadline keeps the value alive past the original
	t.Run("overwrite extends ttl", func(t *testing.T) {
		err := svc.SetState(ctx, shared.ProbeActorType, "ttl-extend-1", shared.ProbeState{N: 1}, &actor.SetStateOpts{TTL: time.Second})
		require.NoError(t, err)

		// Re-write with a much longer TTL before the first deadline elapses
		err = svc.SetState(ctx, shared.ProbeActorType, "ttl-extend-1", shared.ProbeState{N: 2}, &actor.SetStateOpts{TTL: 30 * time.Second})
		require.NoError(t, err)

		// The original one-second deadline must not take effect, so the value stays readable across a window that spans it
		assert.Never(t, func() bool {
			var v shared.ProbeState
			return errors.Is(svc.GetState(ctx, shared.ProbeActorType, "ttl-extend-1", &v), actor.ErrStateNotFound)
		}, 1500*time.Millisecond, 200*time.Millisecond, "a refreshed TTL should extend the expiry past the original deadline")

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "ttl-extend-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(2), got.N)
	})

	// State written through an invocation is visible to a direct service read, confirming both paths share one store
	t.Run("invocation persists state readable via service", func(t *testing.T) {
		env, err := svc.Invoke(ctx, shared.ProbeActorType, "inv-1", shared.ProbeMethodIncrement, nil)
		require.NoError(t, err)
		var out shared.ProbeState
		err = env.Decode(&out)
		require.NoError(t, err)
		assert.Equal(t, int64(1), out.N)

		var got shared.ProbeState
		err = svc.GetState(ctx, shared.ProbeActorType, "inv-1", &got)
		require.NoError(t, err)
		assert.Equal(t, int64(1), got.N)
	})
}
