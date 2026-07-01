//go:build integration

// Package ratelimit exercises the built-in rate limiter actor end to end:
//
//   - Take throttles repeated calls for the same key down to the configured rate
//   - distinct keys are limited independently, so activity on one key never delays another
//   - clients cannot invoke a built-in actor directly
package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/ratelimit"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

// per is the rate limiter's window, kept at one second so the half-window threshold cleanly separates a throttled call from an immediate one even with cluster invocation overhead
const per = time.Second

// matrix runs the scenario across representative topology/provider combinations
// Multi-host entries also prove a key is consistently placed on one host, so its in-memory limiter is authoritative cluster-wide
var matrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int
}{
	{cluster.Local, provider.SQLite, 2},
	{cluster.Local, provider.StandaloneMemory, 1},
	{cluster.Remote, provider.Postgres, 2},
}

func init() {
	for _, m := range matrix {
		suite.Register(&builtinRateLimit{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}
}

// builtinRateLimit drives a cluster whose hosts register a built-in rate limiter actor and asserts its per-key behavior
type builtinRateLimit struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster *cluster.Cluster
	rl      *ratelimit.RateLimit
	rlType  string
}

func (s *builtinRateLimit) Name() string {
	return "builtinratelimit/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinRateLimit) Setup(t *testing.T) []framework.Option {
	// One call per second; the limiter is strict by default (no burst slack), so the first call for a key is immediate and the next is held for a full window
	rlActor, err := ratelimit.New(
		"e2e",
		ratelimit.WithRate(1),
		ratelimit.WithPer(per),
	)
	require.NoError(t, err)

	s.rl = rlActor

	// The host registers the actor under the reserved prefix, so the direct-target guard uses the full type
	s.rlType = builtinactor.FullActorType(rlActor.ActorType())

	s.cluster = cluster.New(t, cluster.Options{
		Kind:          s.kind,
		Variant:       s.variant,
		Hosts:         s.hosts,
		BuiltInActors: []builtinactor.BuiltInActor{rlActor},
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *builtinRateLimit) Run(t *testing.T) {
	ctx := t.Context()
	rl := s.rl.Service(s.cluster.Service(0))

	// Repeated calls for the same key are throttled: the first is admitted right away, the second waits out the window
	t.Run("throttles repeated calls for one key", func(t *testing.T) {
		const key = "throttle-key"

		start := time.Now()

		err := rl.Take(ctx, key)
		require.NoError(t, err)

		err = rl.Take(ctx, key)
		require.NoError(t, err)

		elapsed := time.Since(start)

		assert.GreaterOrEqual(t, elapsed, per/2, "the second call for the same key should be held to enforce the rate")
	})

	// A fresh key is admitted immediately even though another key has been used, proving each key has its own limiter
	t.Run("limits keys independently", func(t *testing.T) {
		// Touch one key so its limiter exists and its slot is spent
		err := rl.Take(ctx, "other-key")
		require.NoError(t, err)

		const fresh = "fresh-key"
		start := time.Now()

		err = rl.Take(ctx, fresh)
		require.NoError(t, err)

		elapsed := time.Since(start)

		assert.Less(t, elapsed, per/2, "a fresh key must not be throttled by activity on a different key")
	})

	// Clients cannot target a built-in actor through the public Service, on any host
	t.Run("cannot be targeted directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			s.assertClientRejected(t, s.cluster.Service(i), i)
		}
	})
}

// assertClientRejected checks that the Service methods that target an actor by type reject the built-in rate limiter type with ErrActorTypeReserved
func (s *builtinRateLimit) assertClientRejected(t *testing.T, svc *actor.Service, host int) {
	t.Helper()
	ctx := t.Context()

	const key = "some-key"

	_, invErr := svc.Invoke(ctx, s.rlType, key, "take", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, _, streamErr := svc.InvokeStream(ctx, s.rlType, key, "take", "", nil)
	require.ErrorIs(t, streamErr, actor.ErrActorTypeReserved, "host %d InvokeStream", host)

	setStateErr := svc.SetState(ctx, s.rlType, key, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	deleteStateErr := svc.DeleteState(ctx, s.rlType, key)
	require.ErrorIs(t, deleteStateErr, actor.ErrActorTypeReserved, "host %d DeleteState", host)

	haltErr := svc.Halt(s.rlType, key)
	require.ErrorIs(t, haltErr, actor.ErrActorTypeReserved, "host %d Halt", host)
}
