//go:build integration

// Package ratelimit exercises the built-in rate limiter actor end to end:
//
//   - Allow throttles repeated calls for the same key down to the configured rate, rejecting excess calls with a retry-after
//   - distinct keys are limited independently, so activity on one key never throttles another
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

// per is the rate limiter's window, kept at one second so a throttled call's retry-after is comfortably above zero and bounded by the window even with cluster invocation overhead
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
	// One call per second
	// The limiter is strict by default (a single-token bucket), so the first call for a key is admitted and the next is rejected until the bucket refills
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

	// Repeated calls for the same key are throttled: the first is admitted right away, the second is rejected with a retry-after
	t.Run("throttles repeated calls for one key", func(t *testing.T) {
		const key = "throttle-key"

		allowed, _, err := rl.Allow(ctx, key)
		require.NoError(t, err)
		require.True(t, allowed, "the first call for a fresh key should be admitted")

		allowed, retryAfter, err := rl.Allow(ctx, key)
		require.NoError(t, err)
		assert.False(t, allowed, "the second call for the same key should be throttled")
		assert.Positive(t, retryAfter, "a throttled call should report how long to wait")
		assert.LessOrEqual(t, retryAfter, per, "the retry-after should not exceed the window")
	})

	// A fresh key is admitted immediately even though another key has been throttled, proving each key has its own limiter
	t.Run("limits keys independently", func(t *testing.T) {
		// Touch one key so its limiter exists and its token is spent
		allowed, _, err := rl.Allow(ctx, "other-key")
		require.NoError(t, err)
		require.True(t, allowed)

		const fresh = "fresh-key"
		allowed, retryAfter, err := rl.Allow(ctx, fresh)
		require.NoError(t, err)
		assert.True(t, allowed, "a fresh key must not be throttled by activity on a different key")
		assert.Zero(t, retryAfter, "an admitted call should not report a retry-after")
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

	_, invErr := svc.Invoke(ctx, s.rlType, key, "allow", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, _, streamErr := svc.InvokeStream(ctx, s.rlType, key, "allow", "", nil)
	require.ErrorIs(t, streamErr, actor.ErrActorTypeReserved, "host %d InvokeStream", host)

	setStateErr := svc.SetState(ctx, s.rlType, key, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	deleteStateErr := svc.DeleteState(ctx, s.rlType, key)
	require.ErrorIs(t, deleteStateErr, actor.ErrActorTypeReserved, "host %d DeleteState", host)

	haltErr := svc.Halt(s.rlType, key)
	require.ErrorIs(t, haltErr, actor.ErrActorTypeReserved, "host %d Halt", host)
}
