package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/ratelimit"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
)

func TestNew(t *testing.T) {
	t.Run("valid rate", func(t *testing.T) {
		b, err := New("api", WithRate(100))
		require.NoError(t, err)
		assert.Equal(t, rateLimitActorTypePrefix+"api", b.ActorType())
		assert.NotNil(t, b.Factory())
	})

	t.Run("valid with all options", func(t *testing.T) {
		b, err := New("api", WithRate(2), WithPer(time.Minute), WithSlack(5), WithIdleTimeout(time.Minute))
		require.NoError(t, err)
		assert.Equal(t, time.Minute, b.RegisterOptions().IdleTimeout)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := New("", WithRate(100))
		require.Error(t, err)
	})

	t.Run("rejects name with slash", func(t *testing.T) {
		_, err := New("a/b", WithRate(100))
		require.Error(t, err)
	})

	t.Run("rejects missing rate", func(t *testing.T) {
		_, err := New("x")
		require.Error(t, err)
	})

	t.Run("rejects zero rate", func(t *testing.T) {
		_, err := New("x", WithRate(0))
		require.Error(t, err)
	})

	t.Run("rejects negative rate", func(t *testing.T) {
		_, err := New("x", WithRate(-1))
		require.Error(t, err)
	})

	t.Run("rejects negative period", func(t *testing.T) {
		_, err := New("x", WithRate(1), WithPer(-time.Second))
		require.Error(t, err)
	})

	t.Run("rejects negative slack", func(t *testing.T) {
		_, err := New("x", WithRate(1), WithSlack(-1))
		require.Error(t, err)
	})
}

// TestNewDefaultIdleTimeout verifies the default idle timeout is double the period floored at one minute, and that an explicit WithIdleTimeout overrides it
func TestNewDefaultIdleTimeout(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{
			// No WithPer leaves the period at zero, so the one-minute floor applies
			name: "no period falls back to the one-minute floor",
			opts: []Option{WithRate(100)},
			want: time.Minute,
		},
		{
			// Double a short period is still below the floor
			name: "short period is floored at one minute",
			opts: []Option{WithRate(100), WithPer(10 * time.Second)},
			want: time.Minute,
		},
		{
			// Double this period is above the floor and is used as-is
			name: "period whose double exceeds the floor",
			opts: []Option{WithRate(100), WithPer(40 * time.Second)},
			want: 80 * time.Second,
		},
		{
			// An explicit timeout wins outright, even when it is far below double the period
			name: "explicit idle timeout overrides the default",
			opts: []Option{WithRate(100), WithPer(time.Hour), WithIdleTimeout(5 * time.Second)},
			want: 5 * time.Second,
		},
		{
			// A non-positive timeout is treated as unset and falls back to the default
			name: "non-positive idle timeout falls back to the default",
			opts: []Option{WithRate(100), WithPer(time.Minute), WithIdleTimeout(-1)},
			want: 2 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New("idle", tc.opts...)
			require.NoError(t, err)

			got := b.RegisterOptions().IdleTimeout
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestNewDefaultsToStrict verifies that, without WithSlack, the limiter is strict and spaces calls out rather than admitting an initial burst
func TestNewDefaultsToStrict(t *testing.T) {
	// Two per 50ms
	// With the library's default slack of 10 a rapid burst would all be admitted instantly, so real spacing proves strict is the default
	b, err := New("strict", WithRate(2), WithPer(50*time.Millisecond))
	require.NoError(t, err)

	a, ok := b.Factory()("k", nil).(*rateLimitActor)
	require.True(t, ok)

	start := time.Now()
	for range 3 {
		_, takeErr := a.Invoke(t.Context(), methodTake, nil)
		require.NoError(t, takeErr)
	}
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "the default limiter should be strict, spacing calls out instead of bursting")
}

// TestFactoryBuildsRateLimitActor verifies the factory builds a rate limit actor with a live limiter for any actor ID (key)
func TestFactoryBuildsRateLimitActor(t *testing.T) {
	b, err := New("keys", WithRate(100))
	require.NoError(t, err)

	for _, key := range []string{"1.2.3.4", "user-42", "singleton"} {
		a, ok := b.Factory()(key, nil).(*rateLimitActor)
		require.True(t, ok, "the factory should build a rate limit actor for key %q", key)
		require.NotNil(t, a.limiter, "the actor should carry a limiter")
	}
}

func TestInvokeTake(t *testing.T) {
	ctx := t.Context()

	t.Run("admits a call under a high rate", func(t *testing.T) {
		a := &rateLimitActor{limiter: ratelimit.New(1_000_000)}
		_, err := a.Invoke(ctx, methodTake, nil)
		require.NoError(t, err)
	})

	t.Run("throttles to the configured rate", func(t *testing.T) {
		// Two per 50ms without slack: after the first immediate admit, the next is held until the window elapses
		a := &rateLimitActor{limiter: ratelimit.New(2, ratelimit.Per(50*time.Millisecond), ratelimit.WithoutSlack)}

		start := time.Now()
		for range 3 {
			_, err := a.Invoke(ctx, methodTake, nil)
			require.NoError(t, err)
		}
		// Three calls at two-per-50ms means at least one full inter-call gap was enforced
		assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "the limiter should have blocked to enforce the rate")
	})

	t.Run("rejects an unknown method", func(t *testing.T) {
		a := &rateLimitActor{limiter: ratelimit.New(1_000_000)}
		_, err := a.Invoke(ctx, "bogus", nil)
		require.Error(t, err)
	})

	t.Run("returns the context error without blocking when already cancelled", func(t *testing.T) {
		// A strict, very slow limiter would block for an hour on the second take, so this proves the cancelled context short-circuits before Take
		a := &rateLimitActor{limiter: ratelimit.New(1, ratelimit.Per(time.Hour), ratelimit.WithoutSlack)}

		// The first take is admitted immediately, draining the only slot in the window
		_, err := a.Invoke(ctx, methodTake, nil)
		require.NoError(t, err)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = a.Invoke(cancelled, methodTake, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestServiceTakeValidation(t *testing.T) {
	ctx := t.Context()
	// A service with a nil Service never reaches the host: key validation happens first and these calls fail before any invocation
	svc := (&RateLimit{actorType: rateLimitActorTypePrefix + "api"}).Service(nil)

	t.Run("rejects empty key", func(t *testing.T) {
		err := svc.Take(ctx, "")
		require.Error(t, err)
	})

	t.Run("rejects key with slash", func(t *testing.T) {
		err := svc.Take(ctx, "a/b")
		require.Error(t, err)
	})
}

var (
	_ builtinactor.BuiltInActor = (*RateLimit)(nil)
	_ actor.ActorInvoke         = (*rateLimitActor)(nil)
)
