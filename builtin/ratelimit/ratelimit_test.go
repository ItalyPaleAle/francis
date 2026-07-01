package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/time/rate"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
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
		b, err := New("api", WithRate(2), WithPer(time.Minute), WithBurst(5), WithIdleTimeout(time.Minute))
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

	t.Run("rejects negative burst", func(t *testing.T) {
		_, err := New("x", WithRate(1), WithBurst(-1))
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
			// No WithPer leaves the period at its one-second default, so the one-minute floor applies
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

// TestNewDefaultsToStrict verifies that, without WithBurst, the bucket holds a single token so a rapid second call is rejected instead of bursting
func TestNewDefaultsToStrict(t *testing.T) {
	// Two per 50ms, default burst of one
	// With a larger burst a rapid second call would be admitted instantly, so a rejection proves strict is the default
	b, err := New("strict", WithRate(2), WithPer(50*time.Millisecond))
	require.NoError(t, err)

	a, ok := b.Factory()("k", nil).(*rateLimitActor)
	require.True(t, ok)

	// The first call is admitted: the bucket starts full with its single token
	res := invokeAllow(t, a)
	assert.True(t, res.Allowed, "the first call should be admitted")
	assert.Zero(t, res.RetryAfter)

	// The immediate second call is rejected with a positive retry-after, proving there is no burst beyond one
	res = invokeAllow(t, a)
	assert.False(t, res.Allowed, "the second call should be throttled with the default single-token bucket")
	assert.Positive(t, res.RetryAfter, "a throttled call must report how long to wait")
	assert.LessOrEqual(t, res.RetryAfter, 50*time.Millisecond, "the retry-after should not exceed the window")
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

func TestInvokeAllow(t *testing.T) {
	ctx := t.Context()

	t.Run("admits a call under a high rate", func(t *testing.T) {
		a := &rateLimitActor{limiter: rate.NewLimiter(1_000_000, 1)}
		res := invokeAllow(t, a)
		assert.True(t, res.Allowed)
		assert.Zero(t, res.RetryAfter)
	})

	t.Run("throttles to the configured rate", func(t *testing.T) {
		// One per hour with a single-token bucket: the first call is admitted and drains the bucket, the next is throttled for nearly the whole window
		a := &rateLimitActor{limiter: rate.NewLimiter(rate.Every(time.Hour), 1)}

		first := invokeAllow(t, a)
		require.True(t, first.Allowed, "the first call should be admitted")

		second := invokeAllow(t, a)
		assert.False(t, second.Allowed, "the second call should be throttled")
		assert.Positive(t, second.RetryAfter, "a throttled call must report how long to wait")
	})

	t.Run("a rejected call does not drain the bucket", func(t *testing.T) {
		// A single-token bucket that refills slowly: after the first admit, repeated rejects must all report a retry-after rather than pushing it ever further out
		a := &rateLimitActor{limiter: rate.NewLimiter(rate.Every(time.Hour), 1)}

		require.True(t, invokeAllow(t, a).Allowed)

		firstReject := invokeAllow(t, a)
		require.False(t, firstReject.Allowed)

		secondReject := invokeAllow(t, a)
		require.False(t, secondReject.Allowed)
		// Because the rejected call handed its token back, the wait does not grow with each throttled attempt
		assert.LessOrEqual(t, secondReject.RetryAfter, firstReject.RetryAfter, "a rejected call must not consume a token")
	})

	t.Run("rejects an unknown method", func(t *testing.T) {
		a := &rateLimitActor{limiter: rate.NewLimiter(1_000_000, 1)}
		_, err := a.Invoke(ctx, "bogus", nil)
		require.Error(t, err)
	})

	t.Run("returns the context error without touching the limiter when already cancelled", func(t *testing.T) {
		a := &rateLimitActor{limiter: rate.NewLimiter(1_000_000, 1)}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := a.Invoke(cancelled, methodAllow, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestAllowResultRoundTrip verifies the allow reply survives the msgpack round-trip used for cross-host invocations.
// The owning host marshals the actor's return value, the caller decodes the body into a generic any, and the envelope then decodes it into allowResult
func TestAllowResultRoundTrip(t *testing.T) {
	cases := map[string]allowResult{
		"admitted":  {Allowed: true},
		"throttled": {Allowed: false, RetryAfter: 250 * time.Millisecond},
	}

	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			// The owning host marshals the actor's return value for the response body
			body, err := msgpack.Marshal(want)
			require.NoError(t, err)

			// The caller decodes the response body into a generic any, exactly like the peer invocation path
			var out any
			require.NoError(t, msgpack.Unmarshal(body, &out))

			// The envelope re-encodes the generic value and decodes it into the concrete result type
			var got allowResult
			require.NoError(t, actorcore.NewObjectEnvelope(out).Decode(&got))

			assert.Equal(t, want, got)
		})
	}
}

func TestServiceAllowValidation(t *testing.T) {
	ctx := t.Context()
	// A service with a nil Service never reaches the host: key validation happens first and these calls fail before any invocation
	svc := (&RateLimit{actorType: rateLimitActorTypePrefix + "api"}).Service(nil)

	t.Run("rejects empty key", func(t *testing.T) {
		_, _, err := svc.Allow(ctx, "")
		require.Error(t, err)
	})

	t.Run("rejects key with slash", func(t *testing.T) {
		_, _, err := svc.Allow(ctx, "a/b")
		require.Error(t, err)
	})
}

// invokeAllow drives the actor's allow method and returns the decoded result, failing the test on any error
func invokeAllow(t *testing.T, a *rateLimitActor) allowResult {
	t.Helper()

	out, err := a.Invoke(t.Context(), methodAllow, nil)
	require.NoError(t, err)

	res, ok := out.(allowResult)
	require.True(t, ok, "allow should return an allowResult")
	return res
}

var (
	_ builtinactor.BuiltInActor = (*RateLimit)(nil)
	_ actor.ActorInvoke         = (*rateLimitActor)(nil)
)
