package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

func TestNewRejectsAnInvalidName(t *testing.T) {
	tests := []struct {
		name    string
		wfName  string
		wantErr string
	}{
		{name: "empty", wfName: "", wantErr: "workflow name is required"},
		{name: "path separator", wfName: "orders/ship", wantErr: "invalid workflow name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.wfName, WithSteps(Step("a", WithRun(noopRun))))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewRejectsInvalidCapabilities(t *testing.T) {
	tests := []struct {
		name         string
		capabilities []string
		wantErr      string
	}{
		{name: "empty", capabilities: []string{""}, wantErr: "capability name must not be empty"},
		{name: "path separator", capabilities: []string{"gpu/large"}, wantErr: "invalid capability"},
		{name: "declared twice", capabilities: []string{"gpu", "gpu"}, wantErr: "declared more than once"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []Option{WithSteps(Step("a", WithRun(noopRun)))}
			for _, capName := range tt.capabilities {
				opts = append(opts, WithCapability(capName))
			}

			_, err := New("caps", opts...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestApplyDefaultsFillsEveryUnsetOption(t *testing.T) {
	// The rest of the engine reads these values without asking whether the caller set them, so every one of them has to come out of New with something usable
	def := testDefinition(t, "defaults", WithSteps(Step("a", WithRun(noopRun))))

	assert.Equal(t, defaultVersion, def.version)
	assert.Equal(t, defaultTimeout, def.timeout)
	assert.Equal(t, defaultMaxInputSize, def.maxInputSize)
	assert.Equal(t, defaultMaxOutputSize, def.maxOutputSize)
	assert.Equal(t, defaultMaxJournalSize, def.maxJournalSize)
	assert.Equal(t, defaultMaxDepth, def.maxDepth)
	assert.Equal(t, ParkUnknownVersion, def.unknownVersion)
	assert.Equal(t, ContinueUnwinding, def.compensationFailurePolicy)
}

func TestExplicitOptionsSurviveTheDefaults(t *testing.T) {
	// A caller's value has to win, including where it is smaller than the default, since these are the caps an operator tunes down
	def := testDefinition(t, "explicit",
		WithVersion(7),
		WithTimeout(time.Minute),
		WithMaxInputSize(64),
		WithMaxOutputSize(128),
		WithMaxJournalSize(256),
		WithMaxDepth(2),
		WithUnknownVersionPolicy(FailUnknownVersion),
		WithCompensationFailurePolicy(AbortUnwinding),
		WithSteps(Step("a", WithRun(noopRun))),
	)

	assert.Equal(t, 7, def.version)
	assert.Equal(t, time.Minute, def.timeout)
	assert.Equal(t, 64, def.maxInputSize)
	assert.Equal(t, 128, def.maxOutputSize)
	assert.Equal(t, 256, def.maxJournalSize)
	assert.Equal(t, 2, def.maxDepth)
	assert.Equal(t, FailUnknownVersion, def.unknownVersion)
	assert.Equal(t, AbortUnwinding, def.compensationFailurePolicy)
}

func TestCompensationRetryPolicyIsPerStep(t *testing.T) {
	def := testDefinition(t, "comp-policy", WithSteps(
		Step("tuned", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(3), WithCompensateBackoff(time.Second, 5*time.Second)),
		Step("default", WithRun(noopRun), WithCompensate(noopCompensate)),
	))

	tuned := def.byName["tuned"]
	assert.Equal(t, 3, effectiveCompMaxAttempts(tuned))
	assert.Equal(t, time.Second, backoff(tuned.compInitial, tuned.compMax, defaultCompInitial, defaultCompMax, 1))
	assert.Equal(t, 4*time.Second, backoff(tuned.compInitial, tuned.compMax, defaultCompInitial, defaultCompMax, 3))
	assert.Equal(t, 5*time.Second, backoff(tuned.compInitial, tuned.compMax, defaultCompInitial, defaultCompMax, 10), "the doubling stops at the step's cap")

	// An unset policy stays zero on the step and is resolved against the engine's defaults where the delay is computed, so nothing copies the defaults into the graph
	plain := def.byName["default"]
	assert.Zero(t, plain.compInitial)
	assert.Zero(t, plain.compMax)
	assert.Equal(t, defaultCompMaxAttempts, effectiveCompMaxAttempts(plain))
	assert.Equal(t, defaultCompInitial, backoff(plain.compInitial, plain.compMax, defaultCompInitial, defaultCompMax, 1))
}

func TestValidateInstanceIDRejectsAnAmbiguousID(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "path separator", id: "a/b"},
		{name: "worker ID delimiter", id: "a|b"},
		{name: "empty", id: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInstanceID(tt.id)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid instance ID")
		})
	}

	assert.NoError(t, validateInstanceID("order-1234"))
}

func TestEncodeInputEnforcesTheCap(t *testing.T) {
	// A nil input encodes to nothing rather than to "null", so a step that decodes it sees an untouched value
	enc, err := encodeInput(nil, 10)
	require.NoError(t, err)
	assert.Nil(t, enc)

	enc, err = encodeInput("ok", 10)
	require.NoError(t, err)
	assert.JSONEq(t, `"ok"`, string(enc))

	_, err = encodeInput("far too long to fit", 10)
	require.ErrorIs(t, err, ErrInputTooLarge)

	// A value JSON cannot represent fails the same way on every call, so it is rejected before anything durable happens
	_, err = encodeInput(make(chan int), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to encode the workflow input")

	// A zero limit means no cap, which is what lets an operator turn the check off
	_, err = encodeInput("far too long to fit", 0)
	require.NoError(t, err)
}

func TestHasEventMatchesTheEffectiveName(t *testing.T) {
	wf, err := New("events", WithSteps(
		WaitForEvent("plain"),
		WaitForEvent("renamed", WithEventName("external-name")),
	))
	require.NoError(t, err)

	assert.True(t, wf.hasEvent("plain"), "a wait step's name is its event name by default")
	assert.True(t, wf.hasEvent("external-name"))
	assert.False(t, wf.hasEvent("renamed"), "WithEventName replaces the step's name rather than adding to it")
	assert.False(t, wf.hasEvent("nothing-listens-for-this"))
}

func TestRetryWhilePlacementMovesWaitsForThePlacementToSettle(t *testing.T) {
	// Each of these says where the actor lives is still being settled, which a caller sees whenever it reaches an instance that is terminating, being purged, or moving with a rebalance
	for _, moving := range []error{actor.ErrActorHalted, actor.ErrActorNotHosted, actor.ErrActorNotActive} {
		t.Run(moving.Error(), func(t *testing.T) {
			calls := 0
			env, err := retryWhilePlacementMoves(t.Context(), func(ctx context.Context) (actor.Envelope, error) {
				calls++
				if calls < 3 {
					return nil, fmt.Errorf("invoking the instance: %w", moving)
				}
				return &fakeEnvelope{value: "settled"}, nil
			})
			require.NoError(t, err)
			require.NotNil(t, env)
			assert.Equal(t, 3, calls)
		})
	}

	t.Run("any other error is the caller's answer", func(t *testing.T) {
		calls := 0
		_, err := retryWhilePlacementMoves(t.Context(), func(ctx context.Context) (actor.Envelope, error) {
			calls++
			return nil, errors.New("the provider is down")
		})
		require.Error(t, err)
		assert.Equal(t, 1, calls)
	})

	t.Run("a placement that never settles gives up", func(t *testing.T) {
		calls := 0
		_, err := retryWhilePlacementMoves(t.Context(), func(ctx context.Context) (actor.Envelope, error) {
			calls++
			return nil, actor.ErrActorHalted
		})
		require.ErrorIs(t, err, actor.ErrActorHalted)
		assert.Equal(t, placementRetryAttempts, calls, "the wait is bounded, so a caller is never blocked indefinitely")
	})
}
