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
	assert.Equal(t, 3, tuned.compMaxAttempt)
	assert.Equal(t, time.Second, backoff(tuned.compInitial, tuned.compMax, 1))
	assert.Equal(t, 4*time.Second, backoff(tuned.compInitial, tuned.compMax, 3))
	assert.Equal(t, 5*time.Second, backoff(tuned.compInitial, tuned.compMax, 10), "the doubling stops at the step's cap")

	// The compiler resolves defaults into the IR so the runtime plan and its canonical hash describe the policy actually enforced
	plain := def.byName["default"]
	assert.Equal(t, defaultCompInitial, plain.compInitial)
	assert.Equal(t, defaultCompMax, plain.compMax)
	assert.Equal(t, defaultCompMaxAttempts, plain.compMaxAttempt)
	assert.Equal(t, defaultCompInitial, backoff(plain.compInitial, plain.compMax, 1))
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

func TestNewRejectsUninitializedDeclarations(t *testing.T) {
	// Invalid values can be assembled through the exported API and must produce construction errors rather than panics
	tests := []struct {
		name  string
		steps []StepSpec
	}{
		{"zero step", []StepSpec{{}}},
		{"zero member", []StepSpec{Parallel("group", StepSpec{})}},
		{"configured zero step", []StepSpec{StepSpec{}.With(WithRun(noopRun))}},
		{"uninitialized child", []StepSpec{Child("child", WithDefinition(&Workflow{}))}},
		{"uninitialized fan-out child", []StepSpec{Step("items", WithRun(noopRun)), ForEach("children", WithItemsFrom("items"), WithChild(&Workflow{}))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := New("invalid-declarations", WithSteps(tt.steps...))
				require.ErrorContains(t, err, "uninitialized")
			})
		})
	}
}

func TestNewRejectsUnknownPolicies(t *testing.T) {
	// Values decoded from configuration must not fall through to a different engine policy
	tests := []struct {
		name string
		opts []Option
	}{
		{"unknown version", []Option{WithUnknownVersionPolicy("parkk"), WithSteps(WaitForEvent("event"))}},
		{"compensation failure", []Option{WithCompensationFailurePolicy("abortt"), WithSteps(Step("work", WithRun(noopRun)))}},
		{"group failure", []Option{WithSteps(Parallel("group", Step("member", WithRun(noopRun))).With(WithFailurePolicy("failfast")))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("invalid-policies", tt.opts...)
			require.ErrorContains(t, err, "unknown")
		})
	}
}

func TestNewRejectsIgnoredParallelMemberOptions(t *testing.T) {
	// These settings govern whole steps and are not implemented for individual parallel tasks
	tests := []struct {
		name string
		opt  StepOption
	}{
		{"WithSkipIf", WithSkipIf("source", false)},
		{"WithOptional", WithOptional()},
		{"WithSkipOnFailure", WithSkipOnFailure("dependent")},
		{"WithStepTimeout", WithStepTimeout(time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("member-options", WithSteps(
				Step("source", WithRun(noopRun)),
				Parallel("group", Step("member", WithRun(noopRun), tt.opt)),
				Step("dependent", WithRun(noopRun)),
			))
			require.ErrorContains(t, err, "parallel member")
			require.ErrorContains(t, err, tt.name)
		})
	}
}

func TestNewRejectsOptionsOnIncompatibleKinds(t *testing.T) {
	child, err := New("option-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)

	// The applicability matrix prevents unused handlers, routing settings, and data dependencies from looking effective
	tests := []struct {
		name string
		step StepSpec
	}{
		{"step event name", Step("target", WithRun(noopRun), WithEventName("event"))},
		{"step fan-out source", Step("target", WithRun(noopRun), WithItemsFrom("source"))},
		{"step group policy", Step("target", WithRun(noopRun), WithFailurePolicy(TolerateFailures))},
		{"step child definition", Step("target", WithRun(noopRun), WithDefinition(child))},
		{"group handler", Parallel("target", Step("member", WithRun(noopRun))).With(WithRun(noopRun))},
		{"group compensation", Parallel("target", Step("member", WithRun(noopRun))).With(WithCompensate(noopCompensate))},
		{"group capability", Parallel("target", Step("member", WithRun(noopRun))).With(WithRequiredCapability("gpu"))},
		{"wait optional", WaitForEvent("target", WithOptional())},
		{"wait input dependencies", WaitForEvent("target", WithInputFrom("source"))},
		{"wait conflicting timeouts", WaitForEvent("target", WithEventTimeout(time.Second), WithStepTimeout(time.Minute))},
		{"child handler", Child("target", WithDefinition(child), WithRun(noopRun))},
		{"child input dependencies", Child("target", WithDefinition(child), WithInputFrom("source"))},
		{"child capability", Child("target", WithDefinition(child), WithRequiredCapability("gpu"))},
		{"child fan-out compensation", ForEach("target", WithItemsFrom("source"), WithChild(child), WithCompensate(noopCompensate))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, newErr := New("kind-options", WithSteps(Step("source", WithRun(noopRun)), tt.step))
			require.ErrorContains(t, newErr, "cannot")
		})
	}
}

func TestSupportedParallelAndChildOptionsRemainValid(t *testing.T) {
	// Keep supported member policies and group-wide settings available while rejecting only ignored combinations
	child, err := New("supported-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	_, err = New("supported-options", WithSteps(
		Step("source", WithRun(noopRun)),
		Parallel("group",
			Step("member", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateOnFailure(), WithMaxAttempts(5), WithRetryBackoff(time.Second, time.Minute), WithCompensateBackoff(time.Second, time.Minute), WithInputFrom("source"), WithRequiredCapability("gpu")),
			Child("child", WithDefinition(child), WithCompensateMaxAttempts(3)),
		).With(WithInputFrom("source"), WithOptional(), WithSkipIf("source", false), WithStepTimeout(time.Minute), WithFailurePolicy(CollectFailures)),
		ForEach("fan", WithItemsFrom("source"), WithRun(noopRun), WithMaxParallel(2), WithFailurePolicy(TolerateFailures)),
		WaitForEvent("event", WithSkipIf("source", true), WithEventTimeout(time.Minute)),
	))
	require.NoError(t, err)
}

func TestStepSpecReuseDoesNotMutateBuiltDefinition(t *testing.T) {
	// Reusing a declaration for a second workflow must preserve the first workflow's validated attempt policy
	spec := Step("charge", WithRun(noopRun), WithMaxAttempts(1))
	first, err := New("first", WithSteps(spec))
	require.NoError(t, err)
	originalFingerprint := first.def.fingerprint
	_, err = New("second", WithSteps(spec.With(WithMaxAttempts(9))))
	require.NoError(t, err)
	assert.Equal(t, 1, first.def.byName["charge"].maxAttempts, "building another workflow mutated the existing definition")

	// Recomputing the fingerprint reveals that the live graph no longer matches the fingerprint cached at construction
	err = first.def.setFingerprint()
	require.NoError(t, err)
	assert.Equal(t, originalFingerprint, first.def.fingerprint, "the graph changed behind its previously published fingerprint")
}

func TestNewRejectsAnInvalidLoop(t *testing.T) {
	tests := []struct {
		name    string
		spec    StepSpec
		wantErr string
	}{
		{
			name:    "no body",
			spec:    Loop("poll").With(WithUntil("check", true)),
			wantErr: "requires at least one step in its body",
		},
		{
			name:    "no condition",
			spec:    Loop("poll", Step("check", WithRun(noopRun))),
			wantErr: "requires WithUntil",
		},
		{
			name:    "condition outside the body",
			spec:    Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("elsewhere", true)),
			wantErr: `names "elsewhere" in WithUntil, which is not a step of its body`,
		},
		{
			name:    "negative bound",
			spec:    Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", true), WithMaxIterations(-1)),
			wantErr: "negative WithMaxIterations",
		},
		{
			name: "parallel group in the body",
			spec: Loop("poll",
				Parallel("group", Step("m1", WithRun(noopRun)), Step("m2", WithRun(noopRun))),
			).With(WithUntil("group", true)),
			wantErr: "may only contain plain, child, or wait steps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("looped", WithSteps(tt.spec))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewRejectsLoopOptionsOnOtherKinds(t *testing.T) {
	tests := []struct {
		name    string
		spec    StepSpec
		wantErr string
	}{
		{
			name:    "WithUntil on a plain step",
			spec:    Step("a", WithRun(noopRun), WithUntil("a", true)),
			wantErr: "cannot use WithUntil on a step node",
		},
		{
			name:    "WithMaxIterations on a plain step",
			spec:    Step("a", WithRun(noopRun), WithMaxIterations(3)),
			wantErr: "cannot use WithMaxIterations on a step node",
		},
		{
			name:    "WithStepTimeout on a loop",
			spec:    Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", true), WithStepTimeout(time.Minute)),
			wantErr: "cannot use WithStepTimeout on a loop node",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("looped", WithSteps(tt.spec))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoopDefaultsItsIterationBound(t *testing.T) {
	wf, err := New("looped", WithSteps(
		Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", true)),
	))
	require.NoError(t, err)

	// The bound is resolved when the declaration is flattened, so the fingerprint carries the number the engine enforces
	assert.Equal(t, defaultMaxIterations, wf.def.byName["poll"].maxIterations)
}

func TestLoopBodyChangesTheFingerprint(t *testing.T) {
	base, err := New("looped", WithSteps(
		Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", true), WithMaxIterations(4)),
	))
	require.NoError(t, err)

	tests := []struct {
		name string
		spec StepSpec
	}{
		{name: "another bound", spec: Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", true), WithMaxIterations(5))},
		{name: "another condition value", spec: Loop("poll", Step("check", WithRun(noopRun))).With(WithUntil("check", false), WithMaxIterations(4))},
		{name: "another body", spec: Loop("poll", Step("check", WithRun(noopRun)), Step("pause", WithRun(noopRun))).With(WithUntil("check", true), WithMaxIterations(4))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Everything the engine reads while running an instance is in the fingerprint, so two hosts cannot serve one version and then repeat a body differently
			other, err := New("looped", WithSteps(tt.spec))
			require.NoError(t, err)
			assert.NotEqual(t, base.def.fingerprint, other.def.fingerprint)
		})
	}
}
