package workflow

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

func TestHardeningNewRejectsUninitializedDeclarations(t *testing.T) {
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

func TestHardeningNewRejectsUnknownPolicies(t *testing.T) {
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

func TestHardeningNewRejectsIgnoredParallelMemberOptions(t *testing.T) {
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

func TestHardeningNewRejectsOptionsOnIncompatibleKinds(t *testing.T) {
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

func TestHardeningSupportedParallelAndChildOptionsRemainValid(t *testing.T) {
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

func TestHardeningChildAdmissionEnforcesItsInputLimit(t *testing.T) {
	// A child start bypasses the public service, so admission must still fail oversized input durably and report it to the parent
	wf, err := New("input-limited-child", WithMaxInputSize(128), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, wf, host, "child-1")
	encoded, err := json.Marshal(strings.Repeat("x", 1024))
	require.NoError(t, err)
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{
		Version: 1,
		Input:   encoded,
		Parent:  &parentRef{Workflow: "parent", InstanceID: "parent-1", Step: "child", Attempt: 1},
	}})
	require.NoError(t, err)
	st := readJournal(t, host, wf, "child-1")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Contains(t, st.Cause, ErrInputTooLarge.Error())
	assert.Empty(t, host.dispatchedTo(builtinActorType(wf.workerType("")), workerActorID("child-1", "work", 0)))
	report := reportedRun(t, host)
	assert.Equal(t, StatusFailed, report.ChildStatus)
	assert.Contains(t, report.Error, ErrInputTooLarge.Error())
}

func TestHardeningRaiseEventUsesTheTargetVersionsMetadata(t *testing.T) {
	// A newer service must accept the event name and payload limit of the older instance it is driving
	old, err := New("event-upgrade", WithVersion(1), WithMaxOutputSize(128), WithSteps(WaitForEvent("wait", WithEventName("approval-v1"))))
	require.NoError(t, err)
	current, err := New("event-upgrade", WithVersion(2), WithMaxOutputSize(16), WithSteps(WaitForEvent("wait", WithEventName("approval-v2"))))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, old, host, "old-instance")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	svc := current.Service(actor.NewService(host))
	err = svc.RaiseEvent(t.Context(), "old-instance", "approval-v1", strings.Repeat("x", 32))
	require.NoError(t, err)
	require.ErrorIs(t, svc.RaiseEvent(t.Context(), "old-instance", "approval-v2", nil), ErrNoSuchEvent)
	require.ErrorIs(t, svc.RaiseEvent(t.Context(), "old-instance", "approval-v1", strings.Repeat("x", 256)), ErrInputTooLarge)

	// Deliver the accepted job to the compatible orchestrator and verify the old wait completes
	jobID := host.jobIDFor(builtinActorType(old.baseType), "old-instance", methodEvent)
	require.NotEmpty(t, jobID)
	err = o.Job(t.Context(), methodEvent, &payloadEnvelope{value: host.jobPayloads[jobID]})
	require.NoError(t, err)
	st := readJournal(t, host, old, "old-instance")
	assert.Equal(t, StatusCompleted, st.Status)
}

func TestHardeningLegacyEventValidationRequiresTheMatchingVersion(t *testing.T) {
	// Legacy journals can use the caller's graph only when it has the same definition version
	wf, err := New("legacy-event", WithVersion(2), WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	svc := wf.Service(nil)
	limit, err := svc.eventLimit(&instanceState{Version: 2}, "approval")
	require.NoError(t, err)
	assert.Equal(t, wf.def.maxOutputSize, limit)
	_, err = svc.eventLimit(&instanceState{Version: 1}, "approval")
	require.ErrorContains(t, err, "use a service with that definition version")
	assert.NotErrorIs(t, err, ErrNoSuchEvent, "an unavailable legacy contract does not establish that the event is invalid")

	// A recorded empty contract must not acquire an event merely because the newer host declares one
	_, err = svc.eventLimit(&instanceState{Version: 1, MaxEventSize: 128}, "approval")
	require.ErrorIs(t, err, ErrNoSuchEvent)
}
