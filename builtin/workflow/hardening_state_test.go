package workflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHardeningOptionalLateSuccessKeepsCommittedEffects(t *testing.T) {
	for _, kind := range []string{"plain", "parallel", "child"} {
		for _, afterCompletion := range []bool{false, true} {
			t.Run(kind+"/"+map[bool]string{false: "before-completion", true: "after-completion"}[afterCompletion], func(t *testing.T) {
				// Exercise each source of optional abandonment with the same committed surrounding steps
				now := time.Now()
				optional := Step("optional", WithRun(noopRun), WithCompensate(noopCompensate), WithOptional(), WithStepTimeout(time.Second))
				if kind == "parallel" {
					optional = Parallel("optional",
						Step("slow", WithRun(noopRun), WithCompensate(noopCompensate)),
						Step("failure", WithRun(noopRun)),
					).With(WithOptional())
				}
				if kind == "child" {
					child, err := New("optional-child", WithSteps(Step("effect", WithRun(noopRun))))
					require.NoError(t, err)
					optional = Child("optional", WithDefinition(child), WithOptional(), WithStepTimeout(time.Second))
				}
				def := testDefinition(t, "optional-late", WithSteps(
					Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
					optional,
					Step("last", WithRun(noopRun), WithCompensate(noopCompensate)),
				))
				st := startJournal(t, def, now)
				reportSuccess(t, st, def, "first", 0, "committed-first", now)
				advance(st, def, "inst-1", now)
				if kind == "parallel" {
					reportFailure(t, st, def, "optional", 1, "failed", false, now)
				} else {
					o := &orchestrator{def: def}
					o.applyElapsedDeadlines(st, now.Add(2*time.Second))
				}
				advance(st, def, "inst-1", now.Add(2*time.Second))
				require.True(t, st.step("optional").task(0).Abandoned)

				// Moving the late result across the final completion must not change committed effects
				if !afterCompletion {
					reportSuccess(t, st, def, "optional", 0, "late-effect", now.Add(3*time.Second))
					advance(st, def, "inst-1", now.Add(3*time.Second))
				}
				reportSuccess(t, st, def, "last", 0, "committed-last", now.Add(4*time.Second))
				advance(st, def, "inst-1", now.Add(4*time.Second))
				if afterCompletion {
					reportSuccess(t, st, def, "optional", 0, "late-effect", now.Add(5*time.Second))
					advance(st, def, "inst-1", now.Add(5*time.Second))
				}
				require.Equal(t, StatusCompleted, st.Status)
				assert.Empty(t, st.TerminalStatus)
				assert.Equal(t, CompensationNone, st.Compensation)
				assert.ElementsMatch(t, []string{"first", "optional", "last"}, st.Stack)
				for _, name := range []string{"first", "optional", "last"} {
					assert.Nil(t, st.step(name).task(0).Comp, "committed effect %s must not be undone", name)
				}

				// The retained late effect remains available when a parent explicitly requests rollback
				apply(st, def, &event{kind: evUnwind, fromParent: true, compAttempt: 1}, now.Add(6*time.Second))
				advance(st, def, "inst-1", now.Add(6*time.Second))
				require.Equal(t, StatusCompensating, st.Status)
				assert.Contains(t, st.Stack, "optional")
			})
		}
	}
}

func TestHardeningLateCancellationRecomputesOutcomeAndBudget(t *testing.T) {
	// A late effect may arrive after both the original cancellation and its forward deadline
	now := time.Now()
	def := testDefinition(t, "late-cancel-budget", WithTimeout(time.Minute), WithSteps(
		Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
	))
	st := startJournal(t, def, now)
	apply(st, def, &event{kind: evCancel}, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, CompensationNone, st.Compensation)
	late := now.Add(2 * time.Minute)
	reportSuccess(t, st, def, "effect", 0, "late-effect", late)
	advance(st, def, "inst-1", late)
	require.Equal(t, StatusCompensating, st.Status)
	assert.Equal(t, late.Add(time.Minute), st.DeadlineAt)

	// The new rollback must report the compensation that actually ran
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "effect", Attempt: 1}}, late)
	advance(st, def, "inst-1", late)
	assert.Equal(t, StatusCancelled, st.Status)
	assert.Equal(t, CompensationCompleted, st.Compensation)
}

func TestHardeningLateEffectFencesEarlierUndo(t *testing.T) {
	for _, undoCompleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "undo-still-running", true: "undo-already-completed"}[undoCompleted], func(t *testing.T) {
			// Cancellation may run a defensive undo before an abandoned handler finishes producing its effect
			now := time.Now()
			def := testDefinition(t, "late-effect-generation", WithSteps(
				Step("effect", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateOnFailure(), WithCompensateMaxAttempts(2)),
			))
			st := startJournal(t, def, now)
			apply(st, def, &event{kind: evCancel}, now)
			advance(st, def, "inst-1", now)
			oldUndo := &event{kind: evCompensated, comp: &compReportPayload{Step: "effect", Attempt: 1}}
			if undoCompleted {
				apply(st, def, oldUndo, now)
				advance(st, def, "inst-1", now)
				require.Equal(t, StatusCancelled, st.Status)
			}

			// The forward outcome requires a fresh undo payload and rejects acknowledgements for the earlier payload
			reportSuccess(t, st, def, "effect", 0, "late-effect", now.Add(time.Second))
			advance(st, def, "inst-1", now.Add(time.Second))
			tr := st.step("effect").task(0)
			require.NotNil(t, tr.Comp)
			require.Equal(t, 2, tr.Comp.Attempts)
			assert.Equal(t, 2, tr.Comp.GenerationStart)
			assert.False(t, tr.Compensated)
			assert.True(t, apply(st, def, oldUndo, now.Add(time.Second)))
			advance(st, def, "inst-1", now.Add(time.Second))
			require.Equal(t, StatusCompensating, st.Status)
			assert.False(t, tr.Comp.Done)

			// A new generation receives its own retry budget while retaining globally distinct attempt keys
			apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "effect", Attempt: 2, Error: "retry", Retryable: true}}, now.Add(time.Second))
			require.Equal(t, 3, tr.Comp.Attempts)
			assert.Equal(t, now.Add(time.Second+defaultCompInitial), tr.Comp.RetryAt)
			apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "effect", Attempt: 3}}, now.Add(2*time.Second))
			advance(st, def, "inst-1", now.Add(2*time.Second))
			assert.Equal(t, StatusCancelled, st.Status)
			assert.Equal(t, CompensationCompleted, st.Compensation)
			assert.True(t, tr.Compensated)
		})
	}
}

func TestHardeningParentUnwindsForwardFailedChild(t *testing.T) {
	for _, reported := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-result-delivery", true: "after-result-delivery"}[reported], func(t *testing.T) {
			// A child can fail after continuing its forward path without ever opening rollback
			now := time.Now()
			def := testDefinition(t, "forward-failed-child", WithSteps(
				Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
				Step("failure", WithRun(noopRun), WithSkipOnFailure("skipped")),
				Step("skipped", WithRun(noopRun)),
			))
			st := startJournal(t, def, now)
			st.Parent = childOf("parent")
			reportSuccess(t, st, def, "effect", 0, "effect", now)
			advance(st, def, "inst-1", now)
			reportFailure(t, st, def, "failure", 0, "failed", false, now)
			advance(st, def, "inst-1", now)
			st.Reported = reported
			require.Equal(t, StatusFailed, st.Status)
			require.Equal(t, CompensationNone, st.Compensation)
			require.Empty(t, st.TerminalStatus)

			// Parent rollback must undo the retained effect regardless of when the forward failure was delivered
			unwind := &event{kind: evUnwind, fromParent: true, compAttempt: 1}
			apply(st, def, unwind, now)
			advance(st, def, "inst-1", now)
			require.Equal(t, StatusCompensating, st.Status)
			require.NotNil(t, st.step("effect").task(0).Comp)
			apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "effect", Attempt: 1}}, now)
			advance(st, def, "inst-1", now)
			require.Equal(t, CompensationCompleted, st.Compensation)
			apply(st, def, unwind, now)
			advance(st, def, "inst-1", now)
			assert.True(t, st.Status.IsTerminal())
			assert.True(t, st.step("effect").task(0).Comp.Done)
			assert.Equal(t, 1, st.step("effect").task(0).Comp.Attempts)
		})
	}
}

func TestHardeningJoiningChildUnwindKeepsLatestAttempt(t *testing.T) {
	// A parent's newer causal undo must be acknowledged even if a prior child rollback is still running
	now := time.Now()
	def := testDefinition(t, "joining-child-unwind", WithSteps(
		Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("failure", WithRun(noopRun)),
	))
	st := startJournal(t, def, now)
	st.Parent = childOf("parent")
	reportSuccess(t, st, def, "effect", 0, "effect", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "failure", 0, "failed", false, now)
	advance(st, def, "inst-1", now)
	assert.False(t, apply(st, def, &event{kind: evUnwind, fromParent: true, compAttempt: 2}, now))
	assert.Equal(t, 2, st.Parent.UnwoundBy)
	assert.True(t, apply(st, def, &event{kind: evUnwind, fromParent: true, compAttempt: 1}, now))
	assert.Equal(t, 2, st.Parent.UnwoundBy)
}

func TestHardeningCompensationIgnoresForwardStepDeadline(t *testing.T) {
	for _, kind := range []string{"plain", "child"} {
		t.Run(kind, func(t *testing.T) {
			// Both a defensive undo and a child rollback can begin when the forward step times out
			now := time.Now()
			step := Step("effect", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateOnFailure(), WithStepTimeout(time.Second))
			if kind == "child" {
				child, err := New("deadline-child", WithSteps(Step("work", WithRun(noopRun))))
				require.NoError(t, err)
				step = Child("effect", WithDefinition(child), WithStepTimeout(time.Second))
			}
			def := testDefinition(t, "compensation-deadline", WithTimeout(time.Hour), WithSteps(step))
			st := startJournal(t, def, now)
			o := &orchestrator{def: def}

			// Repeated recovery turns must retain the future instance deadline instead of hot-looping on the expired step
			for second := 2; second < 5; second++ {
				turnTime := now.Add(time.Duration(second) * time.Second)
				o.applyElapsedDeadlines(st, turnTime)
				advance(st, def, "inst-1", turnTime)
				require.Equal(t, StatusCompensating, st.Status)
				assert.Equal(t, now.Add(time.Hour), st.DeadlineAt)
				assert.True(t, st.DeadlineAt.After(turnTime))
			}
		})
	}
}

func TestHardeningChildRejectsOversizedStartDurably(t *testing.T) {
	// A parent bypasses the child's public Start method, so the receiving actor must validate the encoded input
	host := newFakeHost()
	wf, err := New("small-input-child", WithMaxInputSize(4), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "child")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{
		Version: 1,
		Input:   json.RawMessage(`"oversized"`),
		Parent:  childOf("parent"),
	}})
	require.NoError(t, err)

	// Failure must be reportable without retaining oversized data or dispatching a worker
	st := readJournal(t, host, wf, "child")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Contains(t, st.Cause, ErrInputTooLarge.Error())
	assert.Empty(t, st.Input)
	assert.Equal(t, StepSkipped, st.step("work").Status)
	assert.Empty(t, host.dispatchedTo(builtinActorType(wf.workerType("")), workerActorID("child", "work", 0)))
	assert.Equal(t, []string{methodDone}, reportsToParent(t, host, "parent"))
}
