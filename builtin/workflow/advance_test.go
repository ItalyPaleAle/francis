package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopRun is the handler a test step needs only so the definition validates, since advance never runs one
func noopRun(ctx context.Context, t Task) (any, error) {
	return nil, nil
}

// noopCompensate is the compensation a test step needs only so the step is compensable
func noopCompensate(ctx context.Context, c Compensation) error {
	return nil
}

// testDefinition builds a validated definition from step specs, so a test declares its graph the way an application does
func testDefinition(t *testing.T, name string, opts ...Option) *definition {
	t.Helper()

	wf, err := New(name, opts...)
	require.NoError(t, err)
	return wf.def
}

// startJournal folds a start event into an empty journal, which is where every advance test begins
func startJournal(t *testing.T, def *definition, now time.Time) *instanceState {
	t.Helper()

	st := &instanceState{}
	dup := apply(st, def, &event{kind: evStart, start: &startPayload{Version: def.version, CreatedAt: now}}, now)
	require.False(t, dup)

	advance(st, def, "inst-1", now)
	return st
}

// reportSuccess folds a successful report for one task
func reportSuccess(t *testing.T, st *instanceState, def *definition, step string, index int, output any, now time.Time) bool {
	t.Helper()

	sr := st.step(step)
	require.NotNil(t, sr, "journal has no step %q", step)
	tr := sr.task(index)
	require.NotNil(t, tr, "step %q has no task %d", step, index)

	enc, err := json.Marshal(output)
	require.NoError(t, err)

	return apply(st, def, &event{kind: evDone, report: &reportPayload{
		Step:    step,
		Index:   index,
		Attempt: tr.Attempts,
		Output:  enc,
	}}, now)
}

// reportFailure folds a failed report for one task, which the step's own policy then decides what to make of
func reportFailure(t *testing.T, st *instanceState, def *definition, step string, index int, msg string, retryable bool, now time.Time) bool {
	t.Helper()

	sr := st.step(step)
	require.NotNil(t, sr)
	tr := sr.task(index)
	require.NotNil(t, tr)

	return apply(st, def, &event{kind: evDone, report: &reportPayload{
		Step:      step,
		Index:     index,
		Attempt:   tr.Attempts,
		Error:     msg,
		Retryable: retryable,
	}}, now)
}

// stepStatus returns one step's status, so assertions read by name rather than by position
func stepStatus(t *testing.T, st *instanceState, name string) StepStatus {
	t.Helper()

	sr := st.step(name)
	require.NotNil(t, sr, "journal has no step %q", name)
	return sr.Status
}

func TestAdvanceOpensStepsInOrder(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "seq", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
		Step("c", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)

	// Only the first step is opened: the one in flight has to settle before anything after it does
	assert.Equal(t, StatusRunning, st.Status)
	assert.Equal(t, StepRunning, stepStatus(t, st, "a"))
	assert.Equal(t, StepPending, stepStatus(t, st, "b"))
	assert.Equal(t, "a", st.Cursor)

	reportSuccess(t, st, def, "a", 0, "done-a", now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepCompleted, stepStatus(t, st, "a"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "b"))

	reportSuccess(t, st, def, "b", 0, "done-b", now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "c"))

	reportSuccess(t, st, def, "c", 0, "done-c", now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusCompleted, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
	assert.JSONEq(t, `"done-c"`, string(st.Output))
	assert.Empty(t, st.Cursor)
}

func TestAdvanceRecordsEveryStepAtStart(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "recorded", WithSteps(
		Step("a", WithRun(noopRun)),
		Parallel("group", Step("m1", WithRun(noopRun)), Step("m2", WithRun(noopRun))),
		WaitForEvent("ev"),
	))

	st := startJournal(t, def, now)

	// The full step list is why status and the unknown-version path can be answered from the journal alone
	require.Len(t, st.Steps, 3)
	assert.Equal(t, []string{"a", "group", "ev"}, []string{st.Steps[0].Name, st.Steps[1].Name, st.Steps[2].Name})
	assert.Equal(t, KindParallel, st.Steps[1].Kind)
	assert.Equal(t, StepPending, st.Steps[1].Status)
}

func TestAdvanceRetriesARetryableFailure(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "retry", WithSteps(
		Step("flaky", WithRun(noopRun), WithMaxAttempts(3), WithRetryBackoff(2*time.Second, time.Minute)),
	))

	st := startJournal(t, def, now)
	tr := st.step("flaky").task(0)
	require.Equal(t, 1, tr.Attempts)

	// A retryable failure numbers the next attempt before the job that runs it exists, and dates it from the step's own backoff
	reportFailure(t, st, def, "flaky", 0, "boom", true, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, 2, tr.Attempts)
	assert.False(t, tr.Done)
	assert.Equal(t, now.Add(2*time.Second), tr.RetryAt)
	assert.Equal(t, StepRunning, stepStatus(t, st, "flaky"))

	reportFailure(t, st, def, "flaky", 0, "boom", true, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, 3, tr.Attempts)
	assert.Equal(t, now.Add(4*time.Second), tr.RetryAt)

	// The third failure exhausts the policy, so the task is failed rather than retried again
	reportFailure(t, st, def, "flaky", 0, "boom", true, now)
	advance(st, def, "inst-1", now)
	assert.True(t, tr.Done)
	assert.Equal(t, "boom", tr.Error)

	// Nothing was on the compensation stack, so the instance passes through compensating in the same turn
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
}

func TestAdvanceFailsPermanentlyWithoutRetrying(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "permanent", WithSteps(
		Step("hard", WithRun(noopRun), WithMaxAttempts(10)),
	))

	st := startJournal(t, def, now)

	reportFailure(t, st, def, "hard", 0, "declined", false, now)
	advance(st, def, "inst-1", now)

	tr := st.step("hard").task(0)
	assert.Equal(t, 1, tr.Attempts)
	assert.True(t, tr.Done)
	assert.Equal(t, StepFailed, stepStatus(t, st, "hard"))
}

func TestAdvanceIgnoresADuplicateReport(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "duplicate", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)

	dup := reportSuccess(t, st, def, "a", 0, "first", now)
	assert.False(t, dup)
	advance(st, def, "inst-1", now)

	// The guard is whether the journal already has this outcome, never whether this delivery has been seen before
	dup = apply(st, def, &event{kind: evDone, report: &reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"second"`)}}, now)
	assert.True(t, dup)
	assert.JSONEq(t, `"first"`, string(st.step("a").task(0).Output))

	// A duplicate still leaves the journal saying the next step should be running, so the instance does not stall
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "b"))
}

func TestAdvanceIgnoresALateFailureButAcceptsALateSuccess(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "late", WithSteps(
		Step("a", WithRun(noopRun), WithMaxAttempts(3)),
	))

	st := startJournal(t, def, now)
	reportFailure(t, st, def, "a", 0, "first attempt failed", true, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, 2, st.step("a").task(0).Attempts)

	// A late failure from attempt 1 cannot be mistaken for attempt 2's, because a newer attempt is already in flight
	dup := apply(st, def, &event{kind: evDone, report: &reportPayload{Step: "a", Index: 0, Attempt: 1, Error: "stale", Retryable: true}}, now)
	assert.True(t, dup)
	assert.Equal(t, 2, st.step("a").task(0).Attempts)

	// A late success is accepted whatever its attempt, because the work really was done
	dup = apply(st, def, &event{kind: evDone, report: &reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"ok"`)}}, now)
	assert.False(t, dup)
	assert.True(t, st.step("a").task(0).Done)
}

func TestAdvanceSkipsDependentsOfAFailedStep(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "skip-on-failure", WithSteps(
		Step("manifest", WithRun(noopRun), WithSkipOnFailure("notify"), WithMaxAttempts(1)),
		Step("notify", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportFailure(t, st, def, "manifest", 0, "store down", false, now)
	advance(st, def, "inst-1", now)

	// The run continues past the failure, but there is nothing to notify about, and there is nothing to undo either
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
	assert.Equal(t, StepSkipped, stepStatus(t, st, "notify"))
}

func TestAdvanceCompletesPastAnOptionalFailure(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "optional", WithSteps(
		Step("work", WithRun(noopRun)),
		Step("notify", WithRun(noopRun), WithOptional(), WithMaxAttempts(1)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "work", 0, "done", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "notify", 0, "callback 503", false, now)
	advance(st, def, "inst-1", now)

	// The work the caller asked for was done and only a notification was lost, so the run counts
	assert.Equal(t, StatusCompleted, st.Status)
	assert.Equal(t, StepFailed, stepStatus(t, st, "notify"))
}

func TestAdvanceUnwindsInReverseOrder(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "unwind-order", WithSteps(
		Step("reserve", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("charge", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("ship", WithRun(noopRun), WithMaxAttempts(1)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "reserve", 0, "res", now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "charge", 0, "ch", now)
	advance(st, def, "inst-1", now)

	// Frames are pushed in completion order, and the failing step is not among them because it never completed
	assert.Equal(t, []string{"reserve", "charge"}, st.Stack)

	reportFailure(t, st, def, "ship", 0, "carrier refused", false, now)
	advance(st, def, "inst-1", now)

	// The unwind opens at the top of the stack, which is the step that ran last
	require.Equal(t, StatusCompensating, st.Status)
	assert.Equal(t, StepCompensating, stepStatus(t, st, "charge"))
	assert.Equal(t, StepCompleted, stepStatus(t, st, "reserve"))

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "charge", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepCompensated, stepStatus(t, st, "charge"))
	assert.Equal(t, StepCompensating, stepStatus(t, st, "reserve"))

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "reserve", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationCompleted, st.Compensation)
	assert.Empty(t, st.Stack)
}

func TestAdvanceContinuesUnwindingPastAFailedCompensation(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "partial-unwind", WithSteps(
		Step("reserve", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("charge", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(1)),
		Step("ship", WithRun(noopRun), WithMaxAttempts(1)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "reserve", 0, "res", now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "charge", 0, "ch", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "ship", 0, "boom", false, now)
	advance(st, def, "inst-1", now)

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "charge", Index: 0, Attempt: 1, Error: "provider down", Retryable: true}}, now)
	advance(st, def, "inst-1", now)

	// Stopping at the first problem usually leaves more state stranded than continuing does, so the remaining frames still unwind
	assert.Equal(t, StepCompensationFailed, stepStatus(t, st, "charge"))
	assert.Equal(t, StepCompensating, stepStatus(t, st, "reserve"))

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "reserve", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)

	// A workflow whose rollback did not complete is never reported as cleanly rolled back
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationPartial, st.Compensation)
}

func TestAdvanceAbortsTheUnwindWhenAskedTo(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "abort-unwind",
		WithCompensationFailurePolicy(AbortUnwinding),
		WithSteps(
			Step("reserve", WithRun(noopRun), WithCompensate(noopCompensate)),
			Step("charge", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(1)),
			Step("ship", WithRun(noopRun), WithMaxAttempts(1)),
		),
	)

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "reserve", 0, "res", now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "charge", 0, "ch", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "ship", 0, "boom", false, now)
	advance(st, def, "inst-1", now)

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "charge", Index: 0, Attempt: 1, Error: "provider down", Retryable: true}}, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationFailed, st.Compensation)

	// The journal names exactly which frames were not unwound, because the stack is left as it stands
	assert.Equal(t, []string{"reserve"}, st.Stack)
	assert.Equal(t, StepCompleted, stepStatus(t, st, "reserve"))
}

func TestAdvanceFanOutIsSizedFromTheUpstreamOutput(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "fanout", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithFailurePolicy(TolerateFailures)),
		Step("collect", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "plan", 0, []string{"a", "b", "c"}, now)
	advance(st, def, "inst-1", now)

	work := st.step("work")
	require.Len(t, work.Tasks, 3)
	assert.Equal(t, 3, work.Remaining)
	assert.JSONEq(t, `"b"`, string(work.Tasks[1].Item))

	reportSuccess(t, st, def, "work", 0, 1, now)
	reportFailure(t, st, def, "work", 1, "unlucky", false, now)
	reportSuccess(t, st, def, "work", 2, 3, now)
	advance(st, def, "inst-1", now)

	// Tolerating a failure leaves it visible in the step's output, and it is the next step's business what to do about it
	assert.Equal(t, StepCompleted, stepStatus(t, st, "work"))
	assert.JSONEq(t, `[1,{"error":"unlucky"},3]`, string(def.byName["work"].stepOutput(st.step("work"))))
}

func TestAdvanceFanOutFailFastDecidesOnTheFirstFailure(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "failfast", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithMaxAttempts(1)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "plan", 0, []int{1, 2, 3}, now)
	advance(st, def, "inst-1", now)

	// The step is decided the moment one task has failed for good, without waiting for the stragglers
	reportFailure(t, st, def, "work", 1, "boom", false, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StepFailed, stepStatus(t, st, "work"))
	assert.Equal(t, StatusFailed, st.Status)

	// The stragglers were abandoned rather than failed, so a success one of them reports late is still recorded
	assert.True(t, st.step("work").task(0).Abandoned)
}

func TestAdvanceCollectFailuresWaitsForEveryTask(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "collect", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithMaxAttempts(1), WithFailurePolicy(CollectFailures)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "plan", 0, []int{1, 2}, now)
	advance(st, def, "inst-1", now)

	reportFailure(t, st, def, "work", 0, "boom", false, now)
	advance(st, def, "inst-1", now)

	// Partial progress is worth having before unwinding, so the step stays open until every task has reported
	assert.Equal(t, StepRunning, stepStatus(t, st, "work"))

	reportSuccess(t, st, def, "work", 1, 2, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepFailed, stepStatus(t, st, "work"))
}

func TestAdvanceEmptyFanOutCompletesImmediately(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "empty-fanout", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun)),
		Step("after", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "plan", 0, []int{}, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StepCompleted, stepStatus(t, st, "work"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "after"))
}

func TestAdvanceSkipsAStepWhoseConditionSaysSo(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "conditional", WithSteps(
		Step("approved", WithRun(noopRun)),
		Step("verify", WithRun(noopRun), WithSkipIf("approved", false)),
		Step("finish", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "approved", 0, false, now)
	advance(st, def, "inst-1", now)

	// A condition is a recorded output rather than a predicate the orchestrator evaluates
	assert.Equal(t, StepSkipped, stepStatus(t, st, "verify"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "finish"))
}

func TestAdvanceWaitStepCompletesOnItsEvent(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "waiting", WithSteps(
		WaitForEvent("approval", WithEventTimeout(time.Hour)),
		Step("after", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "approval"))
	assert.Empty(t, st.step("approval").Tasks)

	// An event nothing is waiting for records nothing, and only the open wait accepts its own
	dup := apply(st, def, &event{kind: evRaise, raise: &eventPayload{Name: "other"}}, now)
	assert.True(t, dup)

	dup = apply(st, def, &event{kind: evRaise, raise: &eventPayload{Name: "approval", Payload: json.RawMessage(`{"by":"ops"}`)}}, now)
	assert.False(t, dup)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StepCompleted, stepStatus(t, st, "approval"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "after"))
	assert.JSONEq(t, `{"by":"ops"}`, string(def.byName["approval"].stepOutput(st.step("approval"))))
}

func TestAdvanceSuspendPausesTheDeadlinesAndStartsNothing(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "suspendable",
		WithTimeout(time.Hour),
		WithSteps(
			Step("a", WithRun(noopRun), WithStepTimeout(10*time.Minute)),
			Step("b", WithRun(noopRun)),
		),
	)

	st := startJournal(t, def, now)
	require.Equal(t, now.Add(10*time.Minute), st.DeadlineAt)

	// Half the instance timeout and half the step's own budget have gone by the time it is paused
	paused := now.Add(30 * time.Minute)
	apply(st, def, &event{kind: evSuspend, reason: "maintenance"}, paused)
	advance(st, def, "inst-1", paused)

	require.NotNil(t, st.Suspended)
	assert.Equal(t, StatusSuspended, st.Status)
	assert.Equal(t, StatusRunning, st.Suspended.ResumeTo)
	assert.Equal(t, 30*time.Minute, st.Suspended.RemainingTimeout)
	assert.True(t, st.DeadlineAt.IsZero())

	// Work already in flight is still recorded while paused, but the next step is not opened
	reportSuccess(t, st, def, "a", 0, "one", paused)
	advance(st, def, "inst-1", paused)
	assert.Equal(t, StepCompleted, stepStatus(t, st, "a"))
	assert.Equal(t, StepPending, stepStatus(t, st, "b"))
	assert.Equal(t, StatusSuspended, st.Status)

	// A two-day pause does not eat a one-hour timeout: the remainder is what comes back
	resumed := paused.Add(48 * time.Hour)
	apply(st, def, &event{kind: evResume}, resumed)
	advance(st, def, "inst-1", resumed)

	assert.Equal(t, StatusRunning, st.Status)
	assert.Nil(t, st.Suspended)
	assert.Equal(t, StepRunning, stepStatus(t, st, "b"))
	assert.WithinDuration(t, resumed.Add(30*time.Minute), instanceDeadline(st, def), time.Second)
}

func TestAdvanceCancelUnwindsAndTerminatesAsCancelled(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "cancellable", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "one", now)
	advance(st, def, "inst-1", now)

	apply(st, def, &event{kind: evCancel, reason: "customer cancelled"}, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusCompensating, st.Status)
	assert.Equal(t, "customer cancelled", st.Cause)

	// The step that was in flight is closed out so the unwind does not wait on attempts nobody will re-drive
	assert.Equal(t, StepFailed, stepStatus(t, st, "b"))
	assert.True(t, st.step("b").task(0).Abandoned)

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "a", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusCancelled, st.Status)
	assert.Equal(t, CompensationCompleted, st.Compensation)
}

func TestAdvanceIgnoresEverythingOnceTerminal(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "terminal", WithSteps(Step("a", WithRun(noopRun))))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "done", now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StatusCompleted, st.Status)

	// A terminated instance ignores everything, so a late report or a repeated cancel cannot revive it
	dup := apply(st, def, &event{kind: evCancel, reason: "too late"}, now)
	assert.True(t, dup)
	assert.Equal(t, StatusCompleted, st.Status)
}

func TestAdvanceIsIdempotent(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "idempotent", WithSteps(
		Step("a", WithRun(noopRun)),
		Parallel("g", Step("m1", WithRun(noopRun)), Step("m2", WithRun(noopRun))),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "one", now)

	// Running advance any number of times over the same journal converges on the same answer, so recovery needs no special code path
	advance(st, def, "inst-1", now)
	first, err := json.Marshal(st)
	require.NoError(t, err)

	for range 5 {
		advance(st, def, "inst-1", now)
	}
	again, err := json.Marshal(st)
	require.NoError(t, err)

	assert.JSONEq(t, string(first), string(again))
}

func TestAdvanceParallelGroupOutputIsKeyedByMemberName(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "group-output", WithSteps(
		Parallel("notify",
			Step("email", WithRun(noopRun)),
			Step("sms", WithRun(noopRun)),
		),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "notify", 0, "email-ok", now)
	reportSuccess(t, st, def, "notify", 1, "sms-ok", now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusCompleted, st.Status)
	assert.JSONEq(t, `{"email":"email-ok","sms":"sms-ok"}`, string(def.byName["notify"].stepOutput(st.step("notify"))))
}

func TestNewRejectsAnInvalidGraph(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "no steps",
			opts:    nil,
			wantErr: "WithSteps is required",
		},
		{
			name:    "duplicate step name",
			opts:    []Option{WithSteps(Step("a", WithRun(noopRun)), Step("a", WithRun(noopRun)))},
			wantErr: "declared more than once",
		},
		{
			name: "two steps listening for the same event",
			opts: []Option{WithSteps(
				WaitForEvent("first", WithEventName("go")),
				WaitForEvent("second", WithEventName("go")),
			)},
			wantErr: "both listen for event",
		},
		{
			name:    "step with no handler",
			opts:    []Option{WithSteps(Step("a"))},
			wantErr: "requires WithRun",
		},
		{
			name: "input from a step that runs later",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun), WithInputFrom("b")),
				Step("b", WithRun(noopRun)),
			)},
			wantErr: "does not run before it",
		},
		{
			name: "skip on failure naming a step that runs earlier",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				Step("b", WithRun(noopRun), WithSkipOnFailure("a")),
			)},
			wantErr: "does not run after it",
		},
		{
			name: "fan-out with no items",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				ForEach("b", WithRun(noopRun)),
			)},
			wantErr: "requires WithItemsFrom",
		},
		{
			name: "fan-out with both a handler and a child",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				ForEach("b", WithItemsFrom("a"), WithRun(noopRun), WithChild(mustWorkflow(t, "kid"))),
			)},
			wantErr: "mutually exclusive",
		},
		{
			name:    "output naming a step that does not exist",
			opts:    []Option{WithOutput("nope"), WithSteps(Step("a", WithRun(noopRun)))},
			wantErr: "WithOutput names step",
		},
		{
			name: "group containing a fan-out",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				Parallel("g", Step("m", WithRun(noopRun)), ForEach("f", WithItemsFrom("a"), WithRun(noopRun))),
			)},
			wantErr: "may only contain plain or child steps",
		},
		{
			name:    "wait step with a handler",
			opts:    []Option{WithSteps(WaitForEvent("w", WithRun(noopRun)))},
			wantErr: "cannot have a handler",
		},
		{
			name:    "step with no name",
			opts:    []Option{WithSteps(Step("", WithRun(noopRun)))},
			wantErr: "step name is required",
		},
		{
			name:    "step name carrying the delimiter that joins a worker's actor ID",
			opts:    []Option{WithSteps(Step("a|b", WithRun(noopRun)))},
			wantErr: `must not contain "|"`,
		},
		{
			name:    "step name carrying a path separator",
			opts:    []Option{WithSteps(Step("a/b", WithRun(noopRun)))},
			wantErr: "invalid step name",
		},
		{
			name: "group member reusing a top-level step's name",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				Parallel("g", Step("a", WithRun(noopRun))),
			)},
			wantErr: "declared more than once",
		},
		{
			name:    "group with no members",
			opts:    []Option{WithSteps(Parallel("g"))},
			wantErr: "requires at least one member",
		},
		{
			name:    "group member with no handler",
			opts:    []Option{WithSteps(Parallel("g", Step("m")))},
			wantErr: "requires WithRun",
		},
		{
			name:    "child step with no definition",
			opts:    []Option{WithSteps(Child("c"))},
			wantErr: "requires WithDefinition",
		},
		{
			name:    "input from a step that does not exist",
			opts:    []Option{WithSteps(Step("a", WithRun(noopRun), WithInputFrom("nope")))},
			wantErr: "is not a top-level step of this workflow",
		},
		{
			name:    "skip on failure naming a step that does not exist",
			opts:    []Option{WithSteps(Step("a", WithRun(noopRun), WithSkipOnFailure("nope")))},
			wantErr: "is not a top-level step of this workflow",
		},
		{
			name: "condition decided by a step that runs later",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun), WithSkipIf("b", true)),
				Step("b", WithRun(noopRun)),
			)},
			wantErr: "does not run before it",
		},
		{
			name:    "fan-out iterating its own output",
			opts:    []Option{WithSteps(ForEach("f", WithItemsFrom("f"), WithRun(noopRun)))},
			wantErr: "does not run before it",
		},
		{
			name: "fan-out with neither a handler nor a child",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun)),
				ForEach("f", WithItemsFrom("a")),
			)},
			wantErr: "requires either WithRun or WithChild",
		},
		{
			name:    "step requiring a capability that is not a valid type component",
			opts:    []Option{WithSteps(Step("a", WithRun(noopRun), WithRequiredCapability("gpu/large")))},
			wantErr: "invalid required capability",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("invalid", tt.opts...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// mustWorkflow builds a trivial workflow for tests that only need a valid child definition
func mustWorkflow(t *testing.T, name string) *Workflow {
	t.Helper()

	wf, err := New(name, WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	return wf
}

func TestFingerprintChangesWithTheGraphButNotWithAHandler(t *testing.T) {
	base := func(run RunFunc) *definition {
		return testDefinition(t, "fp", WithSteps(Step("a", WithRun(run)), Step("b", WithRun(noopRun))))
	}

	// A change to a handler's body alone needs no new version, which is the direct consequence of not replaying code
	other := func(ctx context.Context, tk Task) (any, error) { return "different", nil }
	assert.Equal(t, base(noopRun).fingerprint, base(other).fingerprint)

	// Any change to the graph does, or the registry would let two hosts serve different graphs under one number
	reordered := testDefinition(t, "fp", WithSteps(Step("b", WithRun(noopRun)), Step("a", WithRun(noopRun))))
	assert.NotEqual(t, base(noopRun).fingerprint, reordered.fingerprint)

	policyChanged := testDefinition(t, "fp", WithSteps(Step("a", WithRun(noopRun), WithOptional()), Step("b", WithRun(noopRun))))
	assert.NotEqual(t, base(noopRun).fingerprint, policyChanged.fingerprint)
}

func TestFingerprintCoversEverythingATurnReads(t *testing.T) {
	// Two hosts agreeing on the graph but not on these would apply different transitions to one journal, which the registry exists to stop
	steps := []StepSpec{
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		ForEach("b", WithItemsFrom("a"), WithRun(noopRun)),
	}
	stepsWith := func(opts ...StepOption) []StepSpec {
		return []StepSpec{
			Step("a", append([]StepOption{WithRun(noopRun), WithCompensate(noopCompensate)}, opts...)...),
			ForEach("b", WithItemsFrom("a"), WithRun(noopRun)),
		}
	}

	baseline := testDefinition(t, "fp-wide", WithSteps(steps...)).fingerprint

	tests := []struct {
		name string
		opts []Option
	}{
		{name: "the instance timeout", opts: []Option{WithTimeout(time.Minute), WithSteps(steps...)}},
		{name: "the retention policy", opts: []Option{WithRetention(RetentionPolicy{Completed: time.Hour}), WithSteps(steps...)}},
		{name: "the input cap", opts: []Option{WithMaxInputSize(128), WithSteps(steps...)}},
		{name: "the output cap", opts: []Option{WithMaxOutputSize(128), WithSteps(steps...)}},
		{name: "the journal cap", opts: []Option{WithMaxJournalSize(1 << 15), WithSteps(steps...)}},
		{name: "the child depth limit", opts: []Option{WithMaxDepth(2), WithSteps(steps...)}},
		{name: "the unknown-version policy", opts: []Option{WithUnknownVersionPolicy(FailUnknownVersion), WithSteps(steps...)}},
		{name: "the compensation-failure policy", opts: []Option{WithCompensationFailurePolicy(AbortUnwinding), WithSteps(steps...)}},
		{name: "a step's attempt budget", opts: []Option{WithSteps(stepsWith(WithMaxAttempts(7))...)}},
		{name: "a step's retry backoff", opts: []Option{WithSteps(stepsWith(WithRetryBackoff(time.Second, time.Minute))...)}},
		{name: "a step's compensation budget", opts: []Option{WithSteps(stepsWith(WithCompensateMaxAttempts(4))...)}},
		{name: "a step's compensation backoff", opts: []Option{WithSteps(stepsWith(WithCompensateBackoff(time.Second, time.Minute))...)}},
		{name: "a step's timeout", opts: []Option{WithSteps(stepsWith(WithStepTimeout(time.Minute))...)}},
		{name: "compensating a step that failed", opts: []Option{WithSteps(stepsWith(WithCompensateOnFailure())...)}},
		{
			name: "a fan-out's window",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
				ForEach("b", WithItemsFrom("a"), WithRun(noopRun), WithMaxParallel(2)),
			)},
		},
		{
			name: "a wait step's timeout",
			opts: []Option{WithSteps(
				Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
				WaitForEvent("b", WithEventTimeout(time.Minute)),
			)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEqual(t, baseline, testDefinition(t, "fp-wide", tt.opts...).fingerprint)
		})
	}

	// The same definition built twice hashes the same, or no two hosts would ever agree
	assert.Equal(t, baseline, testDefinition(t, "fp-wide", WithSteps(steps...)).fingerprint)
}

func TestBackoffDoublesAndStopsAtTheCap(t *testing.T) {
	assert.Equal(t, 2*time.Second, backoff(2*time.Second, time.Minute, 1))
	assert.Equal(t, 4*time.Second, backoff(2*time.Second, time.Minute, 2))
	assert.Equal(t, 8*time.Second, backoff(2*time.Second, time.Minute, 3))
	assert.Equal(t, time.Minute, backoff(2*time.Second, time.Minute, 20))
}

func TestDecodeOutputReportsASkippedStep(t *testing.T) {
	task := &taskEnvelope{p: &runPayload{
		Outputs: map[string]json.RawMessage{"gone": nil, "here": json.RawMessage(`"value"`)},
		Skipped: []string{"gone"},
	}}

	var out string
	err := task.DecodeOutput("gone", &out)
	require.ErrorIs(t, err, ErrStepSkipped)

	err = task.DecodeOutput("absent", &out)
	require.ErrorIs(t, err, ErrStepNotFound)

	require.NoError(t, task.DecodeOutput("here", &out))
	assert.Equal(t, "value", out)
}

func TestJournalSizeCapFailsTheInstance(t *testing.T) {
	now := time.Now()
	st := &instanceState{Status: StatusRunning, Steps: []stepRecord{{Name: "a", Status: StepPending}}}

	failForOversizedJournal(st, 2048, 1024, now)

	// An instance that can no longer persist can no longer progress, so failing it is the better outcome
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
	assert.Contains(t, st.Cause, ErrJournalTooLarge.Error())
	assert.Equal(t, StepSkipped, st.Steps[0].Status)
}

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{StatusCompleted, StatusFailed, StatusCancelled}
	for _, s := range terminal {
		assert.True(t, s.IsTerminal(), "%q is an end state", s)
	}

	// Everything else can still be driven forward, which is what the guard on every turn asks
	for _, s := range []Status{StatusPending, StatusRunning, StatusSuspended, StatusCompensating, Status("")} {
		assert.False(t, s.IsTerminal(), "%q is not an end state", s)
	}
}

func TestStepKindsAreRecordedInTheJournal(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "kinds", WithSteps(
		Step("plain", WithRun(noopRun)),
		Parallel("group", Step("m", WithRun(noopRun))),
		ForEach("fan", WithItemsFrom("plain"), WithRun(noopRun)),
		Child("kid", WithDefinition(mustWorkflow(t, "the-kid"))),
		WaitForEvent("wait"),
	))

	st := startJournal(t, def, now)

	// A caller reads the kind off the status view, so the journal has to carry it for every step from the start
	kinds := map[string]Kind{}
	for i := range st.Steps {
		kinds[st.Steps[i].Name] = st.Steps[i].Kind
	}
	assert.Equal(t, map[string]Kind{
		"plain": KindStep,
		"group": KindParallel,
		"fan":   KindForEach,
		"kid":   KindChild,
		"wait":  KindWait,
	}, kinds)
}

func TestAdvanceSuspendPausesAnUnwindAtTheCurrentFrame(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "pausable-unwind", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("c", WithRun(noopRun), WithMaxAttempts(1)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "one", now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "b", 0, "two", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "c", 0, "boom", false, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StatusCompensating, st.Status)
	require.Equal(t, StepCompensating, stepStatus(t, st, "b"))

	// Suspending during an unwind pauses it at the current frame, and remembers to go back to compensating
	apply(st, def, &event{kind: evSuspend, reason: "maintenance"}, now)
	advance(st, def, "inst-1", now)
	require.NotNil(t, st.Suspended)
	assert.Equal(t, StatusCompensating, st.Suspended.ResumeTo)

	// The frame in flight still records its outcome, but the next frame is not opened
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "b", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StatusSuspended, st.Status)
	assert.Equal(t, StepCompleted, stepStatus(t, st, "a"), "the next frame stays closed while paused")

	apply(st, def, &event{kind: evResume}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepCompensating, stepStatus(t, st, "a"), "resuming continues the unwind where it stopped")
}

func TestAdvanceAcceptsAnEventWhileSuspended(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "event-while-paused", WithSteps(
		WaitForEvent("approval", WithEventTimeout(time.Hour)),
		Step("after", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	require.Equal(t, StepRunning, stepStatus(t, st, "approval"))

	apply(st, def, &event{kind: evSuspend, reason: "maintenance"}, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StatusSuspended, st.Status)

	// The event is recorded while paused, so nothing is lost by suspending an instance that was waiting on one
	dup := apply(st, def, &event{kind: evRaise, raise: &eventPayload{Name: "approval", Payload: json.RawMessage(`{"by":"ops"}`)}}, now)
	assert.False(t, dup)
	advance(st, def, "inst-1", now)

	// But the step after it is not opened until the instance is resumed
	assert.Equal(t, StatusSuspended, st.Status)
	assert.Equal(t, StepPending, stepStatus(t, st, "after"))

	apply(st, def, &event{kind: evResume}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "after"))
	assert.JSONEq(t, `{"by":"ops"}`, string(def.byName["approval"].stepOutput(st.step("approval"))))
}

func TestAdvanceFailsAFanOutWhoseItemsAreNotAList(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "bad-items", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "plan", 0, "not a list", now)
	advance(st, def, "inst-1", now)

	// The list is the fan-out's own input, so a list that cannot be read is the step failing rather than the instance crashing
	assert.Equal(t, StepFailed, stepStatus(t, st, "work"))
	assert.Empty(t, st.step("work").Tasks)
	assert.Contains(t, st.step("work").Error, "output a JSON array")
	assert.Equal(t, StatusFailed, st.Status)
}

func TestAdvanceRunsAFanOutOverASkippedStepAsAnEmptyOne(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "skipped-items", WithSteps(
		Step("decide", WithRun(noopRun)),
		Step("plan", WithRun(noopRun), WithSkipIf("decide", true)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun)),
		Step("after", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "decide", 0, true, now)
	advance(st, def, "inst-1", now)

	// A fan-out over a skipped step has nothing to iterate, which is an empty fan-out rather than a failure
	require.Equal(t, StepSkipped, stepStatus(t, st, "plan"))
	assert.Equal(t, StepCompleted, stepStatus(t, st, "work"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "after"))
}

func TestAdvanceRunsAConditionalStepWhenTheConditionIsNotABoolean(t *testing.T) {
	tests := []struct {
		name   string
		output any
	}{
		{name: "a value that is not a boolean", output: "maybe"},
		{name: "no output at all", output: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			def := testDefinition(t, "unreadable-condition", WithSteps(
				Step("decide", WithRun(noopRun)),
				Step("verify", WithRun(noopRun), WithSkipIf("decide", true)),
			))

			st := startJournal(t, def, now)
			reportSuccess(t, st, def, "decide", 0, tt.output, now)
			advance(st, def, "inst-1", now)

			// A condition nothing recorded is not a decision to skip, so the step runs rather than being silently dropped
			assert.Equal(t, StepRunning, stepStatus(t, st, "verify"))
		})
	}
}

func TestAdvanceCompensatesOnlyTheGroupMembersThatDeclaredOne(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "selective-group", WithSteps(
		Parallel("notify",
			Step("email", WithRun(noopRun), WithCompensate(noopCompensate)),
			Step("sms", WithRun(noopRun)),
		),
		Step("boom", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "notify", 0, "email-ok", now)
	reportSuccess(t, st, def, "notify", 1, "sms-ok", now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StepCompleted, stepStatus(t, st, "notify"))

	reportFailure(t, st, def, "boom", 0, "induced failure", false, now)
	advance(st, def, "inst-1", now)

	// A group's member is only compensable when that member declares a compensation of its own
	notify := st.step("notify")
	require.Equal(t, StepCompensating, notify.Status)
	assert.NotNil(t, notify.task(0).Comp, "the member with a compensation is on the frame")
	assert.Nil(t, notify.task(1).Comp, "the member with nothing to undo is not")
}

func TestAdvanceIgnoresAResumeOfAnInstanceThatIsNotSuspended(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "spurious-resume", WithSteps(Step("a", WithRun(noopRun))))

	st := startJournal(t, def, now)
	require.Equal(t, StatusRunning, st.Status)

	// A resume nothing paused is a duplicate rather than an error, since Resume coalesces on a constant key and can be delivered twice
	dup := apply(st, def, &event{kind: evResume}, now)
	assert.True(t, dup)
	assert.Equal(t, StatusRunning, st.Status)
}

func TestAdvanceIgnoresASecondSuspendOfASuspendedInstance(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "double-suspend", WithSteps(Step("a", WithRun(noopRun))))

	st := startJournal(t, def, now)

	dup := apply(st, def, &event{kind: evSuspend, reason: "first"}, now)
	require.False(t, dup)
	require.Equal(t, StatusSuspended, st.Status)

	// A second suspend must not overwrite the status the first one recorded to resume into
	dup = apply(st, def, &event{kind: evSuspend, reason: "second"}, now)
	assert.True(t, dup)
	require.NotNil(t, st.Suspended)
	assert.Equal(t, "first", st.Suspended.Reason)
	assert.Equal(t, StatusRunning, st.Suspended.ResumeTo)
}

func TestAdvanceIgnoresAReportItCannotPlace(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "misplaced-report", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)

	tests := []struct {
		name   string
		report reportPayload
	}{
		{name: "a step the journal does not have", report: reportPayload{Step: "nope", Index: 0, Attempt: 1}},
		{name: "a task index the step does not have", report: reportPayload{Step: "a", Index: 7, Attempt: 1}},
		{name: "a step that has not opened yet", report: reportPayload{Step: "b", Index: 0, Attempt: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A report with nowhere to land is dropped rather than failing the turn, since the job would otherwise be retried forever
			dup := apply(st, def, &event{kind: evDone, report: &tt.report}, now)
			assert.True(t, dup)
			assert.Equal(t, StepRunning, stepStatus(t, st, "a"))
			assert.Equal(t, StepPending, stepStatus(t, st, "b"))
		})
	}
}

func TestAdvanceIgnoresACompensationReportForAFrameItIsNotUnwinding(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "misplaced-comp", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
	))

	st := startJournal(t, def, now)

	tests := []struct {
		name   string
		report compReportPayload
	}{
		{name: "a step the journal does not have", report: compReportPayload{Step: "nope", Index: 0, Attempt: 1}},
		{name: "a task index the step does not have", report: compReportPayload{Step: "a", Index: 7, Attempt: 1}},
		{name: "a task that is not being compensated", report: compReportPayload{Step: "a", Index: 0, Attempt: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dup := apply(st, def, &event{kind: evCompensated, comp: &tt.report}, now)
			assert.True(t, dup)
			assert.Equal(t, StatusRunning, st.Status)
		})
	}
}

func TestAdvanceGivesUpOnACompensationThatKeepsFailing(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "comp-budget", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(2)),
		Step("boom", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "done", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "boom", 0, "induced failure", false, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StepCompensating, stepStatus(t, st, "a"))

	// Each retryable failure costs one of the compensation's own, more generous attempts
	for attempt := 1; attempt <= 2; attempt++ {
		comp := st.step("a").task(0).Comp
		require.NotNil(t, comp)
		require.False(t, comp.Done, "the compensation still has attempts left after %d", attempt-1)
		apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "a", Index: 0, Attempt: comp.Attempts, Error: "cannot undo", Retryable: true}}, now)
		advance(st, def, "inst-1", now)
	}

	// A failed rollback leaves the system inconsistent, so the outcome says so rather than reporting a clean unwind
	assert.Equal(t, StepCompensationFailed, stepStatus(t, st, "a"))
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationPartial, st.Compensation, "the default policy carries on past a frame it could not undo, so the unwind is partial rather than stopped")
}

func TestAdvanceRunsToTheEndOfALongChainOfImmediateTransitions(t *testing.T) {
	// A definition is not limited to any particular number of steps, and every one of these settles the moment it opens
	// A fixed iteration bound would stop short partway along, leaving pending steps with no task or event left to open them and an instance that only ends at its timeout
	const steps = 600

	specs := make([]StepSpec, 0, steps+1)
	specs = append(specs, Step("decide", WithRun(noopRun)))
	for i := range steps {
		specs = append(specs, Step(fmt.Sprintf("skipped-%d", i), WithRun(noopRun), WithSkipIf("decide", true)))
	}

	now := time.Now()
	def := testDefinition(t, "long-chain", WithSteps(specs...))
	require.Greater(t, len(def.steps), minAdvanceIterations, "the graph has to be longer than the loop's floor for this to prove anything")

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "decide", 0, true, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StatusCompleted, st.Status)
	for i := range steps {
		assert.Equal(t, StepSkipped, stepStatus(t, st, fmt.Sprintf("skipped-%d", i)), "step %d should have settled", i)
	}
}

func TestTheAdvanceBoundFollowsTheGraph(t *testing.T) {
	small := testDefinition(t, "small", WithSteps(Step("a", WithRun(noopRun))))
	assert.GreaterOrEqual(t, advanceIterations(small), minAdvanceIterations, "a tiny graph still gets the floor")

	large := testDefinition(t, "large", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
		Step("c", WithRun(noopRun)),
	))
	assert.Greater(t, advanceIterations(large), advanceIterations(small), "a longer graph gets more room")
}

func TestOptionalLateSuccessKeepsCommittedEffects(t *testing.T) {
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

func TestLateCancellationRecomputesOutcomeAndBudget(t *testing.T) {
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

func TestLateEffectFencesEarlierUndo(t *testing.T) {
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

func TestCompensationIgnoresForwardStepDeadline(t *testing.T) {
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

func TestSuspendedFailurePreservesSuspension(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "suspend", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun)),
	))
	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "effect", now)
	advance(st, def, "inst-1", now)
	apply(st, def, &event{kind: evSuspend}, now)
	advance(st, def, "inst-1", now)

	// A report arriving during a pause must preserve the pause and leave a resumable unwind
	reportFailure(t, st, def, "b", 0, "failed", false, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StatusSuspended, st.Status)
	t.Logf("status=%s deadline=%v stack=%v a.status=%s a.comp=%v", st.Status, st.DeadlineAt, st.Stack, st.step("a").Status, st.step("a").Tasks[0].Comp)

	// Resuming opens the unwind that the suspended failure recorded
	apply(st, def, &event{kind: evResume}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StatusCompensating, st.Status)
	assert.NotNil(t, st.step("a").Tasks[0].Comp)
}

func TestLateSuccessJoinsOpenCompensationFrame(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "late-frame", WithSteps(Parallel("group",
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("c", WithRun(noopRun)),
	)))
	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "group", 0, "a-effect", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "group", 2, "failed", false, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StepCompensating, st.step("group").Status)

	// The late success is a new effect that still needs its own compensation
	reportSuccess(t, st, def, "group", 1, "b-effect", now)
	advance(st, def, "inst-1", now)
	assert.NotNil(t, st.step("group").Tasks[1].Comp)
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "group", Index: 0, Attempt: 1}}, now)
	advance(st, def, "inst-1", now)
	assert.False(t, st.Status.IsTerminal(), "the second effect still needs undo")
	t.Logf("status=%s compensation=%s b.output=%s b.comp=%v", st.Status, st.Compensation, st.step("group").Tasks[1].Output, st.step("group").Tasks[1].Comp)
}

func TestRepeatedUnwindPreservesFailedRollback(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "child-failed-undo", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun)),
	))
	st := startJournal(t, def, now)
	st.Parent = &parentRef{InstanceID: "parent", Workflow: "parent-workflow", Step: "child", Attempt: 1}
	reportSuccess(t, st, def, "a", 0, "effect", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "b", 0, "failed", false, now)
	advance(st, def, "inst-1", now)
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "a", Index: 0, Attempt: 1, Error: "undo failed"}}, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, CompensationPartial, st.Compensation)

	// Asking for the same rollback later cannot erase its known failure
	apply(st, def, &event{kind: evUnwind, fromParent: true, compAttempt: 1}, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, CompensationPartial, st.Compensation)
	t.Logf("status=%s compensation=%s a.status=%s a.comp.error=%s", st.Status, st.Compensation, st.step("a").Status, st.step("a").Tasks[0].Comp.Error)
}

func TestStepTimeoutRecordsLateSuccess(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "timeout-late", WithSteps(
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("b", WithRun(noopRun), WithCompensate(noopCompensate), WithStepTimeout(time.Second)),
	))
	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "a", 0, "a-effect", now)
	advance(st, def, "inst-1", now)
	o := &orchestrator{def: def}
	o.applyElapsedDeadlines(st, now.Add(2*time.Second))
	advance(st, def, "inst-1", now.Add(2*time.Second))
	require.Equal(t, StatusCompensating, st.Status)

	// Timeout does not interrupt the worker, so its eventual success is still real
	duplicate := reportSuccess(t, st, def, "b", 0, "b-effect", now.Add(3*time.Second))
	advance(st, def, "inst-1", now.Add(3*time.Second))
	assert.False(t, duplicate)
	assert.NotNil(t, st.step("b").Tasks[0].Comp)
}

func TestLateSuccessReopensATerminatedCancellation(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "terminal-late", WithSteps(
		Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
	))
	st := startJournal(t, def, now)
	apply(st, def, &event{kind: evCancel, reason: "stop"}, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StatusCancelled, st.Status)

	// A worker that was already executing can still reveal an effect after the cancellation looked terminal
	duplicate := reportSuccess(t, st, def, "effect", 0, "created-resource", now.Add(time.Second))
	require.False(t, duplicate)
	advance(st, def, "inst-1", now.Add(time.Second))
	assert.Equal(t, StatusCompensating, st.Status)
	assert.NotNil(t, st.step("effect").task(0).Comp)
}

func TestLateSuccessDoesNotRepeatAClosedCompensation(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "terminal-frame", WithSteps(Parallel("effects",
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun), WithCompensate(noopCompensate)),
	)))
	st := startJournal(t, def, now)
	apply(st, def, &event{kind: evCancel, reason: "stop"}, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StatusCancelled, st.Status)

	// The first late effect reopens and completes its compensation
	reportSuccess(t, st, def, "effects", 0, "first-effect", now.Add(time.Second))
	advance(st, def, "inst-1", now.Add(time.Second))
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "effects", Index: 0, Attempt: 1}}, now.Add(2*time.Second))
	advance(st, def, "inst-1", now.Add(2*time.Second))
	require.Equal(t, StatusCancelled, st.Status)
	require.True(t, st.step("effects").task(0).Compensated)
	require.True(t, st.step("effects").task(0).Comp.Done)

	// A second late effect opens only the compensation that has not already run
	reportSuccess(t, st, def, "effects", 1, "second-effect", now.Add(3*time.Second))
	advance(st, def, "inst-1", now.Add(3*time.Second))
	assert.True(t, st.step("effects").task(0).Comp.Done)
	assert.True(t, st.step("effects").task(0).Compensated)
	assert.NotNil(t, st.step("effects").task(1).Comp)
	assert.False(t, st.step("effects").task(1).Comp.Done)
}

func TestAdvanceLoopRepeatsUntilItsConditionHolds(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "polling", WithSteps(
		Step("prepare", WithRun(noopRun)),
		Loop("poll",
			Step("check", WithRun(noopRun)),
			Step("pause", WithRun(noopRun)),
		).With(WithUntil("check", true), WithMaxIterations(5)),
		Step("finish", WithRun(noopRun)),
	))

	// A loop's body is flattened into the graph, so its steps are ordinary steps that run before the loop node
	require.Len(t, def.steps, 5)
	names := make([]string, len(def.steps))
	for i, d := range def.steps {
		names[i] = d.name
	}
	assert.Equal(t, []string{"prepare", "check", "pause", "poll", "finish"}, names)

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "prepare", 0, nil, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "check"))

	// The condition is read only after the body has run, so the first iteration always happens
	reportSuccess(t, st, def, "check", 0, false, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "pause"))

	reportSuccess(t, st, def, "pause", 0, nil, now)
	advance(st, def, "inst-1", now)

	// The condition did not hold, so the body opened again rather than the loop settling
	assert.Equal(t, StepRunning, stepStatus(t, st, "check"))
	assert.Equal(t, StepPending, stepStatus(t, st, "poll"))
	assert.Equal(t, 1, st.step("check").Iteration)
	require.Len(t, st.step("check").Tasks, 2)
	assert.Equal(t, 1, st.step("check").Tasks[1].Index, "a body step's task index is its iteration, which is what keeps every iteration's worker distinct")

	reportSuccess(t, st, def, "check", 1, true, now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "pause", 1, nil, now)
	advance(st, def, "inst-1", now)

	assert.Equal(t, StepCompleted, stepStatus(t, st, "poll"))
	assert.Equal(t, 2, st.step("poll").Iteration)
	assert.Equal(t, StepRunning, stepStatus(t, st, "finish"))
	assert.JSONEq(t, `true`, string(def.byName["check"].stepOutput(st.step("check"))), "a later step reads what the last iteration produced")
}

func TestAdvanceLoopFailsWhenTheConditionNeverHolds(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "stubborn", WithSteps(
		Loop("poll",
			Step("check", WithRun(noopRun)),
		).With(WithUntil("check", true), WithMaxIterations(2)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "check", 0, false, now)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "check"))

	reportSuccess(t, st, def, "check", 1, false, now)
	advance(st, def, "inst-1", now)

	// The bound is what keeps a condition that never holds from running the instance to its timeout instead
	assert.Equal(t, StepFailed, stepStatus(t, st, "poll"))
	assert.Contains(t, st.step("poll").Error, "within 2 iterations")
	assert.Equal(t, StatusFailed, st.Status)
}

func TestAdvanceLoopUndoesEveryIteration(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "looped-saga", WithSteps(
		Loop("attempts",
			Step("charge", WithRun(noopRun), WithCompensate(noopCompensate)),
		).With(WithUntil("charge", true), WithMaxIterations(5)),
		Step("finish", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "charge", 0, false, now)
	advance(st, def, "inst-1", now)
	reportSuccess(t, st, def, "charge", 1, true, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StepCompleted, stepStatus(t, st, "attempts"))

	reportFailure(t, st, def, "finish", 0, "boom", false, now)
	advance(st, def, "inst-1", now)

	// Both iterations really charged, so the unwind has to undo both rather than only the last
	require.Equal(t, StatusCompensating, st.Status)
	sr := st.step("charge")
	require.Len(t, sr.Tasks, 2)
	assert.Len(t, compensableTasks(sr, def.byName["charge"]), 2)
}

func TestAdvanceLoopWaitStepWaitsForItsOwnEventEachIteration(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "gated-loop", WithSteps(
		Loop("rounds",
			WaitForEvent("tick", WithEventTimeout(time.Hour)),
			Step("done", WithRun(noopRun)),
		).With(WithUntil("done", true), WithMaxIterations(3)),
	))

	st := startJournal(t, def, now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "tick"))

	dup := apply(st, def, &event{kind: evRaise, raise: &eventPayload{Name: "tick", Payload: json.RawMessage(`1`)}}, now)
	require.False(t, dup)
	advance(st, def, "inst-1", now)
	assert.Equal(t, StepRunning, stepStatus(t, st, "done"))

	reportSuccess(t, st, def, "done", 0, false, now)
	advance(st, def, "inst-1", now)

	// The next iteration's wait starts empty, so it parks for an event of its own rather than settling on the one already recorded
	assert.Equal(t, StepRunning, stepStatus(t, st, "tick"))
	assert.Nil(t, st.step("tick").Event)
}

func TestAdvanceLoopWhoseBodyAlwaysSkipsStillEndsAtItsBound(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "skipping", WithSteps(
		Step("gate", WithRun(noopRun)),
		Loop("poll",
			Step("check", WithRun(noopRun), WithSkipIf("gate", false)),
		).With(WithUntil("check", true), WithMaxIterations(4)),
	))

	st := startJournal(t, def, now)

	// A skipped body step settles without a task, so every iteration of this loop runs inside one turn
	reportSuccess(t, st, def, "gate", 0, false, now)
	advance(st, def, "inst-1", now)

	// The loop's own bound is what ends it, which is what keeps the fixed-point walk from stopping short and leaving the instance to its timeout
	assert.Equal(t, StepSkipped, stepStatus(t, st, "check"))
	assert.Equal(t, StepFailed, stepStatus(t, st, "poll"))
	assert.Equal(t, 4, st.step("poll").Iteration)
	assert.Equal(t, StatusFailed, st.Status)
}
