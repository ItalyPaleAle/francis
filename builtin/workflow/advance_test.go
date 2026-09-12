package workflow

import (
	"context"
	"encoding/json"
	"errors"
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

	// The full step list is what makes status and the unknown-version path answerable from the journal alone
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

	// A duplicate still leaves the journal saying the next step should be running, which is what keeps the instance from stalling
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
	assert.JSONEq(t, `[1,{"error":"unlucky"},3]`, string(stepOutput(st.step("work"), def.byName["work"])))
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
	assert.JSONEq(t, `{"by":"ops"}`, string(stepOutput(st.step("approval"), def.byName["approval"])))
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

	// Running advance any number of times over the same journal converges on the same answer, which is what makes recovery not a special code path
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
	assert.JSONEq(t, `{"email":"email-ok","sms":"sms-ok"}`, string(stepOutput(st.step("notify"), def.byName["notify"])))
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

func TestBackoffDoublesAndStopsAtTheCap(t *testing.T) {
	assert.Equal(t, 2*time.Second, backoff(2*time.Second, time.Minute, defaultRetryInitial, defaultRetryMax, 1))
	assert.Equal(t, 4*time.Second, backoff(2*time.Second, time.Minute, defaultRetryInitial, defaultRetryMax, 2))
	assert.Equal(t, 8*time.Second, backoff(2*time.Second, time.Minute, defaultRetryInitial, defaultRetryMax, 3))
	assert.Equal(t, time.Minute, backoff(2*time.Second, time.Minute, defaultRetryInitial, defaultRetryMax, 20))

	// An unset initial or cap falls back to the defaults the engine documents
	assert.Equal(t, defaultRetryInitial, backoff(0, 0, defaultRetryInitial, defaultRetryMax, 1))
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
	assert.False(t, errors.Is(nil, ErrJournalTooLarge))
}
