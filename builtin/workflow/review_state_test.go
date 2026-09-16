package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewSuspendedFailurePreservesSuspension(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-suspend", WithSteps(
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

func TestReviewLateSuccessJoinsOpenCompensationFrame(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-late-frame", WithSteps(Parallel("group",
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

func TestReviewMemberCompensateOnFailure(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-member-policy", WithSteps(Parallel("group",
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateOnFailure()),
	)))
	st := startJournal(t, def, now)
	reportFailure(t, st, def, "group", 0, "partial effect", false, now)
	advance(st, def, "inst-1", now)
	assert.NotNil(t, st.step("group").Tasks[0].Comp)
	t.Logf("status=%s compensation=%s", st.Status, st.Compensation)
}

func TestReviewRepeatedUnwindPreservesFailedRollback(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-child-failed-undo", WithSteps(
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

func TestReviewStepTimeoutUnwindsChild(t *testing.T) {
	now := time.Now()
	child, err := New("review-timeout-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "review-timeout-parent", WithSteps(
		Child("child", WithDefinition(child), WithStepTimeout(time.Second)),
	))
	st := startJournal(t, def, now)
	require.NotEmpty(t, st.step("child").Tasks[0].ChildID)

	// A timed-out child remains independently active until its parent sends an unwind
	o := &orchestrator{def: def}
	o.applyElapsedDeadlines(st, now.Add(2*time.Second))
	advance(st, def, "inst-1", now.Add(2*time.Second))
	assert.NotNil(t, st.step("child").Tasks[0].Comp)
	t.Logf("status=%s compensation=%s child.abandoned=%v", st.Status, st.Compensation, st.step("child").Tasks[0].Abandoned)
}

func TestReviewStepTimeoutRecordsLateSuccess(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-timeout-late", WithSteps(
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

func TestReviewCancelDoesNotLeaveQueuedForwardJob(t *testing.T) {
	host := newFakeHost()
	wf, err := New("review-cancel-queue", WithSteps(Step("a", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "inst-1")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}})
	require.NoError(t, err)
	st := readJournal(t, host, wf, "inst-1")
	require.Equal(t, StatusCancelled, st.Status)

	// Cancellation before execution must remove work that is still only queued
	jobs, err := host.ListJobs(t.Context(), builtinActorType(wf.workerType("")), workerActorID("inst-1", "a", 0))
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestReviewLateSuccessReopensATerminatedCancellation(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-terminal-late", WithSteps(
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

func TestOpenedTasksRetainEveryActorTypeNeededForCleanup(t *testing.T) {
	child, err := New("review-cleanup-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	wf, err := New("review-cleanup-types",
		WithCapability("gpu"),
		WithSteps(Parallel("work",
			Step("render", WithRun(noopRun), WithCompensate(noopCompensate), WithRequiredCapability("gpu")),
			Child("child", WithDefinition(child)),
		)),
	)
	require.NoError(t, err)
	st := startJournal(t, wf.def, time.Now())
	sr := st.step("work")
	require.NotNil(t, sr)
	require.Len(t, sr.Tasks, 2)

	require.Equal(t, wf.workerType("gpu"), sr.Tasks[0].WorkerType)
	require.Equal(t, wf.undoType("gpu"), sr.Tasks[0].UndoType)
	require.Equal(t, child.baseType, sr.Tasks[1].ChildType)
}

func TestLateSuccessDoesNotRepeatAClosedCompensation(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "review-terminal-frame", WithSteps(Parallel("effects",
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
