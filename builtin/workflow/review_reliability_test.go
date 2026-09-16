package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func int64MetricTotal(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	err := reader.Collect(t.Context(), &collected)
	require.NoError(t, err)

	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s should contain an int64 sum", name)
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	require.FailNow(t, "metric was not collected", name)
	return 0
}

func TestReviewDeadLetterRecoveryPreservesDeadline(t *testing.T) {
	host := newFakeHost()
	wf, err := New("review-deadline", WithTimeout(time.Hour), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	originalDue, armed := alarmDue(t, host, wf, "instance")
	require.True(t, armed)

	// Deliver the recovery alarm on the same activation that armed the original deadline
	require.NoError(t, o.JobFailed(t.Context(), "report", methodDone, nil, errors.New("transport failed")))
	require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))
	actualDue, armed := alarmDue(t, host, wf, "instance")
	require.True(t, armed)
	require.Equal(t, originalDue, actualDue, "recovery must replace its immediate alarm with the outstanding deadline")
}

func TestReviewTimeoutStartingUnwindKeepsBackstop(t *testing.T) {
	host := newFakeHost()
	wf, err := New("review-timeout-unwind", WithTimeout(time.Hour), WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun)),
	))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1}}))

	// Advance the persisted deadline without requiring the test to wait an hour
	st, err := o.client.GetState(t.Context())
	require.NoError(t, err)
	st.StartedAt = time.Now().Add(-2 * time.Hour)
	st.DeadlineAt = instanceDeadline(&st, wf.def)
	require.NoError(t, o.client.SetState(t.Context(), st, nil))
	require.NoError(t, o.armDeadline(t.Context(), &st))

	// Consume the one-shot alarm before invoking its handler so only a replacement remains afterwards
	require.NoError(t, host.DeleteAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline))
	require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))
	st = readJournal(t, host, wf, "instance")
	_, armed := alarmDue(t, host, wf, "instance")
	require.True(t, st.Status.IsTerminal() || armed, "a newly opened unwind must terminate or retain its timeout backstop")
}

type reviewFailingStateHost struct {
	*fakeHost

	failState bool
}

type reviewFailingCleanupHost struct {
	*fakeHost

	failList bool
}

func (h *reviewFailingCleanupHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	if h.failList {
		return nil, errors.New("injected job listing failure")
	}
	return h.fakeHost.ListJobs(ctx, actorType, actorID)
}

func (h *reviewFailingStateHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	if h.failState {
		return errors.New("injected state write failure")
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func TestReviewFailedStateWriteDoesNotCorruptRetry(t *testing.T) {
	host := &reviewFailingStateHost{fakeHost: newFakeHost()}
	wf, err := New("review-state-failure", WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun)),
	))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1}}))
	report := &payloadEnvelope{value: reportPayload{Step: "second", Index: 0, Attempt: 1, Error: "permanent handler failure"}}

	// Fail persistence while the report changes the workflow from running to compensating
	host.failState = true
	require.Error(t, o.Job(t.Context(), methodDone, report))
	durable := readJournal(t, host.fakeHost, wf, "instance")
	require.Equal(t, StepRunning, durable.step("second").Status)

	// Retry on the same activation, as a transient provider failure normally does
	host.failState = false
	require.NoError(t, o.Job(t.Context(), methodDone, report))
	durable = readJournal(t, host.fakeHost, wf, "instance")
	require.Equal(t, StatusCompensating, durable.Status, "the failed write must not leave a running workflow with a compensating step")
	require.Contains(t, durable.Cause, "permanent handler failure")
}

func TestSuspendedFailureResumesThroughAnOrchestratorTurn(t *testing.T) {
	host := newFakeHost()
	wf, err := New("review-suspended-turn", WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun)),
	))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1}}))
	require.NoError(t, o.Job(t.Context(), methodSuspend, &payloadEnvelope{value: reasonPayload{Reason: "pause"}}))

	// A failure delivered while paused records a resumable compensation state without dispatching undo work
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "second", Index: 0, Attempt: 1, Error: "failed"}}))
	st := readJournal(t, host, wf, "instance")
	require.Equal(t, StatusSuspended, st.Status)
	require.Equal(t, StatusCompensating, st.Suspended.ResumeTo)
	require.Empty(t, host.dispatchedTo(builtinActorType(wf.undoType("")), workerActorID("instance", "first", 0)))

	// Resume opens the compensation frame and dispatches the retained undo
	require.NoError(t, o.Job(t.Context(), methodResume, nil))
	st = readJournal(t, host, wf, "instance")
	require.Equal(t, StatusCompensating, st.Status)
	require.Contains(t, host.dispatchedTo(builtinActorType(wf.undoType("")), workerActorID("instance", "first", 0)), methodCompensate)
}

func TestTerminalCleanupFailureLeavesCancellationRetryable(t *testing.T) {
	host := &reviewFailingCleanupHost{fakeHost: newFakeHost()}
	wf, err := New("review-cleanup-retry", WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// A cleanup failure must leave the pre-cancellation journal durable so Francis can retry the same event
	host.failList = true
	err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}})
	require.Error(t, err)
	st := readJournal(t, host.fakeHost, wf, "instance")
	require.Equal(t, StatusRunning, st.Status)

	// Retrying after storage recovers publishes termination only after the queued work is gone
	host.failList = false
	require.NoError(t, o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}}))
	st = readJournal(t, host.fakeHost, wf, "instance")
	require.Equal(t, StatusCancelled, st.Status)
	jobs, err := host.ListJobs(t.Context(), builtinActorType(wf.workerType("")), workerActorID("instance", "effect", 0))
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestReviewDeadLetteredChildReportCanBeRecovered(t *testing.T) {
	host := newFakeHost()
	kid, err := New("review-report-child", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	parentWF, err := New("review-report-parent", WithSteps(Child("sub", WithDefinition(kid))))
	require.NoError(t, err)
	parent := newTestOrchestrator(t, parentWF, host, "parent")
	require.NoError(t, parent.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	st := readJournal(t, host, parentWF, "parent")
	childID := st.step("sub").task(0).ChildID
	startJob := host.jobIDFor(builtinActorType(kid.baseType), childID, methodStart)
	require.NotEmpty(t, startJob)
	start := host.jobPayloads[startJob]
	child := newTestOrchestrator(t, kid, host, childID)
	require.NoError(t, child.Job(t.Context(), methodStart, &payloadEnvelope{value: start}))
	host.mu.Lock()
	host.removeJobLocked(startJob)
	host.mu.Unlock()
	require.NoError(t, child.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "only", Index: 0, Attempt: 1}}))
	require.True(t, readJournal(t, host, kid, childID).Reported)

	// A durable child report exhausts transport retries before its parent can apply it
	reportJob := host.jobIDFor(builtinActorType(parentWF.baseType), "parent", methodDone)
	require.NotEmpty(t, reportJob)
	host.deadLetter(reportJob, "injected report delivery failure")
	require.NoError(t, parent.JobFailed(t.Context(), reportJob, methodDone, nil, errors.New("delivery failed")))
	require.NoError(t, parent.Alarm(t.Context(), alarmDeadline, nil))

	// The durable report is replayed directly without executing or restarting the completed child
	recoveredJob := host.jobIDFor(builtinActorType(parentWF.baseType), "parent", methodDone)
	require.NotEmpty(t, recoveredJob)
	require.NotEqual(t, reportJob, recoveredJob)
	err = parent.Job(t.Context(), methodDone, &payloadEnvelope{value: host.jobPayloads[recoveredJob]})
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, readJournal(t, host, parentWF, "parent").Status)
}

func TestLateSuccessReopeningKeepsLifecycleMetricsBalanced(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(t.Context()))
	})

	host := newFakeHost()
	wf, err := New("review-reopen-metrics",
		WithMeter(provider.Meter("review")),
		WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))),
	)
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}}))

	// Work that escaped cancellation reopens the terminal instance until its newly discovered effect is undone
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}}))
	require.NoError(t, o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "effect", Index: 0, Attempt: 1}}))

	require.Equal(t, int64(0), int64MetricTotal(t, reader, "francis.workflow.instances.running"))
	require.Equal(t, int64(1), int64MetricTotal(t, reader, "francis.workflow.instances.terminated"))
}
