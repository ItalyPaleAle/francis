package workflow

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

// childOf is the parent reference a child instance is started with
func childOf(parentID string) *parentRef {
	return &parentRef{
		InstanceID: parentID,
		Workflow:   "parent-wf",
		Step:       "sub",
		Index:      0,
		Depth:      1,
		Attempt:    1,
	}
}

// reportsToParent returns the methods dispatched to the parent instance, whatever workflow type it belongs to
func reportsToParent(t *testing.T, host *fakeHost, parentID string) []string {
	t.Helper()

	host.mu.Lock()
	defer host.mu.Unlock()

	var out []string
	for _, j := range host.jobs {
		if j.ActorID == parentID {
			out = append(out, j.Method)
		}
	}
	return out
}

// stateTTL returns the expiry an instance's journal was last written with
func stateTTL(t *testing.T, host *fakeHost, wf *Workflow, instanceID string) time.Duration {
	t.Helper()

	host.mu.Lock()
	defer host.mu.Unlock()
	return host.ttls[key(builtinActorType(wf.baseType), instanceID)]
}

func TestATerminatedChildKeepsTryingToReportToItsParent(t *testing.T) {
	host := newFakeHost()
	wf, err := New("stranded-child", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "child-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1, Parent: childOf("parent-1")}}))

	// The child terminates, and the dispatch that would tell its parent fails
	host.failDispatch = true
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}})
	require.Error(t, err)

	st := readJournal(t, host, wf, "child-1")
	require.Equal(t, StatusCompleted, st.Status)
	require.False(t, st.Reported, "the parent was never told")
	assert.Empty(t, reportsToParent(t, host, "parent-1"))

	// The journal is already terminal, so the delivery being retried is the only thing left that can get the report out
	host.failDispatch = false
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))

	assert.Equal(t, []string{methodDone}, reportsToParent(t, host, "parent-1"))
	assert.True(t, readJournal(t, host, wf, "child-1").Reported)

	// Once the parent has been told, a repeated delivery is dropped rather than rewriting the journal every time
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))
	assert.Len(t, reportsToParent(t, host, "parent-1"), 1)
}

func TestAChildsJournalIsNotGivenAnExpiryItsParentCannotSee(t *testing.T) {
	host := newFakeHost()
	wf, err := New("child-ttl", WithRetention(RetentionPolicy{Completed: time.Hour}), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	t.Run("a child keeps its journal until something removes it", func(t *testing.T) {
		o := newTestOrchestrator(t, wf, host, "child-1")
		require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1, Parent: childOf("parent-1")}}))
		require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))

		require.Equal(t, StatusCompleted, readJournal(t, host, wf, "child-1").Status)
		assert.Zero(t, stateTTL(t, host, wf, "child-1"), "a parent may still ask this child to undo itself, and an expiry it cannot see would take the journal out from under it")
	})

	t.Run("a top-level instance expires on its own", func(t *testing.T) {
		o := newTestOrchestrator(t, wf, host, "top-1")
		require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
		require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))

		assert.Equal(t, 2*time.Hour, stateTTL(t, host, wf, "top-1"), "twice the retention, so the sweep can still find what it needs to clean up")
	})
}

func TestUnwindingAParentCancelsAChildThatIsStillRunning(t *testing.T) {
	kid, err := New("kid", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)

	host := newFakeHost()
	wf, err := New("parent-of-running", WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Child("sub", WithDefinition(kid)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1, Output: json.RawMessage(`"ok"`)}}))

	st := readJournal(t, host, wf, "inst-1")
	require.Equal(t, StepRunning, stepStatus(t, &st, "sub"))
	childID := st.step("sub").task(0).ChildID
	require.NotEmpty(t, childID)

	// Cancelling the parent abandons the child task, which never reported: the child is a live instance of its own and nothing but this frame will stop it
	require.NoError(t, o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "operator changed their mind"}}))

	st = readJournal(t, host, wf, "inst-1")
	require.Equal(t, StatusCompensating, st.Status)
	sub := st.step("sub")
	require.True(t, sub.task(0).Abandoned)
	require.NotNil(t, sub.task(0).Comp, "an abandoned child task is on the compensation stack whatever the step opted into")
	assert.Contains(t, st.Stack, "sub")

	// The child is asked to undo itself, which is the one verb it still accepts however far along it is
	assert.Contains(t, host.dispatchedTo(builtinActorType(kid.baseType), childID), methodUnwind)
}

func TestAChildThatFinishedFirstStillReportsItsUnwind(t *testing.T) {
	host := newFakeHost()
	kid, err := New("late-kid", WithSteps(Step("only", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)

	// The child completes and tells its parent, so its journal is terminal by the time the parent decides to unwind
	child := newTestOrchestrator(t, kid, host, "kid-1")
	require.NoError(t, child.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1, Parent: childOf("parent-1")}}))
	require.NoError(t, child.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "only", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))
	require.Equal(t, StatusCompleted, readJournal(t, host, kid, "kid-1").Status)

	// The unwind reopens it rather than being ignored, so the frame the parent is holding is answered instead of staying outstanding forever
	require.NoError(t, child.Job(t.Context(), methodUnwind, &payloadEnvelope{value: reasonPayload{Reason: "the parent failed", FromParent: true, CompAttempt: 1}}))

	st := readJournal(t, host, kid, "kid-1")
	assert.Equal(t, StatusCompensating, st.Status)
	assert.False(t, st.Reported, "the child owes its parent a second report, this time a compensation")
	require.NotNil(t, st.Parent)
	assert.Equal(t, 1, st.Parent.UnwoundBy)
}

func TestAStuckUnwindEndsAtTheInstanceDeadline(t *testing.T) {
	host := newFakeHost()
	wf, err := New("stuck-unwind",
		WithTimeout(time.Minute),
		WithSteps(
			Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
			Step("boom", WithRun(noopRun)),
		),
	)
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1, Output: json.RawMessage(`"ok"`)}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "boom", Index: 0, Attempt: 1, Error: "induced failure", Retryable: false}}))

	st := readJournal(t, host, wf, "inst-1")
	require.Equal(t, StatusCompensating, st.Status, "the compensation of the first step is outstanding")

	backdateStart(t, host, wf, "inst-1", 2*time.Minute)

	// The deadline is served by a fresh activation, which is also how it arrives in practice: an orchestrator holds its journal for the life of one activation, and this one has been sitting on the unwind
	// Compensation gets a fresh instance-sized timeout, so a compensation nobody is going to finish is abandoned when that second budget elapses rather than left running forever
	stale := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, stale.Alarm(t.Context(), alarmDeadline, nil))

	st = readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationFailed, st.Compensation)
	assert.Contains(t, st.Cause, "induced failure", "why the instance started unwinding is still the first thing an operator asks")
	assert.Contains(t, st.Cause, "unwind abandoned")
	assert.Contains(t, st.Stack, "first", "the frames left on the stack name the effects nothing undid")

	_, armed := alarmDue(t, host, wf, "inst-1")
	assert.False(t, armed, "a terminated instance stops holding its deadline")
}

func TestADeadlineDeliveredAFractionEarlyStillResolves(t *testing.T) {
	// An alarm's due time is stored at the provider's own resolution, which is coarser than the nanoseconds the journal keeps
	// A turn that found nothing elapsed would change nothing, and the one-shot alarm it came from is already gone, so the instance would be left with no timer at all
	st := &instanceState{DeadlineAt: time.Now().Add(400 * time.Microsecond)}
	deadline := &event{kind: evDeadline}

	now := time.Now()
	assert.Equal(t, st.DeadlineAt, st.deadlineTurnTime(deadline, now), "the instant the alarm was armed for counts as reached")

	// The tolerance is bounded to the alarm's resolution, so a deadline genuinely further out is never brought forward
	far := &instanceState{DeadlineAt: now.Add(time.Hour)}
	assert.Equal(t, now, far.deadlineTurnTime(deadline, now))

	// Every other event reads the wall clock, whatever the journal's deadline says
	assert.Equal(t, now, st.deadlineTurnTime(&event{kind: evDone}, now))

	// A journal with no deadline has nothing to bring forward
	assert.Equal(t, now, (&instanceState{}).deadlineTurnTime(deadline, now))
}

func TestTheUnwindAbandonedCauseKeepsTheOriginalOne(t *testing.T) {
	assert.Equal(t, "unwind abandoned: instance timeout elapsed", unwindAbandonedCause(""))
	assert.True(t, strings.HasPrefix(unwindAbandonedCause("payment declined"), "payment declined; "))
}

func TestStartReportsWhichCallCreatedTheInstance(t *testing.T) {
	host := newFakeHost()
	wf, err := New("start-created", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := wf.Service(actor.NewService(host))

	// Nothing runs the start job here, which is the window the question is about: before it has run there is no journal for a second caller to find
	id, created, err := svc.Start(t.Context(), nil, WithInstanceID("inst-1"))
	require.NoError(t, err)
	require.Equal(t, "inst-1", id)
	assert.True(t, created, "this call is the one that started the instance")

	// The second call's start job coalesces onto the one already holding the key, and the dispatch is the only thing that can say so
	_, created, err = svc.Start(t.Context(), nil, WithInstanceID("inst-1"))
	require.NoError(t, err)
	assert.False(t, created)

	// A different instance is a different question
	_, created, err = svc.Start(t.Context(), nil, WithInstanceID("inst-2"))
	require.NoError(t, err)
	assert.True(t, created)
}

func TestStartFindsAnInstanceThatAlreadyHasAJournal(t *testing.T) {
	host := newFakeHost()
	wf, err := New("start-existing", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// An instance that already has a journal is not restarted, whatever its status, and the repeated start is simply dropped
	svc := wf.Service(actor.NewService(host))
	_, created, err := svc.Start(t.Context(), nil, WithInstanceID("inst-1"))
	require.NoError(t, err)
	assert.False(t, created)
}

func TestChildAdmissionEnforcesItsInputLimit(t *testing.T) {
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

func TestParentUnwindsForwardFailedChild(t *testing.T) {
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

func TestJoiningChildUnwindKeepsLatestAttempt(t *testing.T) {
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

func TestChildRejectsOversizedStartDurably(t *testing.T) {
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

func TestParentStatusRetainsTheChildCompensationOutcome(t *testing.T) {
	child, err := New("outcome-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "outcome-parent", WithSteps(
		Child("sub", WithDefinition(child), WithOptional()),
	))
	st := startJournal(t, def, time.Now())
	tr := st.step("sub").task(0)
	require.NotNil(t, tr)

	// A child may leave effects behind even when the parent policy allows the workflow to continue
	apply(st, def, &event{kind: evDone, report: &reportPayload{
		Step:              "sub",
		Index:             0,
		Attempt:           1,
		Error:             "child failed",
		ChildStatus:       StatusFailed,
		ChildCompensation: CompensationPartial,
	}}, time.Now())
	advance(st, def, "parent-1", time.Now())
	view := statusView("parent-1", st, def)
	require.Len(t, view.Steps, 1)
	require.Len(t, view.Steps[0].Children, 1)
	assert.Equal(t, StatusFailed, view.Steps[0].Children[0].Status)
	assert.Equal(t, CompensationPartial, view.Steps[0].Children[0].Compensation)
}

func TestParentStatusUpdatesAChildOutcomeAfterParentDrivenUnwind(t *testing.T) {
	child, err := New("unwind-outcome-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "unwind-outcome-parent", WithSteps(Child("sub", WithDefinition(child))))
	st := startJournal(t, def, time.Now())
	tr := st.step("sub").task(0)
	require.NotNil(t, tr)
	tr.Comp = &compRecord{Attempts: 1}

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{
		Step:              "sub",
		Index:             0,
		Attempt:           1,
		Error:             "child rollback was incomplete",
		ChildStatus:       StatusCancelled,
		ChildCompensation: CompensationPartial,
	}}, time.Now())

	view := statusView("parent-1", st, def)
	require.Len(t, view.Steps[0].Children, 1)
	assert.Equal(t, StatusCancelled, view.Steps[0].Children[0].Status)
	assert.Equal(t, CompensationPartial, view.Steps[0].Children[0].Compensation)
}

func TestDeadLetteredChildReportCanBeRecovered(t *testing.T) {
	host := newFakeHost()
	kid, err := New("report-child", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	parentWF, err := New("report-parent", WithSteps(Child("sub", WithDefinition(kid))))
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

func TestParentInstanceTimeoutUnwindsChild(t *testing.T) {
	now := time.Now()
	child, err := New("timeout-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "timeout-parent", WithTimeout(time.Second), WithSteps(
		Child("child", WithDefinition(child)),
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
