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
