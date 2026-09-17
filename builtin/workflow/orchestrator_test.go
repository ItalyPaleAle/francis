package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
)

// alarmDue returns when the instance's deadline alarm is set for, and reports whether one is armed at all
func alarmDue(t *testing.T, host *fakeHost, wf *Workflow, instanceID string) (time.Time, bool) {
	t.Helper()

	props, err := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), instanceID, alarmDeadline)
	if err != nil {
		return time.Time{}, false
	}
	return props.DueTime, true
}

// backdateStart moves an instance's start time into the past, so a deadline computed from it has unambiguously elapsed
// A test cannot get there by declaring a tiny timeout and letting it pass: a clock's resolution can be coarser than the timeout itself, and on some platforms two reads of the clock return the same instant
func backdateStart(t *testing.T, host *fakeHost, wf *Workflow, instanceID string, by time.Duration) {
	t.Helper()

	st := readJournal(t, host, wf, instanceID)
	st.StartedAt = st.StartedAt.Add(-by)
	st.CreatedAt = st.CreatedAt.Add(-by)

	err := host.SetState(t.Context(), builtinActorType(wf.baseType), instanceID, st, nil)
	require.NoError(t, err)
}

// builtinActorType is the full type the fake host records a built-in actor's operations under
func builtinActorType(bareType string) string {
	return ref.BuiltInActorTypePrefix + bareType
}

func TestOrchestratorRejectsAMethodItDoesNotKnow(t *testing.T) {
	host := newFakeHost()
	wf, err := New("unknown-methods", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	t.Run("a job method", func(t *testing.T) {
		// A method this actor does not know would retry forever, so it fails permanently instead
		err := o.Job(t.Context(), "whatever", &payloadEnvelope{})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	})

	t.Run("an invoke method", func(t *testing.T) {
		_, err := o.Invoke(t.Context(), "whatever", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown workflow method")
	})

	t.Run("a peek method", func(t *testing.T) {
		_, err := o.Peek(t.Context(), "whatever", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown workflow peek method")
	})

	t.Run("an alarm name", func(t *testing.T) {
		// The instance has one alarm, so anything else is a leftover from an older engine and is not worth a turn
		require.NoError(t, o.Alarm(t.Context(), "leftover", nil))
		assert.Empty(t, host.state, "an alarm the engine does not know must not write a journal")
	})
}

func TestOrchestratorRejectsAPayloadItCannotDecode(t *testing.T) {
	host := newFakeHost()
	wf, err := New("bad-payload", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	// A payload that cannot be decoded fails the same way on every attempt, so retrying it would only waste attempts
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: json.RawMessage(`"not a report"`)})
	require.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	assert.Empty(t, host.state)
}

func TestStatusAndPurgeReportAMissingInstance(t *testing.T) {
	host := newFakeHost()
	wf, err := New("missing", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	// An instance with no journal is indistinguishable from one whose journal passed its retention, and both answer "not found" rather than failing
	res, err := o.Peek(t.Context(), methodStatus, nil)
	require.NoError(t, err)
	status, ok := res.(statusResult)
	require.True(t, ok)
	assert.False(t, status.Found)

	res, err = o.Invoke(t.Context(), methodPurge, nil)
	require.NoError(t, err)
	purged, ok := res.(purgeResult)
	require.True(t, ok)
	assert.False(t, purged.Found)
}

func TestPurgeRefusesAnInstanceThatHasNotTerminated(t *testing.T) {
	host := newFakeHost()
	wf, err := New("still-running", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// Purging a running instance would delete the journal that accounts for the work still in flight
	res, err := o.Invoke(t.Context(), methodPurge, nil)
	require.NoError(t, err)
	purged, ok := res.(purgeResult)
	require.True(t, ok)
	assert.True(t, purged.Found)
	assert.True(t, purged.Active)
	assert.NotEmpty(t, host.state, "a refused purge leaves the journal alone")
}

func TestPurgeRemovesTheJournalAndTheJobsOfATerminatedInstance(t *testing.T) {
	host := newFakeHost()
	wf, err := New("purgeable", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))

	st := readJournal(t, host, wf, "inst-1")
	require.Equal(t, StatusCompleted, st.Status)

	res, err := o.Invoke(t.Context(), methodPurge, nil)
	require.NoError(t, err)
	purged, ok := res.(purgeResult)
	require.True(t, ok)
	assert.True(t, purged.Found)
	assert.False(t, purged.Active)
	assert.Empty(t, host.state, "the journal is what the purge is for")

	// Repeating an interrupted purge has to be safe, so a second call reports the instance as gone rather than failing
	res, err = o.Invoke(t.Context(), methodPurge, nil)
	require.NoError(t, err)
	purged, ok = res.(purgeResult)
	require.True(t, ok)
	assert.False(t, purged.Found)
}

func TestTheDeadlineParksOnAHostWithoutTheInstanceVersion(t *testing.T) {
	host := newFakeHost()

	// The journal is written by a host that serves the version, and the alarm then fires on one that does not
	serving, err := New("parked", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, serving, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	declining, err := New("parked", WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)
	host.registryResponse = registerResponse{Found: true, OK: false}

	before := time.Now()
	stale := newTestOrchestrator(t, declining, host, "inst-1")
	require.NoError(t, stale.Alarm(t.Context(), alarmDeadline, nil))

	// Parking re-arms the alarm and waits for a host that can serve the version, which is what List by version shows an operator
	due, armed := alarmDue(t, host, declining, "inst-1")
	require.True(t, armed)
	assert.WithinDuration(t, before.Add(defaultParkInterval), due, time.Minute)

	st := readJournal(t, host, declining, "inst-1")
	assert.Equal(t, StatusRunning, st.Status, "parking must not touch the journal it cannot interpret")
}

func TestTheDeadlineFailsAnInstanceNoHostCanServe(t *testing.T) {
	host := newFakeHost()

	serving, err := New("abandoned", WithTimeout(time.Minute), WithUnknownVersionPolicy(FailUnknownVersion), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, serving, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// The declining host follows the journal's original policy even though its own timeout and default policy differ
	declining, err := New("abandoned",
		WithVersion(2),
		WithSteps(Step("a", WithRun(noopRun))),
	)
	require.NoError(t, err)
	host.registryResponse = registerResponse{Found: true, OK: false}
	backdateStart(t, host, declining, "inst-1", 2*time.Minute)

	stale := newTestOrchestrator(t, declining, host, "inst-1")
	require.NoError(t, stale.Alarm(t.Context(), alarmDeadline, nil))

	// Nothing is compensated, since no host can run the compensations either
	st := readJournal(t, host, declining, "inst-1")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
	assert.Contains(t, st.Cause, "unknown version")
	assert.Equal(t, StepSkipped, st.Steps[0].Status)

	_, armed := alarmDue(t, host, declining, "inst-1")
	assert.False(t, armed, "a terminated instance stops holding its deadline")
}

func TestADeadLetteredInstanceJobIsIgnoredOnATerminatedInstance(t *testing.T) {
	host := newFakeHost()

	declining, err := New("terminal-park", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	host.registryResponse = registerResponse{Found: true, OK: false}

	// There is no journal at all here, which is what the alarm sees for an instance whose retention has passed
	stale := newTestOrchestrator(t, declining, host, "inst-1")
	require.NoError(t, stale.Alarm(t.Context(), alarmDeadline, nil))

	_, armed := alarmDue(t, host, declining, "inst-1")
	assert.False(t, armed, "an instance with no journal has nothing left to park for")
}

func TestAnUnwindOfATerminatedInstanceIsStillAccepted(t *testing.T) {
	host := newFakeHost()
	wf, err := New("late-unwind", WithSteps(Step("a", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"done"`)}}))
	require.Equal(t, StatusCompleted, readJournal(t, host, wf, "inst-1").Status)

	// An unwind is the one verb a terminated instance still accepts, since a parent that fails later has to be able to undo a child that already completed
	require.NoError(t, o.Job(t.Context(), methodUnwind, &payloadEnvelope{value: reasonPayload{Reason: "the parent failed", FromParent: true}}))

	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusCompensating, st.Status)
	assert.Equal(t, "the parent failed", st.Cause)
}

func TestALateReportOnATerminatedInstanceIsDropped(t *testing.T) {
	host := newFakeHost()
	wf, err := New("late-report", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "no longer needed"}}))

	st := readJournal(t, host, wf, "inst-1")
	require.Equal(t, StatusCancelled, st.Status)
	completedAt := st.CompletedAt

	// A task that was already running finishes and reports, and the journal of a terminated instance is not rewritten for it
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"too late"`)}}))

	st = readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusCancelled, st.Status)
	assert.Equal(t, completedAt, st.CompletedAt)
	assert.Equal(t, StepSkipped, stepStatus(t, &st, "b"), "the cancel already accounted for every step that had not run")
}

func TestAnOversizedJournalEndsTheInstanceWithoutCompensating(t *testing.T) {
	host := newFakeHost()
	wf, err := New("journal-cap",
		WithMaxJournalSize(1),
		WithSteps(Step("a", WithRun(noopRun), WithCompensate(noopCompensate))),
	)
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// The unwind would need more journal than there is room for, so the instance ends without one
	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Equal(t, CompensationNone, st.Compensation)
	assert.Contains(t, st.Cause, ErrJournalTooLarge.Error())
	assert.Empty(t, st.Stack)
	assert.Empty(t, host.dispatchedTo(builtinActorType(wf.workerType("")), workerActorID("inst-1", "a", 0)), "an instance that failed on the cap dispatches nothing")
}

func TestASettledStepDropsThePendingJobsOfItsAbandonedTasks(t *testing.T) {
	host := newFakeHost()
	wf, err := New("fail-fast", WithSteps(
		Step("plan", WithRun(noopRun)),
		// The fan-out is optional so its failure leaves the instance running, which is what puts a settled step in front of reconcile
		ForEach("items", WithItemsFrom("plan"), WithRun(noopRun), WithOptional()),
		Step("finish", WithRun(noopRun)),
	))
	require.NoError(t, err)

	workerType := builtinActorType(wf.workerType(""))
	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "plan", Index: 0, Attempt: 1, Output: json.RawMessage(`["a","b","c"]`)}}))

	// All three tasks are dispatched, and then the first failure decides the fan-out under the default fail-fast policy
	for i := range 3 {
		require.NotEmpty(t, host.jobIDFor(workerType, workerActorID("inst-1", "items", i), methodRun), "task %d should be dispatched", i)
	}

	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "items", Index: 0, Attempt: 1, Error: "boom", Retryable: false}}))

	// A job that has not started yet is removed, because there is no reason to run work whose result the journal has already decided
	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StepFailed, stepStatus(t, &st, "items"))
	assert.Equal(t, StatusRunning, st.Status)
	for i := 1; i < 3; i++ {
		assert.Empty(t, host.jobIDFor(workerType, workerActorID("inst-1", "items", i), methodRun), "task %d should no longer have a pending job", i)
		assert.True(t, st.step("items").task(i).Abandoned, "an abandoned task stays countable, since a success it reports late is real work")
	}
}

func TestAStartForAnotherVersionIsDeclined(t *testing.T) {
	host := newFakeHost()
	wf, err := New("version-gate", WithVersion(2), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	// A start queued before this host was upgraded names the older version, and applying this host's graph to it would stamp the journal v1 and populate it from the v2 definition
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.ErrorIs(t, err, actor.ErrJobRejected)
	assert.Empty(t, host.state, "no journal is written for a version this host does not serve")

	// The version this host does serve starts normally
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 2}}))
	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, 2, st.Version)
}

func TestADeadlineOnAJournalOfAnotherVersionFollowsThePolicy(t *testing.T) {
	// The journal is written by the host that serves its version, and the deadline then fires on one that has moved on
	seedJournal := func(t *testing.T, host *fakeHost, policy UnknownVersionPolicy) {
		t.Helper()

		old, err := New("version-drift", WithTimeout(time.Minute), WithUnknownVersionPolicy(policy), WithSteps(Step("a", WithRun(noopRun))))
		require.NoError(t, err)
		o := newTestOrchestrator(t, old, host, "inst-1")
		require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
		require.Equal(t, 1, readJournal(t, host, old, "inst-1").Version)
	}

	t.Run("parking waits for a host that can serve the version", func(t *testing.T) {
		host := newFakeHost()
		seedJournal(t, host, ParkUnknownVersion)

		upgraded, err := New("version-drift", WithVersion(2), WithSteps(Step("a", WithRun(noopRun))))
		require.NoError(t, err)

		// This host's own version is registered and servable, so the mismatch is the journal's alone
		before := time.Now()
		o := newTestOrchestrator(t, upgraded, host, "inst-1")
		require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))

		due, armed := alarmDue(t, host, upgraded, "inst-1")
		require.True(t, armed, "the deadline is re-armed rather than declined forever")
		assert.WithinDuration(t, before.Add(defaultParkInterval), due, time.Minute)

		assert.Equal(t, StatusRunning, readJournal(t, host, upgraded, "inst-1").Status, "parking must not touch the journal it cannot interpret")
	})

	t.Run("failing ends an instance no host can serve", func(t *testing.T) {
		host := newFakeHost()
		seedJournal(t, host, FailUnknownVersion)

		upgraded, err := New("version-drift",
			WithVersion(2),
			WithTimeout(time.Minute),
			WithUnknownVersionPolicy(FailUnknownVersion),
			WithSteps(Step("a", WithRun(noopRun))),
		)
		require.NoError(t, err)
		backdateStart(t, host, upgraded, "inst-1", 2*time.Minute)

		o := newTestOrchestrator(t, upgraded, host, "inst-1")
		require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))

		st := readJournal(t, host, upgraded, "inst-1")
		assert.Equal(t, StatusFailed, st.Status)
		assert.Equal(t, CompensationNone, st.Compensation)
		assert.Contains(t, st.Cause, "unknown version 1")
	})
}

func TestRaiseEventUsesTheTargetVersionsMetadata(t *testing.T) {
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

func TestLegacyEventValidationRequiresTheMatchingVersion(t *testing.T) {
	// Legacy journals can use the caller's graph only when it has the same definition version
	wf, err := New("legacy-event", WithVersion(2), WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	svc := wf.Service(nil)
	limit, err := svc.eventLimit(&instanceState{Version: 2}, "approval")
	require.NoError(t, err)
	assert.Equal(t, wf.def.maxOutputSize, limit)
	_, err = svc.eventLimit(&instanceState{Version: 1}, "approval")
	require.ErrorContains(t, err, "use a service with that definition version")
	require.NotErrorIs(t, err, ErrNoSuchEvent, "an unavailable legacy contract does not establish that the event is invalid")

	// A recorded empty contract must not acquire an event merely because the newer host declares one
	_, err = svc.eventLimit(&instanceState{Version: 1, MaxEventSize: 128}, "approval")
	require.ErrorIs(t, err, ErrNoSuchEvent)
}

type concurrentCleanupHost struct {
	*fakeHost

	workerType string
	entered    chan struct{}
	release    chan struct{}
}

func (h *concurrentCleanupHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	if actorType == h.workerType {
		h.entered <- struct{}{}
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.fakeHost.ListJobs(ctx, actorType, actorID)
}

func TestPurgeRefusesUnresolvableLegacyChild(t *testing.T) {
	child, err := New("legacy-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	oldParent, err := New("legacy-parent", WithVersion(1), WithSteps(Child("sub", WithDefinition(child))))
	require.NoError(t, err)
	newParent, err := New("legacy-parent", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)

	host := newFakeHost()
	o := newTestOrchestrator(t, newParent, host, "parent-1")
	st := &instanceState{
		Workflow:     oldParent.name,
		Version:      oldParent.def.version,
		Status:       StatusCompleted,
		Compensation: CompensationNone,
		CompletedAt:  time.Now(),
		Steps: []stepRecord{{
			Name:   "sub",
			Kind:   KindChild,
			Status: StepCompleted,
			Tasks: []taskRecord{{
				Index:   0,
				Done:    true,
				ChildID: "child-1",
			}},
		}},
	}
	err = o.persist(t.Context(), st, time.Now())
	require.NoError(t, err)

	_, err = o.purge(t.Context())
	require.ErrorIs(t, err, ErrJournalIncompatible)

	var retained instanceState
	err = host.GetState(t.Context(), builtinActorType(newParent.baseType), "parent-1", &retained)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, retained.Status)
}

func TestPurgeRefusesUnresolvableLegacyWorker(t *testing.T) {
	oldWorkflow, err := New("legacy-worker", WithVersion(1), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	newWorkflow, err := New("legacy-worker", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)

	host := newFakeHost()
	o := newTestOrchestrator(t, newWorkflow, host, "instance-1")
	st := &instanceState{
		Workflow:     oldWorkflow.name,
		Version:      oldWorkflow.def.version,
		Status:       StatusCompleted,
		Compensation: CompensationNone,
		CompletedAt:  time.Now(),
		Steps: []stepRecord{{
			Name:   "work",
			Kind:   KindStep,
			Status: StepCompleted,
			Tasks:  []taskRecord{{Index: 0, Attempts: 1, Done: true}},
		}},
	}
	err = o.persist(t.Context(), st, time.Now())
	require.NoError(t, err)

	_, err = o.purge(t.Context())
	require.ErrorIs(t, err, ErrJournalIncompatible)
}

func TestPendingStatusDoesNotInventAVersion(t *testing.T) {
	wf, err := New("pending-version", WithVersion(2), WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, wf, host, "instance-1")

	_, _, err = host.Dispatch(t.Context(), builtinActorType(wf.baseType), "instance-1", methodStart, startPayload{Version: 1}, actor.JobProperties{})
	require.NoError(t, err)

	result, err := o.status(t.Context())
	require.NoError(t, err)
	status, ok := result.(statusResult)
	require.True(t, ok)
	require.True(t, status.Found)
	assert.Equal(t, StatusPending, status.Status.Status)
	assert.Zero(t, status.Status.Version)
}

func TestTaskMetadataOmitsActorsThatCannotReceiveJobs(t *testing.T) {
	child, err := New("task-metadata-child", WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	def := testDefinition(t, "task-metadata", WithSteps(
		Child("child", WithDefinition(child)),
		Step("plain", WithRun(noopRun)),
	))
	st := startJournal(t, def, time.Now())

	childTask := st.step("child").task(0)
	require.NotNil(t, childTask)
	assert.NotEmpty(t, childTask.ChildType)
	assert.Empty(t, childTask.WorkerType)
	assert.Empty(t, childTask.UndoType)

	apply(st, def, &event{kind: evDone, report: &reportPayload{Step: "child", Index: 0, Attempt: 1, ChildStatus: StatusCompleted}}, time.Now())
	advance(st, def, "instance-1", time.Now())
	plainTask := st.step("plain").task(0)
	require.NotNil(t, plainTask)
	assert.NotEmpty(t, plainTask.WorkerType)
	assert.Empty(t, plainTask.UndoType)
}

func TestPurgeOverlapsIndependentJobCleanup(t *testing.T) {
	wf, err := New("parallel-cleanup", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	base := newFakeHost()
	host := &concurrentCleanupHost{
		fakeHost:   base,
		workerType: builtinActorType(wf.workerType("")),
		entered:    make(chan struct{}, 4),
		release:    make(chan struct{}),
	}
	o := newReviewOrchestrator(t, wf, "instance-1", actor.NewService(host))
	st := &instanceState{
		Workflow: wf.name,
		Version:  wf.def.version,
		Status:   StatusCompleted,
		Steps: []stepRecord{{
			Name:   "work",
			Kind:   KindForEach,
			Status: StepCompleted,
			Tasks: []taskRecord{
				{Index: 0, WorkerType: wf.workerType(""), Done: true},
				{Index: 1, WorkerType: wf.workerType(""), Done: true},
				{Index: 2, WorkerType: wf.workerType(""), Done: true},
				{Index: 3, WorkerType: wf.workerType(""), Done: true},
			},
		}},
	}

	done := make(chan error, 1)
	go func() {
		done <- o.purgeJobs(t.Context(), st)
	}()

	for range 2 {
		select {
		case <-host.entered:
		case <-time.After(time.Second):
			t.Fatal("cleanup did not overlap independent job-list calls")
		}
	}
	close(host.release)
	err = <-done
	require.NoError(t, err)
}

func TestTerminationOverlapsIndependentJobCancellation(t *testing.T) {
	wf, err := New("parallel-cancellation", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	base := newFakeHost()
	host := &concurrentCleanupHost{
		fakeHost:   base,
		workerType: builtinActorType(wf.workerType("")),
		entered:    make(chan struct{}, 4),
		release:    make(chan struct{}),
	}
	o := newReviewOrchestrator(t, wf, "instance-1", actor.NewService(host))
	st := &instanceState{
		Workflow: wf.name,
		Version:  wf.def.version,
		Status:   StatusFailed,
		Steps: []stepRecord{{
			Name:   "work",
			Kind:   KindForEach,
			Status: StepFailed,
			Tasks: []taskRecord{
				{Index: 0, WorkerType: wf.workerType(""), Done: true, Abandoned: true},
				{Index: 1, WorkerType: wf.workerType(""), Done: true, Abandoned: true},
				{Index: 2, WorkerType: wf.workerType(""), Done: true, Abandoned: true},
				{Index: 3, WorkerType: wf.workerType(""), Done: true, Abandoned: true},
			},
		}},
	}

	done := make(chan error, 1)
	go func() {
		done <- o.cancelAllOutstanding(t.Context(), st)
	}()

	for range 2 {
		select {
		case <-host.entered:
		case <-time.After(time.Second):
			t.Fatal("termination did not overlap independent job-list calls")
		}
	}
	close(host.release)
	err = <-done
	require.NoError(t, err)
}

func TestUnknownHostUsesJournaledDeadlinePolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  UnknownVersionPolicy
		timeout time.Duration
		status  Status
		want    Status
	}{
		{name: "park survives a host configured to fail", policy: ParkUnknownVersion, timeout: time.Minute, status: StatusRunning, want: StatusRunning},
		{name: "original timeout prevents an early failure", policy: FailUnknownVersion, timeout: time.Hour, status: StatusRunning, want: StatusRunning},
		{name: "original failure policy survives a host configured to park", policy: FailUnknownVersion, timeout: time.Minute, status: StatusRunning, want: StatusFailed},
		{name: "legacy unknown policy parks conservatively", status: StatusRunning, want: StatusRunning},
		{name: "suspended instance stays paused", policy: FailUnknownVersion, timeout: time.Minute, status: StatusSuspended, want: StatusSuspended},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostPolicy := FailUnknownVersion
			if tc.want == StatusFailed {
				hostPolicy = ParkUnknownVersion
			}
			wf, err := New("unknown-policy", WithVersion(2), WithTimeout(time.Minute), WithUnknownVersionPolicy(hostPolicy), WithSteps(Step("replacement", WithRun(noopRun))))
			require.NoError(t, err)
			h := newFakeHost()
			o := newTestOrchestrator(t, wf, h, "inst-1")
			st := instanceState{Workflow: wf.name, Version: 1, Status: tc.status, StartedAt: time.Now().Add(-2 * time.Minute), Timeout: tc.timeout, UnknownVersion: tc.policy}
			err = o.persist(t.Context(), &st, time.Now())
			require.NoError(t, err)
			err = o.runDeadline(t.Context())
			require.NoError(t, err)
			result := readJournal(t, h, wf, "inst-1")
			assert.Equal(t, tc.want, result.Status)
			assert.Equal(t, tc.timeout, result.Timeout)
			assert.Equal(t, tc.policy, result.UnknownVersion)
		})
	}
}

func TestUnknownVersionChildReportsFailureBeforeDroppingDeadline(t *testing.T) {
	wf, err := New("unknown-child", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	o := newTestOrchestrator(t, wf, h, "child-1")
	st := instanceState{
		Workflow: wf.name, Version: 1, Status: StatusRunning,
		DefinitionFingerprint: "original-child-graph", RegistryGeneration: 1,
		StartedAt: time.Now().Add(-2 * time.Minute), Timeout: time.Minute, UnknownVersion: FailUnknownVersion,
		Parent: childOf("parent-1"),
	}
	err = o.persist(t.Context(), &st, time.Now())
	require.NoError(t, err)
	err = o.client.SetAlarm(t.Context(), alarmDeadline, deadlineAlarmProperties(time.Now()))
	require.NoError(t, err)

	// An unavailable parent transport keeps the terminal journal and recovery alarm until the failure report can be delivered
	h.failDispatch = true
	err = o.runDeadline(t.Context())
	require.Error(t, err)
	result := readJournal(t, h, wf, "child-1")
	assert.Equal(t, StatusFailed, result.Status)
	assert.True(t, result.RegistryConfirmed)
	assert.False(t, result.Reported)
	_, armed := alarmDue(t, h, wf, "child-1")
	assert.True(t, armed)

	h.failDispatch = false
	err = o.runDeadline(t.Context())
	require.NoError(t, err)
	result = readJournal(t, h, wf, "child-1")
	assert.True(t, result.Reported)
	assert.Equal(t, []string{methodDone}, reportsToParent(t, h, "parent-1"))
	_, armed = alarmDue(t, h, wf, "child-1")
	assert.False(t, armed)
}

func TestSweepRetainsDescendantsUntilTheirRootTerminates(t *testing.T) {
	leafWF, err := New("leaf", WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)
	middleWF, err := New("middle", WithSteps(Child("leaf", WithDefinition(leafWF))))
	require.NoError(t, err)
	rootWF, err := New("root", WithSteps(Child("middle", WithDefinition(middleWF)), WaitForEvent("approval")))
	require.NoError(t, err)
	host := newFakeHost()
	root := newTestOrchestrator(t, rootWF, host, "root")
	middleID := workerActorID("root", "middle", 0)
	leafID := workerActorID(middleID, "leaf", 0)
	middle := newTestOrchestrator(t, middleWF, host, middleID)
	leaf := newTestOrchestrator(t, leafWF, host, leafID)

	// Complete both descendant workflows while the root remains open for a later external decision
	err = root.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	deliverInstanceJob(t, host, middleWF, middleID, methodStart, middle)
	deliverInstanceJob(t, host, leafWF, leafID, methodStart, leaf)
	err = leaf.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)
	deliverInstanceJob(t, host, middleWF, middleID, methodDone, middle)
	deliverInstanceJob(t, host, rootWF, "root", methodDone, root)
	require.Equal(t, StatusRunning, readJournal(t, host, rootWF, "root").Status)
	require.Equal(t, StatusCompleted, readJournal(t, host, middleWF, middleID).Status)
	leafState := readJournal(t, host, leafWF, leafID)
	leafState.CompletedAt = time.Now().Add(-3 * defaultRetention)
	err = leaf.client.SetState(t.Context(), leafState, nil)
	require.NoError(t, err)

	// The leaf sweep must follow its completed parent to the still-active root before deciding it can purge
	sweepHost := &reviewAPIHost{fakeHost: host, workflows: map[string]*Workflow{builtinActorType(leafWF.baseType): leafWF}}
	removed, err := leafWF.Service(actor.NewService(sweepHost)).PurgeTerminated(t.Context())
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Equal(t, StatusCompleted, readJournal(t, host, leafWF, leafID).Status)

	// Retained descendants must still be able to undo their effects when the root is subsequently cancelled
	err = root.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "approval withdrawn"}})
	require.NoError(t, err)
	deliverInstanceJob(t, host, middleWF, middleID, methodUnwind, middle)
	deliverInstanceJob(t, host, leafWF, leafID, methodUnwind, leaf)
	require.NotEmpty(t, host.jobIDFor(builtinActorType(leafWF.undoType("")), workerActorID(leafID, "effect", 0), methodCompensate))
	err = leaf.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)
	deliverInstanceJob(t, host, middleWF, middleID, methodCompensated, middle)
	deliverInstanceJob(t, host, rootWF, "root", methodCompensated, root)
	rootState := readJournal(t, host, rootWF, "root")
	require.Equal(t, StatusCancelled, rootState.Status)
	require.Equal(t, CompensationCompleted, rootState.Compensation)

	// Once the root terminates, the same retained chain no longer prevents collection
	active, err := leaf.parentStillRunning(t.Context(), leafState.Parent)
	require.NoError(t, err)
	require.False(t, active)
}

func TestPurgeRefusesCyclicAncestry(t *testing.T) {
	host := newFakeHost()
	wf, err := New("cycle", WithSteps(Step("effect", WithRun(noopRun))))
	require.NoError(t, err)
	st := instanceState{Status: StatusCompleted, Parent: &parentRef{Workflow: wf.name, InstanceID: "cycle"}}
	err = host.SetState(t.Context(), builtinActorType(wf.baseType), "cycle", st, nil)
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "cycle")
	_, err = o.purge(t.Context())
	require.ErrorContains(t, err, "ancestry contains a cycle")
	require.Equal(t, StatusCompleted, readJournal(t, host, wf, "cycle").Status)
}

type deleteStateFailingHost struct {
	*fakeHost

	deleteErr error
}

func (h *deleteStateFailingHost) DeleteState(ctx context.Context, actorType string, actorID string) error {
	if h.deleteErr != nil {
		return h.deleteErr
	}
	return h.fakeHost.DeleteState(ctx, actorType, actorID)
}

func TestPurgeRetriesAFailedStateDeletion(t *testing.T) {
	host := &deleteStateFailingHost{fakeHost: newFakeHost()}
	wf, err := New("purge-retry", WithSteps(Step("effect", WithRun(noopRun))))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)

	// A provider failure must leave both the durable journal and the activation's retryable snapshot intact
	host.deleteErr = errors.New("injected deletion failure")
	_, err = o.purge(t.Context())
	require.ErrorIs(t, err, host.deleteErr)
	require.Equal(t, StatusCompleted, readJournal(t, host.fakeHost, wf, "instance").Status)
	host.deleteErr = nil
	result, err := o.purge(t.Context())
	require.NoError(t, err)
	require.Equal(t, purgeResult{Found: true}, result)
	var st instanceState
	err = host.GetState(t.Context(), builtinActorType(wf.baseType), "instance", &st)
	require.ErrorIs(t, err, actor.ErrStateNotFound)
}

type undoCleanupHost struct {
	*fakeHost

	failActorType string
}

func (h *undoCleanupHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	if actorType == h.failActorType {
		return nil, errors.New("injected undo cleanup failure")
	}
	return h.fakeHost.ListJobs(ctx, actorType, actorID)
}
