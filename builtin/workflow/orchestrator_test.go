package workflow

import (
	"encoding/json"
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

	declining, err := New("parked", WithSteps(Step("a", WithRun(noopRun))))
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

	serving, err := New("abandoned", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	o := newTestOrchestrator(t, serving, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// The declining host is the one configured to give up, and it only does so once the instance's own timeout has elapsed
	declining, err := New("abandoned",
		WithTimeout(time.Minute),
		WithUnknownVersionPolicy(FailUnknownVersion),
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
	seedJournal := func(t *testing.T, host *fakeHost) {
		t.Helper()

		old, err := New("version-drift", WithSteps(Step("a", WithRun(noopRun))))
		require.NoError(t, err)
		o := newTestOrchestrator(t, old, host, "inst-1")
		require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
		require.Equal(t, 1, readJournal(t, host, old, "inst-1").Version)
	}

	t.Run("parking waits for a host that can serve the version", func(t *testing.T) {
		host := newFakeHost()
		seedJournal(t, host)

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
		seedJournal(t, host)

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
