package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
)

// fakeHost is an in-memory actor.Host that records what a turn does, so the engine's own behavior can be driven and inspected without a cluster
//
// It is deliberately not a working actor runtime: nothing is delivered, and a dispatched job is simply recorded
// That is why it is useful for the invariants: a test can decide exactly which operation fails and when
type fakeHost struct {
	mu sync.Mutex

	// state holds each actor's encoded state, so a read sees exactly what a write stored
	state map[string][]byte
	// labels holds the labels written alongside each actor's state
	labels map[string]map[string]string
	// alarms records the alarms currently set
	alarms map[string]actor.AlarmProperties
	// jobs holds the jobs dispatched, keyed by job ID
	jobs map[string]actor.JobInfo
	// jobPayloads holds each job's input, which a test uses to assert on what was dispatched
	jobPayloads map[string]any
	// liveKeys maps an actor's idempotency key to the live job holding it, which is how Francis deduplicates a re-dispatch
	liveKeys map[string]string
	// nextID numbers the dispatched jobs
	nextID int

	// failDispatch makes the next Dispatch fail, which is how a fault is injected between the state write and the scheduling half of a turn
	failDispatch bool
	// invokes records every synchronous invocation, which the boundary test asserts on
	invokes []string
	// panicOnInvoke makes any invocation other than the registry's own check a test failure
	panicOnInvoke bool
	// registryResponse is what the registry's consistency check answers
	registryResponse registerResponse
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		state:            map[string][]byte{},
		labels:           map[string]map[string]string{},
		alarms:           map[string]actor.AlarmProperties{},
		jobs:             map[string]actor.JobInfo{},
		jobPayloads:      map[string]any{},
		liveKeys:         map[string]string{},
		registryResponse: registerResponse{OK: true},
	}
}

// key joins an actor reference the way the fake host stores it
func key(parts ...string) string {
	return strings.Join(parts, "/")
}

func (f *fakeHost) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	f.mu.Lock()
	f.invokes = append(f.invokes, actorType+"/"+method)
	panicOnInvoke := f.panicOnInvoke
	resp := f.registryResponse
	f.mu.Unlock()

	// The engine makes exactly one synchronous call from a turn: the cached definition-registry check
	if method == methodRegister {
		return &fakeEnvelope{value: resp}, nil
	}

	if panicOnInvoke {
		panic("the Workflow actor reached past the orchestration boundary: invoke " + actorType + "/" + method)
	}
	return &fakeEnvelope{}, nil
}

func (f *fakeHost) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	f.mu.Lock()
	f.invokes = append(f.invokes, actorType+"/"+method)
	panicOnInvoke := f.panicOnInvoke
	f.mu.Unlock()

	if panicOnInvoke {
		panic("the Workflow actor reached past the orchestration boundary: peek " + actorType + "/" + method)
	}
	return &fakeEnvelope{}, nil
}

func (f *fakeHost) InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...actor.InvokeOption) (string, io.ReadCloser, error) {
	panic("the Workflow actor reached past the orchestration boundary: invoke stream")
}

func (f *fakeHost) PeekStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...actor.InvokeOption) (string, io.ReadCloser, error) {
	panic("the Workflow actor reached past the orchestration boundary: peek stream")
}

func (f *fakeHost) HaltAll() error                         { return nil }
func (f *fakeHost) Halt(string, string) error              { return nil }
func (f *fakeHost) HaltDeferred(actorType, actorID string) {}

func (f *fakeHost) GetAlarm(ctx context.Context, actorType string, actorID string, name string) (actor.AlarmProperties, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	props, ok := f.alarms[key(actorType, actorID, name)]
	if !ok {
		return actor.AlarmProperties{}, actor.ErrAlarmNotFound
	}
	return props, nil
}

func (f *fakeHost) SetAlarm(ctx context.Context, actorType string, actorID string, name string, props actor.AlarmProperties) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.alarms[key(actorType, actorID, name)] = props
	return nil
}

func (f *fakeHost) DeleteAlarm(ctx context.Context, actorType string, actorID string, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := key(actorType, actorID, name)
	_, ok := f.alarms[k]
	if !ok {
		return actor.ErrAlarmNotFound
	}
	delete(f.alarms, k)
	return nil
}

func (f *fakeHost) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, props actor.JobProperties) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failDispatch {
		return "", errors.New("injected dispatch failure")
	}

	// Francis deduplicates an idempotency key against live rows only, so re-dispatching a pending task is a no-op
	if props.IdempotencyKey != "" {
		k := key(actorType, actorID, props.IdempotencyKey)
		existing, ok := f.liveKeys[k]
		if ok {
			return existing, nil
		}
		defer func() { f.liveKeys[k] = fmt.Sprintf("job-%d", f.nextID) }()
	}

	f.nextID++
	jobID := fmt.Sprintf("job-%d", f.nextID)
	f.jobs[jobID] = actor.JobInfo{
		JobID:     jobID,
		ActorType: actorType,
		ActorID:   actorID,
		Method:    method,
		Status:    actor.JobStatusPending,
		DueTime:   props.EffectiveDueTime(time.Now()),
		CreatedAt: time.Now(),
	}
	f.jobPayloads[jobID] = data
	return jobID, nil
}

func (f *fakeHost) GetJob(ctx context.Context, jobID string) (actor.JobInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	j, ok := f.jobs[jobID]
	if !ok {
		return actor.JobInfo{}, actor.ErrJobNotFound
	}
	return j, nil
}

func (f *fakeHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []actor.JobInfo
	for _, j := range f.jobs {
		if j.ActorType == actorType && j.ActorID == actorID {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeHost) RetryJob(ctx context.Context, jobID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	j, ok := f.jobs[jobID]
	if !ok {
		return "", actor.ErrJobNotFound
	}

	f.removeJobLocked(jobID)
	f.nextID++
	newID := fmt.Sprintf("job-%d", f.nextID)
	j.JobID = newID
	j.Status = actor.JobStatusPending
	f.jobs[newID] = j
	return newID, nil
}

func (f *fakeHost) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.jobs[jobID]
	if !ok {
		return actor.ErrJobNotFound
	}
	f.removeJobLocked(jobID)
	return nil
}

// removeJobLocked drops a job and frees the idempotency key it held, the way completing or dead-lettering one does
func (f *fakeHost) removeJobLocked(jobID string) {
	delete(f.jobs, jobID)
	delete(f.jobPayloads, jobID)
	for k, id := range f.liveKeys {
		if id == jobID {
			delete(f.liveKeys, k)
		}
	}
}

func (f *fakeHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	enc, err := msgpack.Marshal(state)
	if err != nil {
		return err
	}

	f.state[key(actorType, actorID)] = enc
	if opts != nil {
		f.labels[key(actorType, actorID)] = opts.Labels
	}
	return nil
}

func (f *fakeHost) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	enc, ok := f.state[key(actorType, actorID)]
	if !ok {
		return actor.ErrStateNotFound
	}
	return msgpack.Unmarshal(enc, dest)
}

func (f *fakeHost) DeleteState(ctx context.Context, actorType string, actorID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.state, key(actorType, actorID))
	delete(f.labels, key(actorType, actorID))
	return nil
}

func (f *fakeHost) ListStates(ctx context.Context, actorType string, opts *actor.ListStatesOpts) (actor.StateList, error) {
	return actor.StateList{}, nil
}

// dispatchedTo returns the methods dispatched to one actor, so a test can assert on what a turn scheduled
func (f *fakeHost) dispatchedTo(actorType string, actorID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []string
	for _, j := range f.jobs {
		if j.ActorType == actorType && j.ActorID == actorID {
			out = append(out, j.Method)
		}
	}
	return out
}

// deadLetter marks a job dead-lettered, which frees its idempotency key the way the real store does
func (f *fakeHost) deadLetter(jobID string, lastError string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	j, ok := f.jobs[jobID]
	if !ok {
		return
	}

	j.Status = actor.JobStatusDeadLettered
	j.LastError = lastError
	j.Attempts = 5
	f.jobs[jobID] = j

	for k, id := range f.liveKeys {
		if id == jobID {
			delete(f.liveKeys, k)
		}
	}
}

// jobIDFor returns the ID of the one job dispatched to an actor with a given method
func (f *fakeHost) jobIDFor(actorType string, actorID string, method string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	for id, j := range f.jobs {
		if j.ActorType == actorType && j.ActorID == actorID && j.Method == method {
			return id
		}
	}
	return ""
}

// fakeEnvelope carries a value back from the fake host the way a real invocation response does
type fakeEnvelope struct {
	value any
}

func (e *fakeEnvelope) Decode(into any) error {
	if e.value == nil {
		return nil
	}

	enc, err := msgpack.Marshal(e.value)
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(enc, into)
}

// payloadEnvelope hands a job's payload to a turn the way the framework delivers one
type payloadEnvelope struct {
	value any
}

func (e *payloadEnvelope) Decode(into any) error {
	if e.value == nil {
		return nil
	}

	enc, err := msgpack.Marshal(e.value)
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(enc, into)
}

// newTestOrchestrator builds a Workflow actor over the fake host, which is what every engine test drives
func newTestOrchestrator(t *testing.T, wf *Workflow, host *fakeHost, instanceID string) *orchestrator {
	t.Helper()

	svc := actor.NewService(host)
	o, ok := newOrchestrator(wf, instanceID, svc).(*orchestrator)
	require.True(t, ok, "the workflow factory should build an orchestrator")
	return o
}

// readJournal decodes the journal the fake host holds for an instance
func readJournal(t *testing.T, host *fakeHost, wf *Workflow, instanceID string) instanceState {
	t.Helper()

	var st instanceState
	err := host.GetState(t.Context(), ref.BuiltInActorTypePrefix+wf.baseType, instanceID, &st)
	require.NoError(t, err)
	return st
}

// TestWorkflowTurnStaysWithinTheOrchestrationBoundary drives a Workflow actor against a transport that panics on anything but state, alarm, and job operations
//
// The one call the engine is allowed to make from a turn is the cached definition-registry check, and nothing else may reach past the boundary (§4.3)
func TestWorkflowTurnStaysWithinTheOrchestrationBoundary(t *testing.T) {
	host := newFakeHost()
	host.panicOnInvoke = true

	wf, err := New("boundary", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	// Starting, reporting, and terminating all run through the same four phases, and none of them may reach past the boundary
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"one"`)}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "b", Index: 0, Attempt: 1, Output: json.RawMessage(`"two"`)}}))

	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusCompleted, st.Status)

	// The registry check is the only invocation, and it happens at most once per version for the life of the process
	host.mu.Lock()
	defer host.mu.Unlock()
	require.Len(t, host.invokes, 1)
	assert.Contains(t, host.invokes[0], methodRegister)
}

// TestTurnConvergesAfterAFaultBetweenTheStateWriteAndTheDispatch injects a failure between SetState and reconcile, and asserts the retried turn converges without double-counting
//
// This is ordering invariant 2 (§7.3): a turn that records nothing still schedules, because the guard is whether the outcome is already recorded and never whether this delivery has been seen before
func TestTurnConvergesAfterAFaultBetweenTheStateWriteAndTheDispatch(t *testing.T) {
	host := newFakeHost()

	wf, err := New("fault-injection", WithSteps(
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	workerType := ref.BuiltInActorTypePrefix + wf.workerType("")
	require.Equal(t, []string{methodRun}, host.dispatchedTo(workerType, workerActorID("inst-1", "a", 0)))

	// The report is delivered, and the dispatch of the next step is made to fail after the journal has already recorded it
	report := &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1, Output: json.RawMessage(`"one"`)}}
	host.failDispatch = true
	err = o.Job(t.Context(), methodDone, report)
	require.Error(t, err)

	// The journal is durable before anything is scheduled, so the result is recorded even though nothing was scheduled
	st := readJournal(t, host, wf, "inst-1")
	require.True(t, st.step("a").task(0).Done)
	assert.Equal(t, StepRunning, st.step("b").Status)
	assert.Empty(t, host.dispatchedTo(ref.BuiltInActorTypePrefix+wf.workerType(""), workerActorID("inst-1", "b", 0)))

	// Francis redelivers the same job occurrence, and this time apply sees an event the journal already reflects
	host.failDispatch = false
	require.NoError(t, o.Job(t.Context(), methodDone, report))

	// The duplicate recorded nothing, and yet the next step was scheduled: without that the instance would wait forever for a step nobody dispatched
	assert.Equal(t, []string{methodRun}, host.dispatchedTo(workerType, workerActorID("inst-1", "b", 0)))

	// And nothing was counted twice
	st = readJournal(t, host, wf, "inst-1")
	assert.Equal(t, 1, st.step("a").task(0).Attempts)
	assert.Equal(t, 1, st.step("b").task(0).Attempts)
	assert.JSONEq(t, `"one"`, string(st.step("a").task(0).Output))
}

// TestATransportFailureIsFoldedAsOneRetryableAttempt drives the report a worker sends when its own run job dead-letters, which is the engine's whole recovery path for a task that ran and could not say so
//
// A dead-lettered job frees its idempotency key, so the next attempt is dispatched under a key of its own rather than coalescing onto the one that died
func TestATransportFailureIsFoldedAsOneRetryableAttempt(t *testing.T) {
	host := newFakeHost()

	wf, err := New("deadletter", WithSteps(
		Step("a", WithRun(noopRun), WithMaxAttempts(3), WithRetryBackoff(time.Millisecond, time.Millisecond)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	workerType := ref.BuiltInActorTypePrefix + wf.workerType("")
	workerID := workerActorID("inst-1", "a", 0)

	// The worker's job dead-letters, which is what happens when a task ran but could not report
	runJob := host.jobIDFor(workerType, workerID, methodRun)
	require.NotEmpty(t, runJob)
	host.deadLetter(runJob, "could not reach the orchestrator")

	// The worker's JobFailed hook sends exactly this, and the orchestrator folds it as one failed attempt of transport kind
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{
		Step: "a", Index: 0, Attempt: 1,
		Error:     "task could not report its outcome: could not reach the orchestrator",
		Retryable: true,
		Transport: true,
	}}))

	st := readJournal(t, host, wf, "inst-1")
	tr := st.step("a").task(0)
	assert.Equal(t, 2, tr.Attempts, "the lost report is accounted for as one failed attempt")
	assert.False(t, tr.Done)
	assert.Contains(t, tr.LastError, "could not report")

	// The work was re-driven rather than left waiting on the key the dead job freed, and the dead record stays put until the instance is purged
	assert.Equal(t, []string{methodRun, methodRun}, host.dispatchedTo(workerType, workerID))
	dead, err := host.GetJob(t.Context(), runJob)
	require.NoError(t, err)
	assert.Equal(t, actor.JobStatusDeadLettered, dead.Status)

	// The same report arriving twice is a duplicate of an attempt already accounted for, and must not cost a second one
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{
		Step: "a", Index: 0, Attempt: 1, Error: "task could not report its outcome", Retryable: true, Transport: true,
	}}))
	st = readJournal(t, host, wf, "inst-1")
	assert.Equal(t, 2, st.step("a").task(0).Attempts)
}

// TestADeadLetteredReportArmsTheDeadline verifies the instance's own JobFailed hook forces a turn, which is what re-dispatches whatever the journal still says is outstanding
func TestADeadLetteredReportArmsTheDeadline(t *testing.T) {
	host := newFakeHost()

	wf, err := New("nudge", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	instanceType := ref.BuiltInActorTypePrefix + wf.baseType
	require.NoError(t, o.JobFailed(t.Context(), "job-1", methodDone, nil, errors.New("could not deliver")))

	host.mu.Lock()
	armed, ok := host.alarms[key(instanceType, "inst-1", alarmDeadline)]
	host.mu.Unlock()
	require.True(t, ok, "a lost report arms the deadline so a turn runs")
	assert.False(t, armed.DueTime.After(time.Now()), "the deadline is armed to fire at once")
}

// TestReconcileDoesNotRedispatchATaskThatIsAlreadyPending verifies a repeated turn coalesces on the task's idempotency key rather than queueing a second copy of the same work
func TestReconcileDoesNotRedispatchATaskThatIsAlreadyPending(t *testing.T) {
	host := newFakeHost()

	wf, err := New("coalesce", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	workerType := ref.BuiltInActorTypePrefix + wf.workerType("")
	workerID := workerActorID("inst-1", "a", 0)
	require.Len(t, host.dispatchedTo(workerType, workerID), 1)

	// Re-running advance and reconcile is safe to do at any time precisely because the dispatch coalesces
	for range 3 {
		require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))
	}
	assert.Len(t, host.dispatchedTo(workerType, workerID), 1)
}

// TestFanOutWindowAdmitsTasksInIndexOrder verifies WithMaxParallel bounds how many of a fan-out's tasks are in flight per instance, and that the window slides as results arrive
func TestFanOutWindowAdmitsTasksInIndexOrder(t *testing.T) {
	host := newFakeHost()

	wf, err := New("window", WithSteps(
		Step("plan", WithRun(noopRun)),
		ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithMaxParallel(2), WithFailurePolicy(TolerateFailures)),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "plan", Index: 0, Attempt: 1, Output: json.RawMessage(`[1,2,3,4]`)}}))

	workerType := ref.BuiltInActorTypePrefix + wf.workerType("")
	assert.Len(t, host.dispatchedTo(workerType, workerActorID("inst-1", "work", 0)), 1)
	assert.Len(t, host.dispatchedTo(workerType, workerActorID("inst-1", "work", 1)), 1)
	assert.Empty(t, host.dispatchedTo(workerType, workerActorID("inst-1", "work", 2)), "the window admits only the first two tasks")

	// As a result arrives the window slides, admitting the next task in index order
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Index: 0, Attempt: 1, Output: json.RawMessage(`1`)}}))
	assert.Len(t, host.dispatchedTo(workerType, workerActorID("inst-1", "work", 2)), 1)
	assert.Empty(t, host.dispatchedTo(workerType, workerActorID("inst-1", "work", 3)))
}

// TestTurnWritesTheStatusLabelsWithTheJournal verifies the listing index can never disagree with the journal, because both are written in the same operation
func TestTurnWritesTheStatusLabelsWithTheJournal(t *testing.T) {
	host := newFakeHost()

	wf, err := New("labels", WithVersion(4), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 4}}))

	host.mu.Lock()
	labels := host.labels[key(ref.BuiltInActorTypePrefix+wf.baseType, "inst-1")]
	host.mu.Unlock()

	assert.Equal(t, string(StatusRunning), labels[labelStatus])
	assert.Equal(t, "4", labels[labelVersion])

	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1}}))

	host.mu.Lock()
	labels = host.labels[key(ref.BuiltInActorTypePrefix+wf.baseType, "inst-1")]
	host.mu.Unlock()

	assert.Equal(t, string(StatusCompleted), labels[labelStatus])
}

// TestAHostWithoutTheInstanceVersionDeclinesTheJob verifies an old instance is left for a host that can serve it, without counting an attempt and without dead-lettering
func TestAHostWithoutTheInstanceVersionDeclinesTheJob(t *testing.T) {
	host := newFakeHost()

	v1, err := New("versioned", WithVersion(1), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, v1, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	// A second host runs version 2 of the same workflow, and its actor is handed the version 1 instance
	v2, err := New("versioned", WithVersion(2), WithSteps(Step("a", WithRun(noopRun)), Step("b", WithRun(noopRun))))
	require.NoError(t, err)

	newHostActor := newTestOrchestrator(t, v2, host, "inst-1")
	err = newHostActor.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1}})
	require.ErrorIs(t, err, actor.ErrJobRejected)

	// Nothing was recorded, so the instance is untouched and waits for a host that still has its version
	st := readJournal(t, host, v1, "inst-1")
	assert.Equal(t, 1, st.Version)
	assert.False(t, st.step("a").task(0).Done)
}

// TestAConflictingDefinitionDeclinesEveryJobOfTheVersion verifies a host whose graph disagrees with the registered one hands its work to hosts whose code matches
func TestAConflictingDefinitionDeclinesEveryJobOfTheVersion(t *testing.T) {
	host := newFakeHost()
	host.registryResponse = registerResponse{OK: false, Fingerprint: "someone-else's-graph", FirstSeenAt: time.Now()}

	wf, err := New("conflicting", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.ErrorIs(t, err, actor.ErrJobRejected)

	// Nothing was written, because the turn never ran
	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Empty(t, host.state)
}

// TestAControlJobWaitsForTheStartJob verifies a suspend that raced ahead of the start job waits rather than inventing an instance out of nothing
func TestAControlJobWaitsForTheStartJob(t *testing.T) {
	host := newFakeHost()

	wf, err := New("racy", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")

	err = o.Job(t.Context(), methodSuspend, &payloadEnvelope{value: reasonPayload{Reason: "too early"}})
	require.ErrorIs(t, err, errWaitingForStart)

	// The journal is created by the start job alone, so nothing was written
	host.mu.Lock()
	empty := len(host.state) == 0
	host.mu.Unlock()
	assert.True(t, empty)

	// Once the instance exists, the retried control job lands
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodSuspend, &payloadEnvelope{value: reasonPayload{Reason: "now"}}))

	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StatusSuspended, st.Status)
}

// TestTerminationDropsTheTimers verifies a terminated instance stops holding its deadline
func TestTerminationDropsTheTimers(t *testing.T) {
	host := newFakeHost()

	wf, err := New("timers", WithTimeout(time.Hour), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	instanceType := ref.BuiltInActorTypePrefix + wf.baseType
	host.mu.Lock()
	_, armed := host.alarms[key(instanceType, "inst-1", alarmDeadline)]
	host.mu.Unlock()
	assert.True(t, armed, "a running instance holds its deadline")

	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1}}))

	host.mu.Lock()
	_, stillArmed := host.alarms[key(instanceType, "inst-1", alarmDeadline)]
	host.mu.Unlock()
	assert.False(t, stillArmed)
}

// TestTheDeadlineFailsTheStepItHit verifies what a timeout costs depends on the step it elapsed on, which the per-step policies decide exactly as a handler failure would
func TestTheDeadlineFailsTheStepItHit(t *testing.T) {
	host := newFakeHost()

	wf, err := New("deadline",
		WithTimeout(time.Hour),
		WithSteps(
			Step("slow", WithRun(noopRun), WithStepTimeout(time.Nanosecond), WithMaxAttempts(1), WithOptional()),
			Step("after", WithRun(noopRun)),
		),
	)
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))

	time.Sleep(time.Millisecond)
	require.NoError(t, o.Alarm(t.Context(), alarmDeadline, nil))

	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StepFailed, st.step("slow").Status)
	assert.Contains(t, st.step("slow").Error, "timed out")

	// The step declared its failure optional, so the run carries on rather than unwinding
	assert.Equal(t, StepRunning, st.step("after").Status)
	assert.Equal(t, StatusRunning, st.Status)
}

// TestWirePayloadsSurviveAGenericDecode guards a whole class of bug: every value the engine sends between actors is
// msgpack-encoded, and a response that crosses hosts is decoded into an interface before being decoded into its type
//
// A map with non-string keys does not survive that round-trip — it decodes to nothing and silently re-encodes as empty —
// so every wire shape is checked here rather than only where a cluster happens to be multi-host
func TestWirePayloadsSurviveAGenericDecode(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name  string
		value any
		into  func() any
	}{
		{
			name: "start",
			value: startPayload{
				Input:       json.RawMessage(`{"a":1}`),
				Version:     3,
				Parent:      &parentRef{InstanceID: "p", Workflow: "w", Step: "s", Index: 2, Depth: 1, Attempt: 1},
				TraceParent: "00-abc-def-01",
				CreatedAt:   now,
				Attempt:     1,
			},
			into: func() any { return &startPayload{} },
		},
		{
			name: "run",
			value: runPayload{
				InstanceID: "i", Workflow: "w", Version: 1, Step: "s", Index: 0, Positional: true, Attempt: 2,
				Handler:          "member",
				Input:            json.RawMessage(`{"a":1}`),
				Item:             json.RawMessage(`2`),
				Outputs:          map[string]json.RawMessage{"prev": json.RawMessage(`"out"`)},
				Skipped:          []string{"gone"},
				Result:           json.RawMessage(`"res"`),
				Cause:            "because",
				OrchestratorType: "workflow.w", MaxOutputSize: 1024, TraceParent: "tp",
			},
			into: func() any { return &runPayload{} },
		},
		{
			name: "report",
			value: reportPayload{
				Step: "s", Index: 1, Attempt: 2,
				Output: json.RawMessage(`"o"`), Error: "e", Retryable: true, Transport: true,
				ChildStatus: StatusFailed, ChildCompensation: CompensationPartial, TraceParent: "tp",
			},
			into: func() any { return &reportPayload{} },
		},
		{
			name:  "compensation report",
			value: compReportPayload{Step: "s", Index: 1, Attempt: 3, Error: "e", Retryable: true, Transport: true, TraceParent: "tp"},
			into:  func() any { return &compReportPayload{} },
		},
		{
			name:  "event",
			value: eventPayload{Name: "approval", Payload: json.RawMessage(`{"by":"ops"}`)},
			into:  func() any { return &eventPayload{} },
		},
		{
			name:  "reason",
			value: reasonPayload{Reason: "cancelled", FromParent: true, CompAttempt: 2},
			into:  func() any { return &reasonPayload{} },
		},
		{
			name:  "registry register",
			value: registerRequest{Version: 4, Fingerprint: "abc"},
			into:  func() any { return &registerRequest{} },
		},
		{
			name:  "registry response",
			value: registerResponse{OK: true, Fingerprint: "abc", FirstSeenAt: now},
			into:  func() any { return &registerResponse{} },
		},
		{
			name:  "registry state",
			value: registryState{Versions: []registryEntry{{Version: 4, Fingerprint: "abc", FirstSeenAt: now}}},
			into:  func() any { return &registryState{} },
		},
		{
			name:  "definitions response",
			value: definitionsResponse{Entries: []registryEntry{{Version: 4, Fingerprint: "abc", FirstSeenAt: now}}},
			into:  func() any { return &definitionsResponse{} },
		},
		{
			name:  "forget request",
			value: forgetRequest{Version: 4},
			into:  func() any { return &forgetRequest{} },
		},
		{
			name:  "purge result",
			value: purgeResult{Found: true, Active: true},
			into:  func() any { return &purgeResult{} },
		},
		{
			name: "status result",
			value: statusResult{Found: true, Status: InstanceStatus{
				InstanceID: "i", Workflow: "w", Version: 2, Status: StatusRunning, CurrentStep: "s",
				Steps:       []StepStatusView{{Name: "s", Kind: KindStep, Status: StepRunning, Tasks: 1, Attempts: 1, ChildIDs: []string{"c"}}},
				Suspended:   &SuspendView{At: now, Reason: "why", ResumeTo: StatusRunning},
				Parent:      &ParentView{InstanceID: "p", Workflow: "w", Step: "s", Index: 1, Depth: 1},
				CreatedAt:   now,
				StartedAt:   now,
				CompletedAt: now,
			}},
			into: func() any { return &statusResult{} },
		},
		{
			name: "journal",
			value: instanceState{
				Workflow: "w", Version: 1, Status: StatusRunning,
				Input:  json.RawMessage(`{"a":1}`),
				Cursor: "s",
				Steps: []stepRecord{{
					Name: "s", Kind: KindForEach, Status: StepRunning, Remaining: 1,
					Tasks: []taskRecord{{Index: 0, Item: json.RawMessage(`1`), Attempts: 2, Comp: &compRecord{Attempts: 1}}},
				}},
				Stack:     []string{"s"},
				CreatedAt: now, StartedAt: now,
			},
			into: func() any { return &instanceState{} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := msgpack.Marshal(tt.value)
			require.NoError(t, err)

			// This is what a cross-host response goes through: decoded into an interface, then re-encoded for the caller's type
			var generic any
			err = msgpack.Unmarshal(enc, &generic)
			require.NoError(t, err, "the value must decode into an interface, which is what crossing a host boundary does")

			reEncoded, err := msgpack.Marshal(generic)
			require.NoError(t, err)

			direct := tt.into()
			require.NoError(t, msgpack.Unmarshal(enc, direct))

			viaGeneric := tt.into()
			require.NoError(t, msgpack.Unmarshal(reEncoded, viaGeneric))

			assert.Equal(t, direct, viaGeneric, "the value must survive the round-trip a cross-host response goes through")
		})
	}
}
