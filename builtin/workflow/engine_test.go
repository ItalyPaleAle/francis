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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
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
	// labels holds the workflow labels written alongside each actor's state
	labels map[string]*components.WorkflowLabels
	// ttls holds the expiry written with each actor's state, which is zero when the write asked for none
	ttls map[string]time.Duration
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
	// registryErr makes the registry's consistency check fail, which is how a lookup that could not reach it is injected
	registryErr error
	// registryPeekErr fails only the concurrent registry read path so rolling-deployment fallback can be exercised
	registryPeekErr error
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		state:            map[string][]byte{},
		labels:           map[string]*components.WorkflowLabels{},
		ttls:             map[string]time.Duration{},
		alarms:           map[string]actor.AlarmProperties{},
		jobs:             map[string]actor.JobInfo{},
		jobPayloads:      map[string]any{},
		liveKeys:         map[string]string{},
		registryResponse: registerResponse{Found: true, OK: true, Generation: 1},
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
	respErr := f.registryErr
	f.mu.Unlock()

	// The engine makes exactly one kind of synchronous call from a turn: the definition-registry check
	if method == methodRegister {
		if respErr != nil {
			return nil, respErr
		}
		// A successful fake registration echoes the requested graph identity like the real registry
		req, ok := data.(registerRequest)
		if ok && resp.OK && resp.Fingerprint == "" {
			resp.Fingerprint = req.Fingerprint
		}
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
	resp := f.registryResponse
	respErr := f.registryErr
	peekErr := f.registryPeekErr
	f.mu.Unlock()

	if method == methodCheck {
		if peekErr != nil {
			return nil, peekErr
		}
		if respErr != nil {
			return nil, respErr
		}
		return &fakeEnvelope{value: resp}, nil
	}

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

func (f *fakeHost) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, props actor.JobProperties) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failDispatch {
		return "", false, errors.New("injected dispatch failure")
	}

	// Francis deduplicates an idempotency key against live rows only, so re-dispatching a pending task is a no-op
	if props.IdempotencyKey != "" {
		k := key(actorType, actorID, props.IdempotencyKey)
		existing, ok := f.liveKeys[k]
		if ok {
			return existing, false, nil
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
	return jobID, true, nil
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

	payload := f.jobPayloads[jobID]
	f.removeJobLocked(jobID)
	f.nextID++
	newID := fmt.Sprintf("job-%d", f.nextID)
	j.JobID = newID
	j.Status = actor.JobStatusPending
	j.Attempts = 0
	j.LastError = ""
	f.jobs[newID] = j
	f.jobPayloads[newID] = payload
	return newID, nil
}

func (f *fakeHost) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string, _ ...actor.DeleteJobOption) error {
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
		f.labels[key(actorType, actorID)] = opts.WorkflowLabels()
		f.ttls[key(actorType, actorID)] = opts.TTL
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
	delete(f.ttls, key(actorType, actorID))
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
// The one call the engine is allowed to make from a turn is the definition-registry check, and nothing else may reach past the boundary
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

	// Start authorization and confirmation share the registry lock, while subsequent turns fence locally against the immutable journal identity
	host.mu.Lock()
	defer host.mu.Unlock()
	require.Len(t, host.invokes, 2)
	for _, invocation := range host.invokes {
		assert.Contains(t, invocation, methodRegister)
	}
}

// TestTurnConvergesAfterAFaultBetweenTheStateWriteAndTheDispatch injects a failure between SetState and reconcile, and asserts the retried turn converges without double-counting
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

// TestTurnWritesTheWorkflowLabelsWithTheJournal verifies the listing index can never disagree with the journal, because both are written in the same operation
func TestTurnWritesTheWorkflowLabelsWithTheJournal(t *testing.T) {
	host := newFakeHost()

	wf, err := New("labels", WithVersion(4), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 4}}))

	host.mu.Lock()
	labels := host.labels[key(ref.BuiltInActorTypePrefix+wf.baseType, "inst-1")]
	host.mu.Unlock()

	require.NotNil(t, labels)
	assert.Equal(t, string(StatusRunning), labels.Status)
	assert.Equal(t, 4, labels.Version)

	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "a", Index: 0, Attempt: 1}}))

	host.mu.Lock()
	labels = host.labels[key(ref.BuiltInActorTypePrefix+wf.baseType, "inst-1")]
	host.mu.Unlock()

	require.NotNil(t, labels)
	assert.Equal(t, string(StatusCompleted), labels.Status)
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
	host.registryResponse = registerResponse{Found: true, OK: false, Fingerprint: "someone-else's-graph", FirstSeenAt: time.Now()}

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

// TestAnAttemptTimeoutUsesTheStepFailurePolicy verifies a worker timeout report costs the step exactly what any other handler failure would
func TestAnAttemptTimeoutUsesTheStepFailurePolicy(t *testing.T) {
	host := newFakeHost()

	wf, err := New("deadline",
		WithTimeout(time.Hour),
		WithSteps(
			Step("slow", WithRun(noopRun), WithAttemptTimeout(time.Nanosecond), WithMaxAttempts(1), WithOptional()),
			Step("after", WithRun(noopRun)),
		),
	)
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "slow", Index: 0, Attempt: 1, Error: errAttemptTimeout.Error(), Retryable: true}}))

	st := readJournal(t, host, wf, "inst-1")
	assert.Equal(t, StepFailed, st.step("slow").Status)
	assert.Contains(t, st.step("slow").Error, errAttemptTimeout.Error())

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
			value: compReportPayload{Step: "s", Index: 1, Attempt: 3, Error: "e", ChildStatus: StatusCancelled, ChildCompensation: CompensationPartial, Retryable: true, Transport: true, TraceParent: "tp"},
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
			value: registerResponse{Found: true, OK: true, Fingerprint: "abc", FirstSeenAt: now},
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

// deliverInstanceJob advances one durable delivery and releases its live idempotency key as the runtime would
func deliverInstanceJob(t *testing.T, host *fakeHost, wf *Workflow, instanceID string, method string, o *orchestrator) {
	t.Helper()
	jobID := host.jobIDFor(builtinActorType(wf.baseType), instanceID, method)
	require.NotEmpty(t, jobID)
	err := o.Job(t.Context(), method, &payloadEnvelope{value: host.jobPayloads[jobID]})
	require.NoError(t, err)
	host.mu.Lock()
	host.removeJobLocked(jobID)
	host.mu.Unlock()
}

func TestCompensationTimeoutRemovesQueuedUndo(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup succeeds", true: "cleanup retries"}[failCleanup], func(t *testing.T) {
			host := &undoCleanupHost{fakeHost: newFakeHost()}
			wf, err := New("undo-timeout", WithTimeout(time.Hour), WithSteps(
				Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
				WaitForEvent("approval"),
			))
			require.NoError(t, err)
			o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
			err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
			require.NoError(t, err)
			err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
			require.NoError(t, err)
			err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}})
			require.NoError(t, err)
			undoJob := host.jobIDFor(builtinActorType(wf.undoType("")), workerActorID("instance", "effect", 0), methodCompensate)
			require.NotEmpty(t, undoJob)

			// Exhaust the compensation budget before the queued undo can enter its handler
			st, err := o.client.GetState(t.Context())
			require.NoError(t, err)
			st.StartedAt = time.Now().Add(-2 * time.Hour)
			st.DeadlineAt = instanceDeadline(&st, wf.def)
			err = o.client.SetState(t.Context(), st, nil)
			require.NoError(t, err)
			if failCleanup {
				host.failActorType = builtinActorType(wf.undoType(""))
				err = o.Alarm(t.Context(), alarmDeadline, nil)
				require.NoError(t, err)
				require.Equal(t, StatusCompensating, readJournal(t, host.fakeHost, wf, "instance").Status)
				host.failActorType = ""
			}

			// Publishing abandonment requires queued undo removal, including after a transient cleanup failure
			err = o.Alarm(t.Context(), alarmDeadline, nil)
			require.NoError(t, err)
			st = readJournal(t, host.fakeHost, wf, "instance")
			require.True(t, st.Status.IsTerminal())
			require.Equal(t, CompensationFailed, st.Compensation)
			_, err = host.GetJob(t.Context(), undoJob)
			require.ErrorIs(t, err, actor.ErrJobNotFound)
		})
	}
}

type recoveryFailureHost struct {
	*fakeHost

	failAlarm bool
	failRead  bool
	failState bool
	failRetry bool
}

func (h *recoveryFailureHost) SetAlarm(ctx context.Context, actorType string, actorID string, name string, props actor.AlarmProperties) error {
	if h.failAlarm {
		return errors.New("injected alarm write failure")
	}
	return h.fakeHost.SetAlarm(ctx, actorType, actorID, name, props)
}

func (h *recoveryFailureHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	if h.failState {
		return errors.New("injected state write failure")
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func (h *recoveryFailureHost) GetState(ctx context.Context, actorType string, actorID string, into any) error {
	if h.failRead {
		return errors.New("injected state read failure")
	}
	return h.fakeHost.GetState(ctx, actorType, actorID, into)
}

func (h *recoveryFailureHost) RetryJob(ctx context.Context, jobID string) (string, error) {
	if h.failRetry {
		return "", errors.New("injected job replay failure")
	}
	return h.fakeHost.RetryJob(ctx, jobID)
}

func TestRecurringDeadlineOutlivesTransientFailures(t *testing.T) {
	for _, failure := range []string{"journal read", "journal write", "alarm replacement"} {
		t.Run(failure, func(t *testing.T) {
			host := &recoveryFailureHost{fakeHost: newFakeHost()}
			wf, err := New("recurring", WithTimeout(time.Minute), WithSteps(WaitForEvent("approval")))
			require.NoError(t, err)
			o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
			err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
			require.NoError(t, err)
			props, err := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.NoError(t, err)
			require.Equal(t, "PT5S", props.Interval)
			err = props.Validate()
			require.NoError(t, err)
			delivery := &payloadEnvelope{value: props.Data}
			o = newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))

			// Fail more occurrences than the ordinary alarm retry limit while preventing any replacement write from repairing the row
			host.failAlarm = true
			host.failRead = failure == "journal read"
			host.failState = failure == "journal write"
			for range orchestratorMaxAttempts + 5 {
				err = o.Alarm(t.Context(), alarmDeadline, delivery)
				require.NoError(t, err, "a recurring occurrence must complete so the runtime reschedules it")
				remaining, readErr := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
				require.NoError(t, readErr)
				require.Equal(t, "PT5S", remaining.Interval)
				require.Equal(t, StatusRunning, readJournal(t, host.fakeHost, wf, "instance").Status)
			}

			// Recovery of the dependencies allows a later occurrence to enforce the original instance timeout and remove the recurrence
			host.failAlarm = false
			host.failRead = false
			host.failState = false
			st, err := o.client.GetState(t.Context())
			require.NoError(t, err)
			st.StartedAt = time.Now().Add(-2 * time.Minute)
			st.DeadlineAt = instanceDeadline(&st, wf.def)
			err = o.client.SetState(t.Context(), st, nil)
			require.NoError(t, err)
			err = o.Alarm(t.Context(), alarmDeadline, delivery)
			require.NoError(t, err)
			require.Equal(t, StatusFailed, readJournal(t, host.fakeHost, wf, "instance").Status)
			_, err = host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.ErrorIs(t, err, actor.ErrAlarmNotFound)
		})
	}
}

func TestLegacyDeadlineMigratesBeforeAJournalFailure(t *testing.T) {
	host := &recoveryFailureHost{fakeHost: newFakeHost()}
	wf, err := New("legacy-alarm", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)

	// An alarm created by an older release must gain its fallback before a journal write can prevent a turn
	err = host.SetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline, actor.AlarmProperties{DueTime: time.Now()})
	require.NoError(t, err)
	host.failState = true
	err = o.Alarm(t.Context(), alarmDeadline, nil)
	require.NoError(t, err)
	props, err := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
	require.NoError(t, err)
	require.Equal(t, "PT5S", props.Interval)
	require.Equal(t, deadlinePayload{Recurring: true}, props.Data)
}

func TestRecurringDeadlineStopsForInactiveJournals(t *testing.T) {
	for _, status := range []Status{"", StatusCompleted, StatusSuspended} {
		t.Run(string(status), func(t *testing.T) {
			host := newFakeHost()
			wf, err := New("inactive-alarm", WithSteps(WaitForEvent("approval")))
			require.NoError(t, err)
			o := newTestOrchestrator(t, wf, host, "instance")
			err = o.client.SetState(t.Context(), instanceState{Status: status}, nil)
			require.NoError(t, err)
			props := deadlineAlarmProperties(time.Now())
			err = o.client.SetAlarm(t.Context(), alarmDeadline, props)
			require.NoError(t, err)

			// A recurrence must stop explicitly when no active journal needs its deadline
			err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: props.Data})
			require.NoError(t, err)
			_, err = host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.ErrorIs(t, err, actor.ErrAlarmNotFound)
		})
	}
}

func TestDeadLetterRecoveryPreservesOriginalEvents(t *testing.T) {
	cases := []struct {
		method  string
		payload any
		status  Status
	}{
		{methodStart, startPayload{Version: 1, Input: json.RawMessage(`{"order":42}`), CreatedAt: time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)}, StatusRunning},
		{methodEvent, eventPayload{Name: "approval", Payload: json.RawMessage(`{"approved":true}`)}, StatusCompleted},
		{methodCancel, reasonPayload{Reason: "customer withdrew order"}, StatusCancelled},
		{methodSuspend, reasonPayload{Reason: "waiting for operator"}, StatusSuspended},
		{methodResume, nil, StatusRunning},
	}
	for _, tc := range cases {
		for _, callbackFails := range []bool{false, true} {
			name := tc.method
			if callbackFails {
				name += "/callback failure"
			}
			t.Run(name, func(t *testing.T) {
				host := &recoveryFailureHost{fakeHost: newFakeHost()}
				wf, err := New("event-recovery", WithSteps(WaitForEvent("approval")))
				require.NoError(t, err)
				svc := actor.NewService(host)
				o := newRoutedOrchestrator(t, wf, "instance", svc)
				if tc.method != methodStart {
					err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
					require.NoError(t, err)
				}
				if tc.method == methodResume {
					err = o.Job(t.Context(), methodSuspend, &payloadEnvelope{value: reasonPayload{Reason: "paused"}})
					require.NoError(t, err)
				}

				// Exhaust a durable request before it can alter the journal and invoke the same best-effort hook as the runtime
				jobID, _, err := o.client.Dispatch(t.Context(), tc.method, tc.payload)
				require.NoError(t, err)
				host.deadLetter(jobID, "transient delivery failure")
				host.failRetry = callbackFails
				err = o.JobFailed(t.Context(), jobID, tc.method, nil, errors.New("transient delivery failure"))
				if callbackFails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				// A new activation's recurring deadline can repair a failed callback from the retained record alone
				host.failRetry = false
				o = newRoutedOrchestrator(t, wf, "instance", svc)
				err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
				require.NoError(t, err)
				recovered := host.jobIDFor(builtinActorType(wf.baseType), "instance", tc.method)
				require.NotEmpty(t, recovered)
				require.NotEqual(t, jobID, recovered)
				require.Equal(t, tc.payload, host.jobPayloads[recovered])
				deliverInstanceJob(t, host.fakeHost, wf, "instance", tc.method, o)
				st := readJournal(t, host.fakeHost, wf, "instance")
				require.Equal(t, tc.status, st.Status)
				if tc.method == methodEvent {
					require.JSONEq(t, `{"approved":true}`, string(st.Output))
				}
				if tc.method == methodStart {
					require.JSONEq(t, `{"order":42}`, string(st.Input))
				}
			})
		}
	}
}

func TestDeadlineRecoversAnEventWhoseCallbackNeverRan(t *testing.T) {
	host := newFakeHost()
	wf, err := New("missed-hook", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	payload := eventPayload{Name: "approval", Payload: json.RawMessage(`"accepted"`)}
	jobID, _, err := o.client.Dispatch(t.Context(), methodEvent, payload)
	require.NoError(t, err)
	host.deadLetter(jobID, "transient delivery failure")

	// The durable alarm is sufficient to repair a dead letter even when no failure callback was delivered
	err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
	require.NoError(t, err)
	deliverInstanceJob(t, host, wf, "instance", methodEvent, o)
	st := readJournal(t, host, wf, "instance")
	require.Equal(t, StatusCompleted, st.Status)
	require.JSONEq(t, `"accepted"`, string(st.Output))
}

func TestPermanentDeliveryFailuresRemainInspectable(t *testing.T) {
	host := newFakeHost()
	wf, err := New("permanent-event", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	jobID, _, err := o.client.Dispatch(t.Context(), methodEvent, "malformed payload")
	require.NoError(t, err)
	host.deadLetter(jobID, actor.ErrJobPermanentFailure.Error())

	// Retrying a malformed payload cannot succeed and must not create an endless recovery loop
	err = o.JobFailed(t.Context(), jobID, methodEvent, nil, actor.ErrJobPermanentFailure)
	require.NoError(t, err)
	err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
	require.NoError(t, err)
	job, err := host.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.Equal(t, actor.JobStatusDeadLettered, job.Status)
	require.Equal(t, StatusRunning, readJournal(t, host, wf, "instance").Status)
}

type dispatchCountingHost struct {
	*fakeHost

	dispatchCalls map[string]int
	failAck       bool
}

func (h *dispatchCountingHost) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, props actor.JobProperties) (string, bool, error) {
	// Count provider requests before idempotency coalescing so repeated dispatch work remains visible
	h.mu.Lock()
	h.dispatchCalls[method]++
	h.mu.Unlock()
	return h.fakeHost.Dispatch(ctx, actorType, actorID, method, data, props)
}

func (h *dispatchCountingHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	// Reject only acknowledgement writes to reproduce a crash after job acceptance but before its marker becomes durable
	st, ok := state.(instanceState)
	if h.failAck && ok {
		for _, sr := range st.Steps {
			for _, tr := range sr.Tasks {
				if tr.DispatchedAttempt > 0 {
					return errors.New("injected dispatch acknowledgement failure")
				}
			}
		}
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func TestFanOutDispatchCallsGrowLinearly(t *testing.T) {
	for _, width := range []int{100, 200, 400} {
		for _, kind := range []string{"worker", "child"} {
			for _, window := range []int{0, 8} {
				t.Run(fmt.Sprintf("%s/%d/window-%d", kind, width, window), func(t *testing.T) {
					// Keep the provider jobs live so the old quadratic dispatch loop would still be counted despite coalescing
					host := &dispatchCountingHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
					work := ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithCompensate(noopCompensate), WithMaxParallel(window))
					forwardMethod := methodRun
					undoMethod := methodCompensate
					if kind == "child" {
						child, err := New("performance-child", WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))))
						require.NoError(t, err)
						work = ForEach("work", WithItemsFrom("plan"), WithChild(child), WithMaxParallel(window))
						forwardMethod = methodStart
						undoMethod = methodUnwind
					}
					wf, err := New("performance-fanout", WithSteps(
						Step("plan", WithRun(noopRun)),
						work,
						WaitForEvent("hold"),
					))
					require.NoError(t, err)
					svc := actor.NewService(host)
					o := newRoutedOrchestrator(t, wf, "instance", svc)
					err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
					require.NoError(t, err)
					items := make([]int, width)
					encoded, err := json.Marshal(items)
					require.NoError(t, err)
					err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "plan", Attempt: 1, Output: encoded}})
					require.NoError(t, err)

					// Every result uses a fresh activation, proving the dispatch bound comes from durable markers
					for index := range width {
						o = newRoutedOrchestrator(t, wf, "instance", svc)
						err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Index: index, Attempt: 1, Output: json.RawMessage(`"effect"`)}})
						require.NoError(t, err)
					}
					expectedForward := width
					if kind == "worker" {
						expectedForward++
					}
					assert.Equal(t, expectedForward, host.dispatchCalls[forwardMethod])

					// A wide compensation frame must also dispatch each member only once across sequential acknowledgements
					err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "undo"}})
					require.NoError(t, err)
					for index := range width {
						o = newRoutedOrchestrator(t, wf, "instance", svc)
						err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Index: index, Attempt: 1}})
						require.NoError(t, err)
					}
					assert.Equal(t, width, host.dispatchCalls[undoMethod])
					st := readJournal(t, host.fakeHost, wf, "instance")
					assert.Equal(t, StatusCancelled, st.Status)
					assert.Equal(t, CompensationCompleted, st.Compensation)
				})
			}
		}
	}
}

func TestDispatchAcknowledgementFailureDoesNotPoisonRetry(t *testing.T) {
	// Accept the first task's job but fail the subsequent acknowledgement write
	host := &dispatchCountingHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}, failAck: true}
	wf, err := New("dispatch-ack-failure", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	svc := actor.NewService(host)
	o := newRoutedOrchestrator(t, wf, "instance", svc)
	start := &payloadEnvelope{value: startPayload{Version: 1}}
	require.ErrorContains(t, o.Job(t.Context(), methodStart, start), "acknowledgement failure")
	st := readJournal(t, host.fakeHost, wf, "instance")
	assert.Zero(t, st.step("work").task(0).DispatchedAttempt)
	cached, err := o.client.GetState(t.Context())
	require.NoError(t, err)
	assert.Zero(t, cached.step("work").task(0).DispatchedAttempt)

	// The retry repeats the same idempotency key, then a new activation skips the durably acknowledged job
	host.failAck = false
	err = o.Job(t.Context(), methodStart, start)
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	assert.Len(t, host.dispatchedTo(builtinActorType(wf.workerType("")), workerActorID("instance", "work", 0)), 1)
	o = newRoutedOrchestrator(t, wf, "instance", svc)
	err = o.Job(t.Context(), methodStart, start)
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	st = readJournal(t, host.fakeHost, wf, "instance")
	assert.Equal(t, 1, st.step("work").task(0).DispatchedAttempt)
}

func TestDispatchMarkersPermitNewAttempts(t *testing.T) {
	// A transport failure reported by a dead-lettered worker must still schedule the engine's next attempt
	host := &dispatchCountingHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
	wf, err := New("dispatch-attempts", WithSteps(
		Step("work", WithRun(noopRun), WithCompensate(noopCompensate)),
		WaitForEvent("hold"),
	))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 1, Error: "transport exhausted", Retryable: true}})
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 2}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "undo"}})
	require.NoError(t, err)

	// Compensation retries use their own durable attempt acknowledgement
	err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Attempt: 1, Error: "transport exhausted", Retryable: true}})
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodCompensate])
	err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Attempt: 2}})
	require.NoError(t, err)
	assert.Equal(t, CompensationCompleted, readJournal(t, host.fakeHost, wf, "instance").Compensation)
}

func TestOversizedDispatchAcknowledgementRecordsTermination(t *testing.T) {
	// Measure the journal before its first dispatch acknowledgement so the marker alone crosses the configured cap
	baselineHost := newFakeHost()
	baselineWF, err := New("oversized-markers", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	baselineActor := newTestOrchestrator(t, baselineWF, baselineHost, "instance")
	err = baselineActor.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	baseline := readJournal(t, baselineHost, baselineWF, "instance")
	baseline.step("work").task(0).DispatchedAttempt = 0
	baseline.encoded = nil
	limit, err := journalSize(&baseline)
	require.NoError(t, err)

	// The second state write may terminate the instance after the start transition was already counted
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		err := provider.Shutdown(t.Context())
		require.NoError(t, err)
	})
	host := &dispatchCountingHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
	wf, err := New("oversized-markers", WithMaxJournalSize(limit), WithMeter(provider.Meter("markers")), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	st := readJournal(t, host.fakeHost, wf, "instance")
	assert.Equal(t, 1, host.dispatchCalls[methodRun], "the ordinary journal must fit until dispatch is acknowledged")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Contains(t, st.Cause, ErrJournalTooLarge.Error())
	assert.Equal(t, int64(0), int64MetricTotal(t, reader, "francis.workflow.instances.running"))
	assert.Equal(t, int64(1), int64MetricTotal(t, reader, "francis.workflow.instances.terminated"))
}

func TestFanOutShipsSourceArrayOnlyWhenRequested(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit-%t", explicit), func(t *testing.T) {
			// The item is sufficient for an ordinary fan-out handler, while explicit input dependencies retain their contract
			now := time.Now()
			work := ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithCompensate(noopCompensate))
			if explicit {
				work = work.With(WithInputFrom("plan"))
			}
			def := testDefinition(t, "fanout-payload", WithSteps(Step("plan", WithRun(noopRun)), work))
			st := startJournal(t, def, now)
			reportSuccess(t, st, def, "plan", 0, []string{"first", "second"}, now)
			advance(st, def, "instance", now)
			o := &orchestrator{instanceID: "instance", def: def, wf: &Workflow{baseType: "workflow-payload"}}
			sr := st.step("work")
			d := def.byName["work"]
			payload := o.buildRunPayload(st, sr, d, d, sr.task(0))
			assert.JSONEq(t, `"first"`, string(payload.Item))
			if explicit {
				assert.JSONEq(t, `["first","second"]`, string(payload.Outputs["plan"]))
			} else {
				assert.NotContains(t, payload.Outputs, "plan")
			}
		})
	}
}

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

func TestDeadLetterRecoveryPreservesDeadline(t *testing.T) {
	host := newFakeHost()
	wf, err := New("deadline", WithTimeout(time.Hour), WithSteps(Step("a", WithRun(noopRun))))
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

func TestTimeoutStartingUnwindKeepsBackstop(t *testing.T) {
	host := newFakeHost()
	wf, err := New("timeout-unwind", WithTimeout(time.Hour), WithSteps(
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

type failingStateHost struct {
	*fakeHost

	failState bool
}

func (h *failingStateHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	if h.failState {
		return errors.New("injected state write failure")
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func TestFailedStateWriteDoesNotCorruptRetry(t *testing.T) {
	host := &failingStateHost{fakeHost: newFakeHost()}
	wf, err := New("state-failure", WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun)),
	))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
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
	wf, err := New("suspended-turn", WithSteps(
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

func TestLateSuccessReopeningKeepsLifecycleMetricsBalanced(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(t.Context()))
	})

	host := newFakeHost()
	wf, err := New("reopen-metrics",
		WithMeter(provider.Meter("lifecycle")),
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
