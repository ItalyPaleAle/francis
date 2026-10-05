package workflow

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// eventKinds returns the seq and kind of each event, which is what must be identical across a retried turn
func eventKinds(events []components.WorkflowEvent) []string {
	out := make([]string, len(events))
	for i, ev := range events {
		out[i] = fmt.Sprintf("%d:%s", ev.Seq, ev.Kind)
	}
	return out
}

// runHistoryScenario starts a two-step workflow, completes the first step, and fails the second, optionally failing the state write of the failure report once
func runHistoryScenario(t *testing.T, failOnce bool) (*failingStateHost, *Workflow, time.Time, time.Time) {
	t.Helper()

	host := &failingStateHost{fakeHost: newFakeHost()}
	wf, err := New("history", WithSteps(
		Step("first", WithRun(noopRun), WithCompensate(noopCompensate)),
		Step("second", WithRun(noopRun)),
	))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1, CreatedAt: time.Now()}})
	require.NoError(t, err)

	// The worker's own times travel with the report
	workerStart := time.Now().Add(-time.Second).UTC().Truncate(time.Microsecond)
	workerEnd := workerStart.Add(500 * time.Millisecond)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "first", Index: 0, Attempt: 1, StartedAt: workerStart, FinishedAt: workerEnd}})
	require.NoError(t, err)
	report := &payloadEnvelope{value: reportPayload{Step: "second", Index: 0, Attempt: 1, Error: "permanent handler failure"}}

	// A failed write followed by a retry on the same activation must not leave gaps or duplicates
	if failOnce {
		host.failState = true
		err = o.Job(t.Context(), methodDone, report)
		require.Error(t, err)
		host.failState = false
	}

	err = o.Job(t.Context(), methodDone, report)
	require.NoError(t, err)

	return host, wf, workerStart, workerEnd
}

func TestEventHistorySequenceSurvivesARetriedTurn(t *testing.T) {
	clean, wf, _, _ := runHistoryScenario(t, false)
	retried, _, workerStart, workerEnd := runHistoryScenario(t, true)

	actorType := ref.BuiltInActorTypePrefix + wf.baseType
	cleanEvents := clean.eventsOf(actorType, "instance")
	retriedEvents := retried.eventsOf(actorType, "instance")
	require.NotEmpty(t, cleanEvents)

	// The retried run records exactly the events of the clean run, with the same numbering
	assert.Equal(t, eventKinds(cleanEvents), eventKinds(retriedEvents))

	// Numbering starts at one with the start and has no gaps
	require.Equal(t, EventKindInstanceStarted, retriedEvents[0].Kind)
	for i, ev := range retriedEvents {
		assert.Equal(t, int64(i+1), ev.Seq, "event %d (%s) is misnumbered", i, ev.Kind)
	}

	// The journal records the last sequence number it wrote
	st := readJournal(t, retried.fakeHost, wf, "instance")
	assert.Equal(t, int64(len(retriedEvents)), st.LastEventSeq)
	view, err := DecodeInstance(mustMarshal(t, st))
	require.NoError(t, err)
	assert.Equal(t, st.LastEventSeq, view.LastEventSeq)
	assert.True(t, view.EventHistory)

	// Every event decodes, and the worker's times are labeled as worker-supplied
	kinds := map[string]EventView{}
	for _, ev := range retriedEvents {
		dv, decodeErr := DecodeEvent(ev)
		require.NoError(t, decodeErr)
		assert.Equal(t, ev.Seq, dv.Seq)
		assert.NotEmpty(t, dv.TimeSource, "event %s has no time source", ev.Kind)
		_, seen := kinds[ev.Kind]
		if !seen {
			kinds[ev.Kind] = dv
		}
	}
	for _, kind := range []string{EventKindStepStarted, EventKindTaskDispatched, EventKindWorkerStarted, EventKindWorkerFinished, EventKindTaskCompleted, EventKindStepCompleted, EventKindTaskFailed, EventKindStepFailed, EventKindCompensationStarted} {
		assert.Contains(t, kinds, kind)
	}
	started := kinds[EventKindWorkerStarted]
	assert.Equal(t, TimeSourceWorker, started.TimeSource)
	assert.True(t, workerStart.Equal(started.Time), "worker_started should carry the worker's start time")
	assert.Equal(t, "first", started.Step)
	require.NotNil(t, started.TaskIndex)
	assert.Equal(t, 0, *started.TaskIndex)
	assert.True(t, workerEnd.Equal(kinds[EventKindWorkerFinished].Time), "worker_finished should carry the worker's finish time")
	assert.Equal(t, "permanent handler failure", kinds[EventKindTaskFailed].Error)
	assert.Equal(t, TimeSourceEngine, kinds[EventKindInstanceStarted].TimeSource)
}

func TestWithoutEventHistoryRecordsNoEvents(t *testing.T) {
	host := newFakeHost()
	wf, err := New("no-history", WithoutEventHistory(), WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "only", Index: 0, Attempt: 1}})
	require.NoError(t, err)

	st := readJournal(t, host, wf, "instance")
	require.Equal(t, StatusCompleted, st.Status)
	assert.Empty(t, host.eventsOf(ref.BuiltInActorTypePrefix+wf.baseType, "instance"))
	view, err := DecodeInstance(mustMarshal(t, st))
	require.NoError(t, err)
	assert.False(t, view.EventHistory)
	assert.Zero(t, view.LastEventSeq)

	// The placeholder reports the opt-out too
	placeholder, _ := newPendingPlaceholder(wf.def, &startPayload{Version: 1})
	view, err = DecodeInstance(mustMarshal(t, placeholder))
	require.NoError(t, err)
	assert.False(t, view.EventHistory)
}

func TestCreatedLabelIsWrittenWithThePlaceholderAndTheJournal(t *testing.T) {
	host := newFakeHost()
	wf, err := New("created", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	svc := actor.NewService(host)
	id, created, err := wf.Service(svc).Start(t.Context(), map[string]string{"hello": "world"}, WithInstanceID("instance"))
	require.NoError(t, err)
	require.True(t, created)
	actorType := ref.BuiltInActorTypePrefix + wf.baseType

	// The start job carries the creation time the label is derived from
	jobID := host.jobIDFor(actorType, id, methodStart)
	require.NotEmpty(t, jobID)
	startData := mustMarshal(t, host.jobPayloads[jobID])
	start, err := DecodeStartPayload(startData)
	require.NoError(t, err)
	require.False(t, start.CreatedAt.IsZero())
	assert.Equal(t, 1, start.Version)
	assert.JSONEq(t, `{"hello":"world"}`, string(start.Input))
	assert.Nil(t, start.Parent)
	want := components.FormatWorkflowCreated(start.CreatedAt)

	// The placeholder is labeled and decodes as pending, with no events
	require.NotNil(t, host.labels[key(actorType, id)])
	assert.Equal(t, want, host.labels[key(actorType, id)].Created)
	view, err := DecodeInstance(host.state[key(actorType, id)])
	require.NoError(t, err)
	assert.True(t, view.Pending)
	assert.Equal(t, StatusPending, view.Status)
	assert.Equal(t, wf.name, view.Workflow)
	assert.Equal(t, 1, view.Version)
	assert.True(t, start.CreatedAt.Equal(view.CreatedAt))
	assert.True(t, view.EventHistory)
	assert.Empty(t, host.eventsOf(actorType, id))

	// Every journal write keeps the label
	var payload startPayload
	err = msgpack.Unmarshal(startData, &payload)
	require.NoError(t, err)
	o := newRoutedOrchestrator(t, wf, id, svc)
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: payload})
	require.NoError(t, err)
	assert.Equal(t, want, host.labels[key(actorType, id)].Created)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "only", Index: 0, Attempt: 1, Output: json.RawMessage(`42`)}})
	require.NoError(t, err)
	assert.Equal(t, want, host.labels[key(actorType, id)].Created)

	// The journal decodes without the definition
	view, err = DecodeInstance(host.state[key(actorType, id)])
	require.NoError(t, err)
	assert.False(t, view.Pending)
	assert.Equal(t, StatusCompleted, view.Status)
	assert.Equal(t, wf.name, view.Workflow)
	assert.Equal(t, 1, view.Version)
	assert.Equal(t, wf.def.fingerprint, view.DefinitionFingerprint)
	assert.JSONEq(t, `{"hello":"world"}`, string(view.Input))
	assert.True(t, view.HasOutput)
	assert.True(t, start.CreatedAt.Equal(view.CreatedAt))
	require.Len(t, view.Steps, 1)
	assert.Equal(t, "only", view.Steps[0].Name)
	assert.Equal(t, string(StepCompleted), view.Steps[0].Status)
	assert.Equal(t, 1, view.Steps[0].TaskCount)

	// The first event is the start, since the placeholder recorded none
	events := host.eventsOf(actorType, id)
	require.NotEmpty(t, events)
	assert.Equal(t, int64(1), events[0].Seq)
	assert.Equal(t, EventKindInstanceStarted, events[0].Kind)
	assert.Equal(t, EventKindInstanceCompleted, events[len(events)-1].Kind)
}

func TestControlJobMatchesTheServiceDispatch(t *testing.T) {
	wf, err := New("control", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	actorType := ref.BuiltInActorTypePrefix + wf.baseType

	for _, tc := range []struct {
		action   ControlAction
		reason   string
		dispatch func(s *WorkflowService, id string) error
	}{
		{ControlCancel, "stop it", func(s *WorkflowService, id string) error { return s.Cancel(t.Context(), id, "stop it") }},
		{ControlSuspend, "hold on", func(s *WorkflowService, id string) error { return s.Suspend(t.Context(), id, "hold on") }},
		{ControlResume, "", func(s *WorkflowService, id string) error { return s.Resume(t.Context(), id) }},
	} {
		t.Run(string(tc.action), func(t *testing.T) {
			host := newFakeHost()
			err = tc.dispatch(wf.Service(actor.NewService(host)), "instance")
			require.NoError(t, err)

			method, idemKey, data, err := ControlJob(tc.action, tc.reason)
			require.NoError(t, err)

			// The service dispatched one job, with the same method, idempotency key, and encoded data
			jobID := host.jobIDFor(actorType, "instance", method)
			require.NotEmpty(t, jobID, "the service did not dispatch %s", method)
			assert.Equal(t, jobID, host.liveKeys[key(actorType, "instance", idemKey)])
			sent := host.jobPayloads[jobID]
			if sent == nil {
				assert.Nil(t, data)
			} else {
				assert.Equal(t, mustMarshal(t, sent), data)
			}
		})
	}

	_, _, _, err = ControlJob("explode", "")
	require.Error(t, err)
}

func TestDecodeRegistryRoundTrips(t *testing.T) {
	seen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	st := registryState{
		Versions: []registryEntry{
			{Version: 1, Fingerprint: "abc", FirstSeenAt: seen, Generation: 1},
			{Version: 2, Fingerprint: "def", FirstSeenAt: seen.Add(time.Hour), Generation: 3},
		},
		NextGeneration: 4,
	}
	view, err := DecodeRegistry(mustMarshal(t, st))
	require.NoError(t, err)
	require.Len(t, view.Versions, 2)
	assert.Equal(t, 1, view.Versions[0].Version)
	assert.Equal(t, "abc", view.Versions[0].Fingerprint)
	assert.True(t, seen.Equal(view.Versions[0].FirstSeenAt))
	assert.Equal(t, uint64(1), view.Versions[0].Generation)
	assert.Equal(t, 2, view.Versions[1].Version)
	assert.Equal(t, uint64(3), view.Versions[1].Generation)

	// An empty registry decodes as one with no versions
	view, err = DecodeRegistry(nil)
	require.NoError(t, err)
	assert.Empty(t, view.Versions)
}

func TestDecodeChildLinksAndParents(t *testing.T) {
	parent := &parentRef{Workflow: "outer", InstanceID: "outer-1", Step: "sub", Index: 2, Depth: 1}
	st := instanceState{
		Workflow: "inner",
		Version:  3,
		Status:   StatusRunning,
		Parent:   parent,
		Steps: []stepRecord{{
			Name:      "children",
			Kind:      KindForEach,
			Status:    StepRunning,
			Remaining: 1,
			Tasks: []taskRecord{
				{Index: 0, Done: true, ChildID: "c-0", ChildType: workflowActorTypePrefix + "leaf"},
				{Index: 1},
			},
		}},
	}
	view, err := DecodeInstance(mustMarshal(t, st))
	require.NoError(t, err)
	require.NotNil(t, view.Parent)
	assert.Equal(t, "outer", view.Parent.Workflow)
	assert.Equal(t, "outer-1", view.Parent.InstanceID)
	assert.Equal(t, "sub", view.Parent.Step)
	assert.Equal(t, 2, view.Parent.Index)
	assert.Equal(t, 1, view.Parent.Depth)
	require.Len(t, view.Steps, 1)
	assert.Equal(t, 2, view.Steps[0].TaskCount)
	assert.Equal(t, 1, view.Steps[0].TasksRemaining)
	assert.Equal(t, []ChildLink{{Workflow: "leaf", InstanceID: "c-0", ActorType: OrchestratorActorType("leaf")}}, view.Steps[0].Children)

	// State that is neither a journal nor a placeholder is rejected
	_, err = DecodeInstance(nil)
	require.Error(t, err)
	_, err = DecodeInstance(mustMarshal(t, instanceState{}))
	require.Error(t, err)
}

func TestParseActorType(t *testing.T) {
	for _, tc := range []struct {
		in         string
		name       string
		role       ActorRole
		capability string
		ok         bool
	}{
		{OrchestratorActorType("orders"), "orders", RoleOrchestrator, "", true},
		{RegistryActorType("orders"), "orders", RoleRegistry, "", true},
		{"francis.builtin.workflow.orders.worker", "orders", RoleWorker, "", true},
		{"francis.builtin.workflow.orders.worker.gpu", "orders", RoleWorker, "gpu", true},
		{"francis.builtin.workflow.orders.undo", "orders", RoleUndo, "", true},
		{"francis.builtin.workflow.orders.undo.gpu", "orders", RoleUndo, "gpu", true},
		{"francis.builtin.workflow.orders.something", "orders", RoleOther, "", true},
		{"francis.builtin.cronjob.orders.purge", "orders", RoleOther, "", true},
		{"francis.builtin.cronjob.orders", "", "", "", false},
		{"francis.builtin.workflow.", "", "", "", false},
		{"myactor", "", "", "", false},
	} {
		name, role, capability, ok := ParseActorType(tc.in)
		assert.Equal(t, tc.ok, ok, tc.in)
		assert.Equal(t, tc.name, name, tc.in)
		assert.Equal(t, tc.role, role, tc.in)
		assert.Equal(t, tc.capability, capability, tc.in)
	}
	assert.Equal(t, ActorTypePrefix+"orders", OrchestratorActorType("orders"))
	assert.Equal(t, "start", StartJobName)
}

func TestManagementDefinition(t *testing.T) {
	wf, err := New("identity", WithVersion(4), WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	name, version, fingerprint := wf.ManagementDefinition()
	assert.Equal(t, "identity", name)
	assert.Equal(t, 4, version)
	assert.Equal(t, wf.def.fingerprint, fingerprint)
	assert.NotEmpty(t, fingerprint)
}

// mustMarshal encodes a value as MessagePack the way the actor client does
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()

	enc, err := msgpack.Marshal(v)
	require.NoError(t, err)
	return enc
}
