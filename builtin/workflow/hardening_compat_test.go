package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
)

type hardeningConcurrentCleanupHost struct {
	*fakeHost
	workerType string
	entered    chan struct{}
	release    chan struct{}
}

func (h *hardeningConcurrentCleanupHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
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

func TestHardeningPurgeRefusesUnresolvableLegacyChild(t *testing.T) {
	child, err := New("hardening-legacy-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	oldParent, err := New("hardening-legacy-parent", WithVersion(1), WithSteps(Child("sub", WithDefinition(child))))
	require.NoError(t, err)
	newParent, err := New("hardening-legacy-parent", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
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

func TestHardeningPurgeRefusesUnresolvableLegacyWorker(t *testing.T) {
	oldWorkflow, err := New("hardening-legacy-worker", WithVersion(1), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	newWorkflow, err := New("hardening-legacy-worker", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
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

func TestHardeningPendingStatusDoesNotInventAVersion(t *testing.T) {
	wf, err := New("hardening-pending-version", WithVersion(2), WithSteps(WaitForEvent("ready")))
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

func TestHardeningJournalSizeUsesTheProviderEncoding(t *testing.T) {
	st := &instanceState{
		Workflow: "hardening-wire-size",
		Version:  1,
		Status:   StatusRunning,
		Steps:    []stepRecord{{Name: "work", Kind: KindStep, Status: StepRunning, Tasks: []taskRecord{{Index: 0, Attempts: 1}}}},
	}

	size, err := journalSize(st)
	require.NoError(t, err)
	encoded, err := msgpack.Marshal(*st)
	require.NoError(t, err)
	assert.Len(t, encoded, size)
	assert.Equal(t, st.encoded, encoded)

	wf, err := New("hardening-wire-size", WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, wf, host, "instance-1")
	st.Status = StatusFailed
	err = o.persist(t.Context(), st, time.Now())
	require.NoError(t, err)
	stored := readJournal(t, host, wf, "instance-1")
	assert.Equal(t, StatusFailed, stored.Status)
}

func TestHardeningTaskMetadataOmitsActorsThatCannotReceiveJobs(t *testing.T) {
	child, err := New("hardening-task-metadata-child", WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	def := testDefinition(t, "hardening-task-metadata", WithSteps(
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

func TestHardeningPurgeOverlapsIndependentJobCleanup(t *testing.T) {
	wf, err := New("hardening-parallel-cleanup", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	base := newFakeHost()
	host := &hardeningConcurrentCleanupHost{
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

func TestHardeningTerminationOverlapsIndependentJobCancellation(t *testing.T) {
	wf, err := New("hardening-parallel-cancellation", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	base := newFakeHost()
	host := &hardeningConcurrentCleanupHost{
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
