package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

// TestListReturnsPendingInstances verifies a started instance is listed, with its version, before its start job runs
func TestListReturnsPendingInstances(t *testing.T) {
	host := newFakeHost()
	wf, err := New("pending-list", WithVersion(2), WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	svc := wf.Service(actor.NewService(host))

	_, created, err := svc.Start(t.Context(), nil, WithInstanceID("instance-1"))
	require.NoError(t, err)
	require.True(t, created)

	for _, opts := range []*ListOptions{nil, {Status: StatusPending}, {Version: 2}} {
		page, err := svc.List(t.Context(), opts)
		require.NoError(t, err)
		require.Len(t, page.Instances, 1)
		inst := page.Instances[0]
		assert.Equal(t, "instance-1", inst.InstanceID)
		assert.Equal(t, "pending-list", inst.Workflow)
		assert.Equal(t, StatusPending, inst.Status)
		assert.Equal(t, 2, inst.Version)
		assert.False(t, inst.CreatedAt.IsZero())
	}

	page, err := svc.List(t.Context(), &ListOptions{Status: StatusRunning})
	require.NoError(t, err)
	assert.Empty(t, page.Instances)

	// GetStatus reads the same placeholder, so it reports the version the instance was started with
	o := newTestOrchestrator(t, wf, host, "instance-1")
	result, err := o.status(t.Context())
	require.NoError(t, err)
	status, ok := result.(statusResult)
	require.True(t, ok)
	require.True(t, status.Found)
	assert.Equal(t, StatusPending, status.Status.Status)
	assert.Equal(t, 2, status.Status.Version)

	// The first journal write replaces the placeholder and its labels
	startJob := host.jobIDFor(builtinActorType(wf.baseType), "instance-1", methodStart)
	require.NotEmpty(t, startJob)
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: host.jobPayloads[startJob]})
	require.NoError(t, err)
	st := readJournal(t, host, wf, "instance-1")
	assert.Nil(t, st.PendingStart)
	assert.Equal(t, StatusRunning, st.Status)

	page, err = svc.List(t.Context(), &ListOptions{Status: StatusPending})
	require.NoError(t, err)
	assert.Empty(t, page.Instances)
	page, err = svc.List(t.Context(), &ListOptions{Status: StatusRunning})
	require.NoError(t, err)
	require.Len(t, page.Instances, 1)
	assert.Equal(t, "instance-1", page.Instances[0].InstanceID)
}

// TestListReturnsPendingChildren verifies a child is listed under its parent as soon as the parent dispatches its start
func TestListReturnsPendingChildren(t *testing.T) {
	host := newFakeHost()
	kid, err := New("pending-child", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	parentWF, err := New("pending-parent", WithSteps(Child("sub", WithDefinition(kid))))
	require.NoError(t, err)

	parent := newTestOrchestrator(t, parentWF, host, "parent")
	err = parent.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	parentState := readJournal(t, host, parentWF, "parent")
	childID := parentState.step("sub").task(0).ChildID
	require.NotEmpty(t, childID)

	page, err := kid.Service(actor.NewService(host)).List(t.Context(), &ListOptions{Parent: "parent"})
	require.NoError(t, err)
	require.Len(t, page.Instances, 1)
	inst := page.Instances[0]
	assert.Equal(t, childID, inst.InstanceID)
	assert.Equal(t, StatusPending, inst.Status)
	require.NotNil(t, inst.Parent)
	assert.Equal(t, "parent", inst.Parent.InstanceID)
	assert.Equal(t, "pending-parent", inst.Parent.Workflow)
	assert.Equal(t, "sub", inst.Parent.Step)
}

// TestAbandonedStartPlaceholderIsRemoved verifies the deadline turn removes the placeholder of a start that failed permanently, so it does not stay listed as pending
func TestAbandonedStartPlaceholderIsRemoved(t *testing.T) {
	host := newFakeHost()
	wf, err := New("pending-abandoned", WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	svc := wf.Service(actor.NewService(host))

	_, _, err = svc.Start(t.Context(), nil, WithInstanceID("instance-1"))
	require.NoError(t, err)
	startJob := host.jobIDFor(builtinActorType(wf.baseType), "instance-1", methodStart)
	require.NotEmpty(t, startJob)

	// A live start keeps its placeholder through a deadline turn
	o := newTestOrchestrator(t, wf, host, "instance-1")
	err = o.Alarm(t.Context(), alarmDeadline, nil)
	require.NoError(t, err)
	page, err := svc.List(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, page.Instances, 1)

	// A start that failed permanently is not retried, and its placeholder goes with the deadline turn the failure arms
	host.deadLetter(startJob, actor.ErrJobPermanentFailure.Error())
	err = o.JobFailed(t.Context(), startJob, methodStart, nil, actor.ErrJobPermanentFailure)
	require.NoError(t, err)
	err = o.runDeadline(t.Context())
	require.NoError(t, err)

	page, err = svc.List(t.Context(), nil)
	require.NoError(t, err)
	assert.Empty(t, page.Instances)
	host.mu.Lock()
	_, hasState := host.state[key(builtinActorType(wf.baseType), "instance-1")]
	host.mu.Unlock()
	assert.False(t, hasState)
}

// TestPendingStartDoesNotHoldAVersion verifies a placeholder does not make its version in use, since its start is fenced by the registry generation it carries
func TestPendingStartDoesNotHoldAVersion(t *testing.T) {
	host := newFakeHost()
	wf, err := New("pending-forget", WithVersion(3), WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	svc := actor.NewService(host)
	workflowType := builtinActorType(wf.baseType)

	_, _, err = wf.Service(svc).Start(t.Context(), nil, WithInstanceID("instance-1"))
	require.NoError(t, err)

	inUse, err := versionHasJournals(t.Context(), svc, wf.baseType, 3)
	require.NoError(t, err)
	assert.False(t, inUse)

	// Once the instance has a journal, the version is in use
	o := newTestOrchestrator(t, wf, host, "instance-1")
	startJob := host.jobIDFor(workflowType, "instance-1", methodStart)
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: host.jobPayloads[startJob]})
	require.NoError(t, err)

	inUse, err = versionHasJournals(t.Context(), svc, wf.baseType, 3)
	require.NoError(t, err)
	assert.True(t, inUse)
}
