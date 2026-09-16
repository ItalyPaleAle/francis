package workflow

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
)

// reviewAPIHost routes synchronous registry and purge calls while retaining the existing fake host's deterministic storage
type reviewAPIHost struct {
	*fakeHost

	registry  *registryActor
	workflows map[string]*Workflow
}

func (h *reviewAPIHost) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	// Route registry calls through the real registry implementation so forget and cache behavior share one durable state
	if method == methodRegister || method == methodForget || method == methodDefinitions {
		result, err := h.registry.Invoke(ctx, method, &payloadEnvelope{value: data})
		return &fakeEnvelope{value: result}, err
	}

	// Route purge calls through the real orchestrator to exercise the service's sweep decisions
	if method == methodPurge {
		wf := h.workflows[actorType]
		obj := newOrchestrator(wf, actorID, actor.NewService(h))
		orchestrator, ok := obj.(*orchestrator)
		if !ok {
			return nil, errors.New("workflow factory did not return an orchestrator")
		}
		result, err := orchestrator.purge(ctx)
		return &fakeEnvelope{value: result}, err
	}
	return h.fakeHost.Invoke(ctx, actorType, actorID, method, data, opts...)
}

func newReviewOrchestrator(t *testing.T, wf *Workflow, actorID string, svc *actor.Service) *orchestrator {
	t.Helper()
	obj := newOrchestrator(wf, actorID, svc)
	orchestrator, ok := obj.(*orchestrator)
	require.True(t, ok)
	return orchestrator
}

func (h *reviewAPIHost) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	if method == methodCheck {
		result, err := h.registry.Peek(ctx, method, &payloadEnvelope{value: data})
		return &fakeEnvelope{value: result}, err
	}
	return h.fakeHost.Peek(ctx, actorType, actorID, method, data, opts...)
}

func (h *reviewAPIHost) ListStates(ctx context.Context, actorType string, opts *actor.ListStatesOpts) (actor.StateList, error) {
	// Supply the filtering that the baseline fake host omits so the actual service listing and sweep can run
	h.mu.Lock()
	defer h.mu.Unlock()
	page := actor.StateList{}
	filter := opts.WorkflowLabels()
	for stateKey, encoded := range h.state {
		if !strings.HasPrefix(stateKey, actorType+"/") {
			continue
		}
		var st instanceState
		err := msgpack.Unmarshal(encoded, &st)
		if err != nil {
			return page, err
		}
		if filter != nil && filter.Status != "" && filter.Status != string(st.Status) {
			continue
		}
		if filter != nil && filter.Version != 0 && filter.Version != st.Version {
			continue
		}
		id := strings.TrimPrefix(stateKey, actorType+"/")
		if id <= opts.After {
			continue
		}
		page.States = append(page.States, actor.StateInfo{ActorID: id, Data: &fakeEnvelope{value: st}})
	}
	sort.Slice(page.States, func(i, j int) bool { return page.States[i].ActorID < page.States[j].ActorID })
	if opts.Limit > 0 && len(page.States) > opts.Limit {
		page.HasMore = true
		page.States = page.States[:opts.Limit]
	}
	return page, nil
}

func TestReviewForgetVersionInvalidatesExistingDecisions(t *testing.T) {
	// Establish one approved graph and one rejected graph before the operator resets the unused version
	host := &reviewAPIHost{fakeHost: newFakeHost()}
	host.registry = newTestRegistry(t, host.fakeHost)
	svc := actor.NewService(host)
	old, err := New("review-cache", WithSteps(Step("old", WithRun(noopRun))))
	require.NoError(t, err)
	corrected, err := New("review-cache", WithSteps(Step("corrected", WithRun(noopRun))))
	require.NoError(t, err)
	served, err := old.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.True(t, served)
	served, err = corrected.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.False(t, served)

	// Reset through the public service and let a new process claim the corrected graph under the same version
	require.NoError(t, corrected.Service(svc).ForgetVersion(t.Context(), 1))
	restarted, err := New("review-cache", WithSteps(Step("corrected", WithRun(noopRun))))
	require.NoError(t, err)
	served, err = restarted.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.True(t, served)

	// Existing processes must converge on the new registry entry instead of retaining conflicting approvals or stale rejections
	served, err = old.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	assert.False(t, served, "the old graph remains approved after another graph claims its version")
	served, err = corrected.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	assert.True(t, served, "the corrected graph remains rejected after the registry reset")
}

func TestReviewStepSpecReuseDoesNotMutateBuiltDefinition(t *testing.T) {
	// Reusing a declaration for a second workflow must preserve the first workflow's validated attempt policy
	spec := Step("charge", WithRun(noopRun), WithMaxAttempts(1))
	first, err := New("review-first", WithSteps(spec))
	require.NoError(t, err)
	originalFingerprint := first.def.fingerprint
	_, err = New("review-second", WithSteps(spec.With(WithMaxAttempts(9))))
	require.NoError(t, err)
	assert.Equal(t, 1, first.def.byName["charge"].maxAttempts, "building another workflow mutated the existing definition")

	// Recomputing the fingerprint reveals that the live graph no longer matches the fingerprint cached at construction
	first.def.setFingerprint()
	assert.Equal(t, originalFingerprint, first.def.fingerprint, "the graph changed behind its previously published fingerprint")
}

func TestReviewFingerprintDistinguishesCommaContainingReferences(t *testing.T) {
	for _, option := range []string{"inputFrom", "skipOnFailure"} {
		t.Run(option, func(t *testing.T) {
			// Keep the full graph identical while changing whether a reference names one comma-containing step or two separate steps
			build := func(references []string) *definition {
				steps := []StepSpec{
					Step("a", WithRun(noopRun)),
					Step("b", WithRun(noopRun)),
					Step("a,b", WithRun(noopRun)),
				}
				if option == "inputFrom" {
					steps = append(steps, Step("consumer", WithRun(noopRun), WithInputFrom(references...)))
				} else {
					steps = append([]StepSpec{Step("producer", WithRun(noopRun), WithSkipOnFailure(references...))}, steps...)
				}
				return testDefinition(t, "review-fingerprint", WithSteps(steps...))
			}
			oneReference := build([]string{"a,b"})
			twoReferences := build([]string{"a", "b"})
			assert.NotEqual(t, oneReference.fingerprint, twoReferences.fingerprint, "different valid reference lists have identical fingerprints")
		})
	}
}

func TestReviewSweepCollectsChildAfterParentJournalExpires(t *testing.T) {
	// Persist an old completed child with an absent parent, which models the parent's independent retention TTL expiring
	wf, err := New("review-orphan", WithSteps(Step("done", WithRun(noopRun))))
	require.NoError(t, err)
	host := &reviewAPIHost{fakeHost: newFakeHost(), workflows: map[string]*Workflow{builtinActorType(wf.baseType): wf}}
	svc := actor.NewService(host)
	child := newReviewOrchestrator(t, wf, "child-1", svc)
	st := &instanceState{
		Workflow:    wf.name,
		Version:     1,
		Status:      StatusCompleted,
		CompletedAt: time.Now().Add(-3 * defaultRetention),
		Parent:      &parentRef{Workflow: "expired-parent", InstanceID: "parent-1", Step: "child"},
	}
	require.NoError(t, child.persist(t.Context(), st, time.Now()))
	require.Zero(t, host.ttls[key(builtinActorType(wf.baseType), "child-1")], "children have no TTL fallback")
	active, err := child.parentStillRunning(t.Context(), st.Parent)
	require.NoError(t, err)
	require.False(t, active, "the missing parent cannot need a future compensation")

	// The actual sweep must reclaim this child now that no parent remains to purge it recursively
	removed, err := wf.Service(svc).PurgeTerminated(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "the sweep skipped a permanently orphaned child")
	var remaining instanceState
	err = host.GetState(t.Context(), builtinActorType(wf.baseType), "child-1", &remaining)
	assert.ErrorIs(t, err, actor.ErrStateNotFound)
}

func TestReviewPurgeRemovesRetainedWorkerAndUndoJobs(t *testing.T) {
	// Recreate a terminal compensated journal with retained jobs in all three actor families
	wf, err := New("review-purge-jobs", WithSteps(Step("charge", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, wf, host, "inst-1")
	st := &instanceState{
		Workflow:     wf.name,
		Version:      1,
		Status:       StatusCancelled,
		Compensation: CompensationCompleted,
		CompletedAt:  time.Now(),
		Steps:        []stepRecord{{Name: "charge", Kind: KindStep, Status: StepCompensated, Tasks: []taskRecord{{Index: 0, Done: true, Compensated: true, Comp: &compRecord{Done: true, Attempts: 1}}}}},
	}
	require.NoError(t, o.persist(t.Context(), st, time.Now()))
	retainedIDs := make([]string, 0, 3)
	for _, target := range []struct{ actorType, actorID, method string }{
		{builtinActorType(wf.baseType), "inst-1", methodDone},
		{builtinActorType(wf.workerType("")), workerActorID("inst-1", "charge", 0), methodRun},
		{builtinActorType(wf.undoType("")), workerActorID("inst-1", "charge", 0), methodCompensate},
	} {
		id, _, dispatchErr := host.Dispatch(t.Context(), target.actorType, target.actorID, target.method, nil, actor.JobProperties{})
		require.NoError(t, dispatchErr)
		host.deadLetter(id, "retained transport failure")
		retainedIDs = append(retainedIDs, id)
	}

	// Successful purge must remove every retained job before deleting the only journal that identifies its task actors
	result, err := o.purge(t.Context())
	require.NoError(t, err)
	require.Equal(t, purgeResult{Found: true}, result)
	for _, id := range retainedIDs {
		job, jobErr := host.GetJob(t.Context(), id)
		assert.ErrorIs(t, jobErr, actor.ErrJobNotFound, "purge left job %s for %s/%s", id, job.ActorType, job.ActorID)
	}
}

func TestReviewPurgeUsesTheJournaledChildType(t *testing.T) {
	child, err := New("review-journaled-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	oldParent, err := New("review-changing-parent", WithVersion(1), WithSteps(Child("sub", WithDefinition(child))))
	require.NoError(t, err)
	newParent, err := New("review-changing-parent", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)

	// Persist a version-one parent and child while the serving parent definition no longer contains the child step
	host := &reviewAPIHost{
		fakeHost: newFakeHost(),
		workflows: map[string]*Workflow{
			builtinActorType(newParent.baseType): newParent,
			builtinActorType(child.baseType):     child,
		},
	}
	svc := actor.NewService(host)
	parentState := &instanceState{
		Workflow:     oldParent.name,
		Version:      1,
		Status:       StatusCompleted,
		Compensation: CompensationNone,
		CompletedAt:  time.Now(),
		Steps: []stepRecord{{
			Name:   "sub",
			Kind:   KindChild,
			Status: StepCompleted,
			Tasks: []taskRecord{{
				Index:     0,
				Done:      true,
				ChildID:   "child-1",
				ChildType: child.baseType,
			}},
		}},
	}
	parent := newReviewOrchestrator(t, newParent, "parent-1", svc)
	require.NoError(t, parent.persist(t.Context(), parentState, time.Now()))
	childState := &instanceState{
		Workflow:     child.name,
		Version:      1,
		Status:       StatusCompleted,
		Compensation: CompensationNone,
		CompletedAt:  time.Now(),
		Parent:       &parentRef{Workflow: oldParent.name, InstanceID: "parent-1", Step: "sub"},
	}
	childOrchestrator := newReviewOrchestrator(t, child, "child-1", svc)
	require.NoError(t, childOrchestrator.persist(t.Context(), childState, time.Now()))

	// Purging through version two must still remove the child named by the version-one journal
	require.NoError(t, newParent.Service(svc).Purge(t.Context(), "parent-1"))
	var remaining instanceState
	err = host.GetState(t.Context(), builtinActorType(child.baseType), "child-1", &remaining)
	assert.ErrorIs(t, err, actor.ErrStateNotFound)
}

func TestReviewParentStatusRetainsTheChildCompensationOutcome(t *testing.T) {
	child, err := New("review-outcome-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "review-outcome-parent", WithSteps(
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
	child, err := New("review-unwind-outcome-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	def := testDefinition(t, "review-unwind-outcome-parent", WithSteps(Child("sub", WithDefinition(child))))
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
