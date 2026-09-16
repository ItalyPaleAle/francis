package workflow

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/builtinkey"
)

// purge removes everything a terminated instance left behind: its children first, then its dead-letters, then its journal
// That order is why an interrupted purge is safe to repeat
func (o *orchestrator) purge(ctx context.Context) (any, error) {
	st, err := o.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the workflow journal: %w", err)
	}
	if st.Status == "" {
		return purgeResult{}, nil
	}
	if !st.Status.IsTerminal() {
		return purgeResult{Found: true, Active: true}, nil
	}

	// A child is never purged from under a parent that might still unwind it, so the parent's own purge is what reaches it
	active, err := o.parentStillRunning(ctx, st.Parent)
	if err != nil {
		return nil, err
	}
	if active {
		return purgeResult{Found: true, Active: true}, nil
	}

	// Children go first, recursively, so a child is never left orphaned by its parent's removal
	for i := range st.Steps {
		for j := range st.Steps[i].Tasks {
			tr := &st.Steps[i].Tasks[j]
			childID := tr.ChildID
			if childID == "" {
				continue
			}

			childType := tr.ChildType
			if childType == "" {
				child := o.childDefinitionFor(st.Steps[i].Name, tr.Index)
				if child != nil {
					childType = child.baseType
				}
			}
			if childType == "" {
				continue
			}

			cErr := o.purgeChild(ctx, childType, childID)
			if cErr != nil && !errors.Is(cErr, ErrInstanceNotFound) {
				return nil, fmt.Errorf("failed to purge child %s: %w", childID, cErr)
			}
		}
	}

	// The instance's own jobs are a bounded set, since each one belongs to a journal entry
	err = o.purgeJobs(ctx, &st)
	if err != nil {
		return nil, err
	}

	err = o.client.DeleteState(ctx)
	if err != nil && !errors.Is(err, actor.ErrStateNotFound) {
		return nil, fmt.Errorf("failed to remove the workflow journal: %w", err)
	}

	o.wf.metrics.instancesPurged.Add(ctx, 1, metric.WithAttributes(attribute.String("workflow", o.def.name)))
	o.client.Halt()
	return purgeResult{Found: true}, nil
}

// parentStillRunning reports whether this instance's parent exists and has not terminated
// Reading the parent's journal is a bounded state read rather than an invocation, so it stays on the right side of the orchestration boundary
func (o *orchestrator) parentStillRunning(ctx context.Context, parent *parentRef) (bool, error) {
	if parent == nil {
		return false, nil
	}

	parentType := workflowActorTypePrefix + parent.Workflow
	client := builtinactor.NewClient[instanceState](parentType, parent.InstanceID, o.svc)
	parentState, err := client.GetState(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to read the parent's journal: %w", err)
	}

	// A parent whose journal is gone was purged already, and cannot come back to unwind anything
	return parentState.Status != "" && !parentState.Status.IsTerminal(), nil
}

// purgeJobs removes every job the instance still has, whether it is still scheduled or has already ended
func (o *orchestrator) purgeJobs(ctx context.Context, st *instanceState) error {
	err := deleteActorJobs(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to remove the instance's jobs: %w", err)
	}

	for i := range st.Steps {
		sr := &st.Steps[i]
		d := o.def.byName[sr.Name]
		for j := range sr.Tasks {
			tr := &sr.Tasks[j]
			workerType := tr.WorkerType
			undoType := tr.UndoType
			if d != nil {
				member := memberDef(d, tr.Index)
				if workerType == "" {
					workerType = o.wf.workerType(member.capability)
				}
				if undoType == "" {
					undoType = o.wf.undoType(member.capability)
				}
			}

			actorID := workerActorID(o.instanceID, sr.Name, tr.Index)
			for _, actorType := range []string{workerType, undoType} {
				if actorType == "" {
					continue
				}
				client := builtinactor.NewClient[struct{}](actorType, actorID, o.svc)
				err = deleteActorJobs(ctx, client)
				if err != nil {
					return fmt.Errorf("failed to remove jobs for %s/%s: %w", actorType, actorID, err)
				}
			}
		}
	}
	return nil
}

// deleteActorJobs removes every retained or live job owned by one actor
func deleteActorJobs[T any](ctx context.Context, client actor.Client[T]) error {
	jobs, err := client.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		err = client.DeleteJob(ctx, job.JobID)
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			return fmt.Errorf("failed to remove job %s: %w", job.JobID, err)
		}
	}
	return nil
}

// purgeChild invokes a journaled child type directly so cleanup does not depend on the current parent's graph
func (o *orchestrator) purgeChild(ctx context.Context, childType string, childID string) error {
	env, err := retryWhilePlacementMoves(ctx, func(ctx context.Context) (actor.Envelope, error) {
		return builtinactor.InvokeActor(ctx, o.svc, childType, childID, methodPurge, nil)
	})
	if err != nil {
		return err
	}

	var res purgeResult
	err = env.Decode(&res)
	if err != nil {
		return fmt.Errorf("failed to decode the child purge result: %w", err)
	}
	if !res.Found {
		return ErrInstanceNotFound
	}
	if res.Active {
		return ErrInstanceActive
	}
	return nil
}

// status builds the caller-facing view of the journal, reading through the provider rather than the activation's cache so an active actor cannot serve a journal past its retention
func (o *orchestrator) status(ctx context.Context) (any, error) {
	// A fresh client is what bypasses the activation's cached copy, since the cache only ever holds this actor's own state
	fresh := builtinactor.NewClient[instanceState](o.wf.baseType, o.instanceID, o.svc)
	st, err := fresh.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the workflow journal: %w", err)
	}

	// An instance with no journal yet is either pending, with its start job still live, or was never started at all
	// Only a job that has not ended says the instance is pending: a retained record of a start that already ran belongs to a journal that has since expired
	if st.Status == "" {
		jobs, jErr := o.client.ListJobs(ctx)
		if jErr == nil {
			for _, j := range jobs {
				if j.Method == methodStart && !j.Status.IsTerminal() {
					return statusResult{Found: true, Status: statusView(o.instanceID, &instanceState{
						Workflow:  o.def.name,
						Version:   o.def.version,
						Status:    StatusPending,
						CreatedAt: j.CreatedAt,
					}, o.def)}, nil
				}
			}
		}
		return statusResult{}, nil
	}

	return statusResult{Found: true, Status: statusView(o.instanceID, &st, o.def)}, nil
}

// reportToParent dispatches a terminated child's outcome to the instance that started it, which is the only thing that crosses between their journals
// A child asked to undo itself reports a compensation; any other termination reports a result, which the parent's step policy then decides what to make of
func (o *orchestrator) reportToParent(ctx context.Context, st *instanceState, force bool) error {
	if st.Parent == nil || (st.Reported && !force) {
		return nil
	}

	parentType := workflowActorTypePrefix + st.Parent.Workflow
	client := builtinactor.NewClient[struct{}](parentType, st.Parent.InstanceID, o.svc)

	if st.Parent.UnwoundBy > 0 {
		errMsg := ""
		if st.Compensation == CompensationPartial || st.Compensation == CompensationFailed {
			errMsg = fmt.Sprintf("child %s unwound with compensation: %s", o.instanceID, st.Compensation)
		}

		key := fmt.Sprintf("comp%s%s%s%d%s%d", idDelimiter, st.Parent.Step, idDelimiter, st.Parent.Index, idDelimiter, st.Parent.UnwoundBy)
		_, _, err := client.Dispatch(ctx, methodCompensated, compReportPayload{
			Step:              st.Parent.Step,
			Index:             st.Parent.Index,
			Attempt:           st.Parent.UnwoundBy,
			Error:             errMsg,
			ChildStatus:       st.Status,
			ChildCompensation: st.Compensation,
			TraceParent:       st.TraceParent,
		}, actor.WithIdempotencyKey(key))
		if err != nil {
			return fmt.Errorf("failed to report the unwind to the parent: %w", err)
		}
		return o.markReported(ctx, st)
	}

	attempt := st.Parent.Attempt
	if attempt <= 0 {
		attempt = 1
	}

	report := reportPayload{
		Step:              st.Parent.Step,
		Index:             st.Parent.Index,
		Attempt:           attempt,
		Output:            st.Output,
		ChildStatus:       st.Status,
		ChildCompensation: st.Compensation,
		TraceParent:       st.TraceParent,
	}
	if st.Status != StatusCompleted {
		report.Output = nil
		report.Error = fmt.Sprintf("child %s terminated %s: %s", o.instanceID, st.Status, st.Cause)
	}

	key := fmt.Sprintf("done%s%s%s%d%s%d", idDelimiter, st.Parent.Step, idDelimiter, st.Parent.Index, idDelimiter, attempt)
	_, _, err := client.Dispatch(ctx, methodDone, report, actor.WithIdempotencyKey(key))
	if err != nil {
		return fmt.Errorf("failed to report to the parent: %w", err)
	}
	return o.markReported(ctx, st)
}

// markReported records that the parent has been told, so a retried turn does not report the same termination twice
func (o *orchestrator) markReported(ctx context.Context, st *instanceState) error {
	st.Reported = true
	opts := &actor.SetStateOpts{
		TTL: o.terminalStateTTL(st),
	}
	opts.SetWorkflowLabels(builtinkey.Key{}, o.labels(st))

	err := o.client.SetState(ctx, *st, opts)
	if err != nil {
		return fmt.Errorf("failed to record the report to the parent: %w", err)
	}
	return nil
}
