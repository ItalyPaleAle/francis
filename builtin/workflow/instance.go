package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"

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

	// A descendant is retained while any ancestor can still reopen the completed children between them to unwind it
	active, err := o.parentStillRunning(ctx, st.Parent)
	if err != nil {
		return nil, err
	}
	if active {
		return purgeResult{Found: true, Active: true}, nil
	}

	// Older journals may omit cleanup targets, and those references are reconstructed only after the registry proves this exact graph owns the version
	err = o.bindLegacyCleanupDefinition(ctx, &st)
	if err != nil {
		return nil, err
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
				childType, err = o.legacyChildType(&st, st.Steps[i].Name, tr.Index)
				if err != nil {
					return nil, err
				}
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

// parentStillRunning reports whether any retained ancestor can still reopen this descendant for compensation
// Reading ancestor journals avoids synchronous actor invocations while a recursive purge holds its ancestors' turn locks
func (o *orchestrator) parentStillRunning(ctx context.Context, parent *parentRef) (bool, error) {
	// Completed intermediate children remain eligible to unwind until every ancestor has terminated
	seen := make(map[[2]string]struct{})
	for parent != nil {
		identity := [2]string{parent.Workflow, parent.InstanceID}
		_, repeated := seen[identity]
		if repeated {
			return false, fmt.Errorf("workflow ancestry contains a cycle at %s/%s", parent.Workflow, parent.InstanceID)
		}
		seen[identity] = struct{}{}

		parentType := workflowActorTypePrefix + parent.Workflow
		client := builtinactor.NewClient[instanceState](parentType, parent.InstanceID, o.svc)
		parentState, err := client.GetState(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to read ancestor %s/%s: %w", parent.Workflow, parent.InstanceID, err)
		}

		// A missing journal breaks the chain because no retained parent can reopen that instance
		if parentState.Status == "" {
			return false, nil
		}
		if !parentState.Status.IsTerminal() {
			return true, nil
		}
		parent = parentState.Parent
	}
	return false, nil
}

// purgeJobs removes every job the instance still has, whether it is still scheduled or has already ended
func (o *orchestrator) purgeJobs(ctx context.Context, st *instanceState) error {
	err := deleteActorJobs(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to remove the instance's jobs: %w", err)
	}

	// Independent worker actors are cleaned concurrently so large fan-outs do not serialize provider round trips
	targets := make([]jobCleanupTarget, 0)
	for i := range st.Steps {
		sr := &st.Steps[i]
		for j := range sr.Tasks {
			tr := &sr.Tasks[j]
			if tr.ChildID != "" {
				continue
			}

			workerType := tr.WorkerType
			if workerType == "" {
				workerType, err = o.legacyTaskActorType(st, sr.Name, tr.Index, false)
				if err != nil {
					return err
				}
			}

			actorID := workerActorID(o.instanceID, sr.Name, tr.Index)
			actorTypes := []string{workerType}
			if tr.Comp != nil {
				undoType := tr.UndoType
				if undoType == "" {
					undoType, err = o.legacyTaskActorType(st, sr.Name, tr.Index, true)
					if err != nil {
						return err
					}
				}
				actorTypes = append(actorTypes, undoType)
			}
			for _, actorType := range actorTypes {
				if actorType == "" {
					continue
				}
				targets = append(targets, jobCleanupTarget{actorType: actorType, actorID: actorID})
			}
		}
	}
	return o.deleteJobTargets(ctx, targets)
}

// jobCleanupTarget identifies one task actor whose retained jobs belong to the purged instance
type jobCleanupTarget struct {
	actorType string
	actorID   string
}

// deleteJobTargets bounds concurrent cleanup while overlapping independent provider calls
func (o *orchestrator) deleteJobTargets(ctx context.Context, targets []jobCleanupTarget) error {
	if len(targets) == 0 {
		return nil
	}

	// A fixed worker bound avoids turning a large fan-out into an equally large goroutine and connection burst
	workers := min(len(targets), 16)
	targetCh := make(chan jobCleanupTarget)
	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for target := range targetCh {
				client := builtinactor.NewClient[struct{}](target.actorType, target.actorID, o.svc)
				err := deleteActorJobs(ctx, client)
				if err != nil {
					errCh <- fmt.Errorf("failed to remove jobs for %s/%s: %w", target.actorType, target.actorID, err)
				}
			}
		})
	}

	// Feed every target before collecting failures because the error channel is sized for the whole batch
	for _, target := range targets {
		targetCh <- target
	}
	close(targetCh)
	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	return joined
}

// cancelJobTargets removes live work concurrently before the journal publishes that the attempts were abandoned
func (o *orchestrator) cancelJobTargets(ctx context.Context, targets []jobCleanupTarget) error {
	if len(targets) == 0 {
		return nil
	}

	// The same bounded fan-out used by purge keeps terminal cleanup latency proportional to provider latency rather than task count
	workers := min(len(targets), 16)
	targetCh := make(chan jobCleanupTarget)
	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for target := range targetCh {
				client := builtinactor.NewClient[struct{}](target.actorType, target.actorID, o.svc)
				err := cancelLiveActorJobs(ctx, client)
				if err != nil {
					errCh <- fmt.Errorf("failed to cancel jobs for %s/%s: %w", target.actorType, target.actorID, err)
				}
			}
		})
	}

	for _, target := range targets {
		targetCh <- target
	}
	close(targetCh)
	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	return joined
}

// cancelLiveActorJobs removes only occurrences that have not completed or dead-lettered
func cancelLiveActorJobs[T any](ctx context.Context, client actor.Client[T]) error {
	jobs, err := client.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Status.IsTerminal() {
			continue
		}
		err = client.DeleteJob(ctx, job.JobID, actor.WithLiveJobsOnly())
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			return fmt.Errorf("failed to cancel job %s: %w", job.JobID, err)
		}
	}
	return nil
}

// legacyChildType resolves a child reference omitted by an older journal only when the local graph is known to describe that journal
func (o *orchestrator) legacyChildType(st *instanceState, stepName string, index int) (string, error) {
	if !o.localDefinitionMatches(st) {
		return "", fmt.Errorf("%w: cannot resolve child %s[%d] from workflow version %d", ErrJournalIncompatible, stepName, index, st.Version)
	}

	child := o.childDefinitionFor(stepName, index)
	if child == nil {
		return "", fmt.Errorf("%w: cannot resolve child %s[%d]", ErrJournalIncompatible, stepName, index)
	}
	return child.baseType, nil
}

// legacyTaskActorType resolves a worker reference omitted by an older journal only when the local graph is known to describe that journal
func (o *orchestrator) legacyTaskActorType(st *instanceState, stepName string, index int, undo bool) (string, error) {
	if !o.localDefinitionMatches(st) {
		return "", fmt.Errorf("%w: cannot resolve task %s[%d] from workflow version %d", ErrJournalIncompatible, stepName, index, st.Version)
	}

	d := o.def.byName[stepName]
	member := memberDef(d, index)
	if member == nil {
		return "", fmt.Errorf("%w: cannot resolve task %s[%d]", ErrJournalIncompatible, stepName, index)
	}
	if undo {
		return o.wf.undoType(member.capability), nil
	}
	return o.wf.workerType(member.capability), nil
}

// localDefinitionMatches reports whether definition-derived cleanup targets are safe for this journal
func (o *orchestrator) localDefinitionMatches(st *instanceState) bool {
	if st.Version != o.def.version {
		return false
	}
	return st.DefinitionFingerprint == o.def.fingerprint
}

// bindLegacyCleanupDefinition verifies an omitted journal identity before definition-derived actor references are used
func (o *orchestrator) bindLegacyCleanupDefinition(ctx context.Context, st *instanceState) error {
	if !st.hasUnresolvedCleanupTargets() {
		return nil
	}
	if st.Version != o.def.version {
		return fmt.Errorf("%w: cleanup metadata is missing for workflow version %d", ErrJournalIncompatible, st.Version)
	}
	if st.DefinitionFingerprint != "" {
		if st.DefinitionFingerprint != o.def.fingerprint {
			return fmt.Errorf("%w: cleanup metadata belongs to another definition", ErrJournalIncompatible)
		}
		return nil
	}

	// A check-only lookup must find this graph already recorded because registering an unknown version could bless the wrong legacy definition
	request := registerRequest{Version: st.Version, Fingerprint: o.def.fingerprint}
	envelope, err := o.wf.registryPeek(ctx, o.svc, methodCheck, request)
	if err != nil {
		return fmt.Errorf("%w: failed to verify the legacy definition: %s", ErrJournalIncompatible, err.Error())
	}
	var response registerResponse
	err = envelope.Decode(&response)
	if err != nil {
		return fmt.Errorf("%w: failed to decode the legacy definition check: %s", ErrJournalIncompatible, err.Error())
	}
	if !response.Found || !response.OK {
		return fmt.Errorf("%w: registry does not confirm workflow version %d", ErrJournalIncompatible, st.Version)
	}

	st.DefinitionFingerprint = o.def.fingerprint
	return nil
}

// hasUnresolvedCleanupTargets reports whether purge would need to reconstruct any actor reference from the deployed graph
func (st *instanceState) hasUnresolvedCleanupTargets() bool {
	for i := range st.Steps {
		for j := range st.Steps[i].Tasks {
			tr := &st.Steps[i].Tasks[j]
			if tr.ChildID != "" {
				if tr.ChildType == "" {
					return true
				}
				continue
			}
			if tr.WorkerType == "" || (tr.Comp != nil && tr.UndoType == "") {
				return true
			}
		}
	}
	return false
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
						Version:   0,
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
	st.encoded = nil
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
