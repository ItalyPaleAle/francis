package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/tracing"
)

// orchestrator is the Workflow actor: one instance per workflow instance, holding the journal and deciding what happens next
//
// It orchestrates and performs nothing (§4): it reads and writes its own state, arms and drops its timers, and dispatches jobs
// No user code runs here at all, because the definition exposes no hook that does, so the orchestration boundary is enforced by the code rather than left to each application to keep
type orchestrator struct {
	wf         *Workflow
	def        *definition
	instanceID string
	svc        *actor.Service
	log        *slog.Logger
	// client is a privileged client bound to this instance, which is how a built-in actor reaches its own state, timers, and jobs
	client actor.Client[instanceState]
	// armedDeadline is what this activation last wrote to the deadline alarm, so an unchanged deadline costs no second write on the same row
	// A fresh activation starts empty and therefore re-arms once, which is also how an alarm someone deleted comes back
	armedDeadline time.Time
}

// newOrchestrator builds the Workflow actor for one instance
func newOrchestrator(wf *Workflow, instanceID string, svc *actor.Service) actor.Actor {
	log := wf.log
	if log != nil {
		log = log.With(slog.String("instanceID", instanceID))
	}

	return &orchestrator{
		wf:         wf,
		def:        wf.def,
		instanceID: instanceID,
		svc:        svc,
		log:        log,
		client:     builtinactor.NewClient[instanceState](wf.baseType, instanceID, svc),
	}
}

// Job handles every durable job delivered to the instance, which is everything that drives it forward
func (o *orchestrator) Job(ctx context.Context, method string, data actor.Envelope) error {
	// A host whose code does not match the graph registered for the version declines the job so it re-routes, rather than advancing an instance against a definition it does not have
	ok, err := o.wf.serveVersion(ctx, o.svc, o.def.version)
	if err != nil {
		return err
	}
	if !ok {
		return actor.ErrJobRejected
	}

	ev, err := o.decodeEvent(method, data)
	if err != nil {
		return err
	}

	return o.turn(ctx, ev)
}

// Alarm handles the instance's single deadline alarm, which is the backstop that guarantees an instance terminates
func (o *orchestrator) Alarm(ctx context.Context, name string, _ actor.Envelope) error {
	if name != alarmDeadline {
		return nil
	}

	// The alarm is delivered to this actor on whatever host holds it, so a host without the instance's version cannot simply decline it forever
	// It works from the journal alone instead, which carries the full step list for exactly this reason (§14.4)
	ok, err := o.wf.serveVersion(ctx, o.svc, o.def.version)
	if err != nil {
		return err
	}
	if !ok {
		return o.handleUnknownVersionDeadline(ctx)
	}

	return o.turn(ctx, &event{kind: evDeadline})
}

// Invoke handles the operations a caller drives synchronously, which are the ones whose result the caller needs
func (o *orchestrator) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	switch method {
	case methodPurge:
		return o.purge(ctx)
	default:
		// Only the engine invokes this actor, so an unknown method is a programming error
		return nil, fmt.Errorf("unknown workflow method %q", method)
	}
}

// Peek serves status reads, which run concurrently with each other and only ever queue behind a write turn
func (o *orchestrator) Peek(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	switch method {
	case methodStatus:
		return o.status(ctx)
	default:
		return nil, fmt.Errorf("unknown workflow peek method %q", method)
	}
}

// JobFailed reacts to one of the instance's own jobs being dead-lettered, which means a report or an event could not be delivered
// The payload it carried is gone, so this cannot reconstruct it; what it can do is force a turn, whose reconcile re-dispatches everything the journal still says is outstanding
func (o *orchestrator) JobFailed(ctx context.Context, jobID string, method string, _ actor.Envelope, jobErr error) error {
	o.wf.metrics.transportFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", o.def.name),
		attribute.String("method", method),
	))

	if o.log != nil {
		o.log.WarnContext(ctx, "Workflow job was dead-lettered", slog.String("jobID", jobID), slog.String("method", method), slog.Any("error", jobErr))
	}

	// Arming the deadline for now runs a turn without adding a second timer
	err := o.client.SetAlarm(ctx, alarmDeadline, actor.AlarmProperties{DueTime: time.Now()})
	if err != nil {
		return fmt.Errorf("failed to arm the deadline after a dead-letter: %w", err)
	}
	return nil
}

// decodeEvent turns a delivered job into the event the turn folds into the journal
func (o *orchestrator) decodeEvent(method string, data actor.Envelope) (*event, error) {
	switch method {
	case methodStart:
		var p startPayload
		err := decodePayload(data, &p)
		if err != nil {
			return nil, err
		}
		return &event{kind: evStart, start: &p}, nil

	case methodDone:
		var p reportPayload
		err := decodePayload(data, &p)
		if err != nil {
			return nil, err
		}
		return &event{kind: evDone, report: &p}, nil

	case methodCompensated:
		var p compReportPayload
		err := decodePayload(data, &p)
		if err != nil {
			return nil, err
		}
		return &event{kind: evCompensated, comp: &p}, nil

	case methodEvent:
		var p eventPayload
		err := decodePayload(data, &p)
		if err != nil {
			return nil, err
		}
		return &event{kind: evRaise, raise: &p}, nil

	case methodCancel, methodUnwind, methodSuspend:
		var p reasonPayload
		err := decodePayload(data, &p)
		if err != nil {
			return nil, err
		}
		kind := evCancel
		switch method {
		case methodUnwind:
			kind = evUnwind
		case methodSuspend:
			kind = evSuspend
		}
		return &event{kind: kind, reason: p.Reason, fromParent: p.FromParent, compAttempt: p.CompAttempt}, nil

	case methodResume:
		return &event{kind: evResume}, nil

	default:
		// A method this actor does not know would retry forever, so it fails permanently instead
		return nil, fmt.Errorf("%w: unknown workflow job method %q", actor.ErrJobPermanentFailure, method)
	}
}

// turn is the instance's single write path, and every event runs the same four phases (§7.2)
func (o *orchestrator) turn(ctx context.Context, ev *event) (err error) {
	ctx, span := tracing.Start(ctx, "workflow.turn", trace.WithAttributes(
		attribute.String("francis.workflow.name", o.def.name),
		attribute.String("francis.workflow.instance", o.instanceID),
		attribute.String("francis.workflow.event", string(ev.kind)),
	))
	start := time.Now()
	defer func() {
		o.wf.metrics.turnDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			attribute.String("workflow", o.def.name),
		))
		tracing.End(span, err)
	}()

	// Phase 1: the journal is the source of truth, and what it says decides whether this turn runs at all
	st, err := o.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the workflow journal: %w", err)
	}

	run, err := o.admits(&st, ev)
	if err != nil || !run {
		return err
	}

	now := time.Now()

	// Phase 2: fold the event into the journal
	// A duplicate, or a report for a task the journal already has an outcome for, records nothing here, including this very turn being retried after its SetState succeeded and its reconcile failed
	duplicate := apply(&st, o.def, ev, now)
	if duplicate {
		o.wf.metrics.duplicateEvents.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow", o.def.name),
			attribute.String("event", string(ev.kind)),
		))
	}

	// A start whose instance was purged while its job was in flight leaves an empty journal, and there is nothing left to run
	if st.Status == "" {
		return nil
	}

	// The deadline acts on the journal rather than folding an event of its own, so resolving it happens here
	o.recover(&st, ev, now)

	// The step statuses are snapshotted before advance so the turn can tell which steps it settled, which is what the step-duration histogram measures
	before := stepStatuses(&st)

	// Phase 3: advance the cursor as far as the journal allows, which is a pure function of the journal and the definition
	advance(&st, o.def, o.instanceID, now)

	// Phase 4a: the journal is durable before anything is scheduled, so a lost dispatch is always recoverable and an orphan result never is
	// The workflow labels are written in the same operation as the state, so the listing index can never disagree with the journal
	err = o.persist(ctx, &st, now)
	if err != nil {
		return err
	}

	o.recordTransitions(ctx, &st, ev, before)

	// Phase 4b: everything the journal says should be running is dispatched, idempotently
	// This runs on every turn, including the ones that recorded nothing, so a turn that persisted a result and then failed to dispatch cannot stall the instance forever
	return o.reconcile(ctx, &st, now)
}

// admits reports whether this turn should run at all, given what the journal says about the instance
// It returns false with no error for an event there is simply nothing to do about, and an error for one this host should not be the one to handle
func (o *orchestrator) admits(st *instanceState, ev *event) (bool, error) {
	// A terminated instance ignores everything but the unwind a parent sends, which is what moves a completed child back into compensating
	if st.Status.IsTerminal() && ev.kind != evUnwind {
		return false, nil
	}

	// The journal is created by the start job alone, so a control job that raced ahead of it waits rather than inventing an instance out of nothing
	// Returning an ordinary error has Francis retry the job, which is how a Suspend issued the moment after Start still lands
	if st.Status == "" && ev.kind != evStart {
		return false, errWaitingForStart
	}

	// An instance whose version this host cannot serve is left for a host that can, which is what drains old instances onto old hosts
	if st.Version > 0 && st.Version != o.def.version {
		return false, actor.ErrJobRejected
	}

	return true, nil
}

// recover is the part of a turn only the deadline drives: resolving an elapsed deadline against the journal
// One alarm stands for every deadline the instance has, so which one elapsed is resolved from the journal rather than carried on the event
func (o *orchestrator) recover(st *instanceState, ev *event, now time.Time) {
	if ev.kind != evDeadline {
		return
	}

	o.applyElapsedDeadlines(st, now)
}

// persist writes the journal, its workflow labels, and its retention TTL in one operation, and fails the instance rather than letting it outgrow what it can store
func (o *orchestrator) persist(ctx context.Context, st *instanceState, now time.Time) error {
	opts := &actor.SetStateOpts{}
	opts.SetWorkflowLabels(builtinkey.Key{}, o.labels(st))

	// A terminated journal is written with a TTL of twice its retention, so an instance whose sweep never runs still expires while the sweep can still find what it needs to clean up
	if st.Status.IsTerminal() {
		opts.TTL = 2 * o.def.retention.forStatus(st.Status)
	}

	// The size is checked before the write, because an instance that can no longer persist can no longer progress, and failing it is a much better outcome
	size, err := journalSize(st)
	if err != nil {
		return fmt.Errorf("failed to measure the workflow journal: %w", err)
	}
	if size > o.def.maxJournalSize {
		failForOversizedJournal(st, size, o.def.maxJournalSize, now)
		opts.SetWorkflowLabels(builtinkey.Key{}, o.labels(st))
		opts.TTL = 2 * o.def.retention.forStatus(st.Status)
	}

	err = o.client.SetState(ctx, *st, opts)
	if err != nil {
		return fmt.Errorf("failed to write the workflow journal: %w", err)
	}
	return nil
}

// labels are what makes "list the running instances" a range scan on an indexed column rather than a walk of every retained journal
func (o *orchestrator) labels(st *instanceState) components.WorkflowLabels {
	labels := components.WorkflowLabels{
		Status:  string(st.Status),
		Version: st.Version,
	}
	if st.Parent != nil {
		labels.Parent = st.Parent.InstanceID
	}
	return labels
}

// recordTransitions emits the instrument updates a turn's outcome calls for, after the journal that justifies them is durable
func (o *orchestrator) recordTransitions(ctx context.Context, st *instanceState, ev *event, before map[string]StepStatus) {
	attrs := metric.WithAttributes(attribute.String("workflow", o.def.name))

	// A step that settled during this turn is timed once, from the records that now carry both of its timestamps
	for i := range st.Steps {
		sr := &st.Steps[i]
		if before[sr.Name] == sr.Status || sr.StartedAt.IsZero() || sr.CompletedAt.IsZero() {
			continue
		}
		switch sr.Status {
		case StepCompleted, StepFailed, StepSkipped:
			o.wf.metrics.stepDuration.Record(ctx, sr.CompletedAt.Sub(sr.StartedAt).Seconds(), metric.WithAttributes(
				attribute.String("workflow", o.def.name),
				attribute.String("step", sr.Name),
				attribute.String("outcome", string(sr.Status)),
			))
		case StepCompensated, StepCompensationFailed:
			o.wf.metrics.compensationsFailed.Add(ctx, boolToInt(sr.Status == StepCompensationFailed), metric.WithAttributes(
				attribute.String("workflow", o.def.name),
				attribute.String("step", sr.Name),
			))
		}
	}

	if ev.kind == evStart && st.Status == StatusRunning {
		o.wf.metrics.instancesStarted.Add(ctx, 1, attrs)
		o.wf.metrics.instancesRunning.Add(ctx, 1, attrs)
	}
	if ev.kind == evSuspend && st.Status == StatusSuspended {
		o.wf.metrics.instancesSuspended.Add(ctx, 1, attrs)
	}

	if !st.Status.IsTerminal() || st.CompletedAt.IsZero() {
		return
	}

	o.wf.metrics.instancesRunning.Add(ctx, -1, attrs)
	o.wf.metrics.instancesTerminated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", o.def.name),
		attribute.String("status", string(st.Status)),
	))
	if !st.StartedAt.IsZero() {
		o.wf.metrics.instanceDuration.Record(ctx, st.CompletedAt.Sub(st.StartedAt).Seconds(), metric.WithAttributes(
			attribute.String("workflow", o.def.name),
			attribute.String("status", string(st.Status)),
		))
	}

	if o.log != nil {
		o.log.InfoContext(ctx, "Workflow instance terminated",
			slog.String("status", string(st.Status)),
			slog.String("compensation", string(st.Compensation)),
			slog.String("cause", st.Cause),
		)
	}
}

// reconcile derives the set of tasks that should be in flight from the journal and dispatches each one with a stable idempotency key
// It is the only thing that schedules work, it is safe to run at any time, and it is a no-op while the instance is suspended
func (o *orchestrator) reconcile(ctx context.Context, st *instanceState, now time.Time) error {
	// A terminated instance reports to its parent, drops its timers, and lets go of its activation, so a wide fan-out does not hold one per instance after it is done
	if st.Status.IsTerminal() {
		err := o.reportToParent(ctx, st)
		if err != nil {
			return err
		}
		return o.finish(ctx)
	}

	// Suspension is the promise not to start anything, which is the whole of the mechanism: the journal keeps recording the truth and only the scheduling half of the turn is gated
	if st.Status == StatusSuspended {
		return o.dropDeadline(ctx)
	}

	err := o.armDeadline(ctx, st)
	if err != nil {
		return err
	}

	for i := range st.Steps {
		sr := &st.Steps[i]
		d := o.def.byName[sr.Name]
		if d == nil {
			continue
		}

		switch sr.Status {
		case StepRunning:
			err = o.dispatchForward(ctx, st, sr, d, now)
		case StepCompensating:
			err = o.dispatchCompensations(ctx, st, sr, d, now)
		default:
			// A step that settled while tasks were still outstanding leaves pending jobs behind, which are cancelled so the work that has not started never does
			// Cancelling is best-effort: an abandoned task costs one slot at worst, which is not worth failing a turn over
			o.cancelOutstanding(ctx, sr, d)
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// dispatchForward dispatches every forward task the journal says should be running, within the fan-out's sliding window
func (o *orchestrator) dispatchForward(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, now time.Time) error {
	// A wait step has no task to dispatch: it is completed by RaiseEvent or failed by its own timeout
	if d.kind == KindWait {
		return nil
	}

	// The window admits the first tasks in index order that are not yet done, and slides as results arrive
	// A redispatch of a task that is already pending or running coalesces on its key, so no in-flight flag is needed and none is kept
	window := d.maxParallel
	var admitted int
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Done {
			continue
		}
		if window > 0 && admitted >= window {
			break
		}
		admitted++

		err := o.dispatchTask(ctx, st, sr, d, tr, now)
		if err != nil {
			return err
		}
	}
	return nil
}

// dispatchTask dispatches one attempt of one task, which is a job to a worker or the start of a child instance
func (o *orchestrator) dispatchTask(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, tr *taskRecord, now time.Time) error {
	member := memberDef(d, tr.Index)

	if member.kind == KindChild || (d.kind == KindForEach && d.child != nil) {
		return o.startChild(ctx, st, sr, d, member, tr)
	}

	payload := o.buildRunPayload(st, sr, d, member, tr)

	opts := []actor.JobOption{actor.WithIdempotencyKey(methodRun + idDelimiter + strconv.Itoa(tr.Attempts))}
	if !tr.RetryAt.IsZero() && tr.RetryAt.After(now) {
		opts = append(opts, actor.WithJobDueTime(tr.RetryAt))
	}

	client := builtinactor.NewClient[struct{}](o.wf.workerType(member.capability), workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
	_, err := client.Dispatch(ctx, methodRun, payload, opts...)
	if err != nil {
		return fmt.Errorf("failed to dispatch task %s[%d]: %w", sr.Name, tr.Index, err)
	}
	return nil
}

// startChild starts the child instance a child task runs, at an ID derived from the parent's so a retried turn finds the same child rather than starting a second
func (o *orchestrator) startChild(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, member *stepDef, tr *taskRecord) error {
	child := member.child
	if child == nil {
		child = d.child
	}
	if child == nil {
		return fmt.Errorf("step %q is a child step with no definition", sr.Name)
	}

	// The depth travels with the start, and the child refuses it if its own definition says the chain is too deep
	depth := 1
	if st.Parent != nil {
		depth = st.Parent.Depth + 1
	}

	// The child's input is what this task would have received, so a child step reads its parent's data exactly as a plain step does
	payload := startPayload{
		Input:   o.childInput(st, sr, d, tr),
		Version: child.def.version,
		Parent: &parentRef{
			InstanceID: o.instanceID,
			Workflow:   o.def.name,
			Step:       sr.Name,
			Index:      tr.Index,
			Depth:      depth,
		},
		TraceParent: st.TraceParent,
		CreatedAt:   time.Now(),
		Attempt:     tr.Attempts,
	}

	client := builtinactor.NewClient[struct{}](child.baseType, tr.ChildID, o.svc)
	_, err := client.Dispatch(ctx, methodStart, payload, actor.WithIdempotencyKey(methodStart))
	if err != nil {
		return fmt.Errorf("failed to start child %s for %s[%d]: %w", child.name, sr.Name, tr.Index, err)
	}

	o.wf.metrics.childrenStarted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", o.def.name),
		attribute.String("child", child.name),
	))
	return nil
}

// dispatchCompensations dispatches every compensation of the frame being unwound, which run concurrently because the tasks had no order between them going forward
func (o *orchestrator) dispatchCompensations(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, now time.Time) error {
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Comp == nil || tr.Comp.Done {
			continue
		}

		member := memberDef(d, tr.Index)

		// Compensating a child means asking the child to undo itself, rather than running a handler here
		if member.kind == KindChild || (d.kind == KindForEach && d.child != nil) {
			err := o.unwindChild(ctx, st, sr.Name, tr)
			if err != nil {
				return err
			}
			continue
		}

		payload := o.buildRunPayload(st, sr, d, member, tr)
		payload.Attempt = tr.Comp.Attempts
		payload.Result = tr.Output
		payload.Cause = st.Cause

		opts := []actor.JobOption{actor.WithIdempotencyKey(methodCompensate + idDelimiter + strconv.Itoa(tr.Comp.Attempts))}
		if !tr.Comp.RetryAt.IsZero() && tr.Comp.RetryAt.After(now) {
			opts = append(opts, actor.WithJobDueTime(tr.Comp.RetryAt))
		}

		// The compensation runs on the undo queue of the same capability the forward task had, since the undo almost always needs the placement the forward task ran on
		client := builtinactor.NewClient[struct{}](o.wf.undoType(member.capability), workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
		_, err := client.Dispatch(ctx, methodCompensate, payload, opts...)
		if err != nil {
			return fmt.Errorf("failed to dispatch compensation %s[%d]: %w", sr.Name, tr.Index, err)
		}

		o.wf.metrics.compensationsRun.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow", o.def.name),
			attribute.String("step", sr.Name),
		))
	}
	return nil
}

// unwindChild asks a child instance to undo itself, cancelling one that is still running and unwinding one that already completed
func (o *orchestrator) unwindChild(ctx context.Context, st *instanceState, stepName string, tr *taskRecord) error {
	child := o.childDefinitionFor(stepName, tr.Index)
	if child == nil {
		return nil
	}

	// A child that is still running is cancelled and unwinds its own stack, while one that already completed is moved back into compensating
	method := methodUnwind
	if !tr.Done || tr.Error != "" {
		method = methodCancel
	}

	// The parent's own compensation attempt number travels with the request, so the child's report lands on the record being unwound
	attempt := 1
	if tr.Comp != nil {
		attempt = tr.Comp.Attempts
	}

	client := builtinactor.NewClient[struct{}](child.baseType, tr.ChildID, o.svc)
	_, err := client.Dispatch(ctx, method, reasonPayload{Reason: st.Cause, FromParent: true, CompAttempt: attempt},
		actor.WithIdempotencyKey(method+idDelimiter+strconv.Itoa(attempt)))
	if err != nil {
		return fmt.Errorf("failed to unwind child %s: %w", tr.ChildID, err)
	}
	return nil
}

// childDefinitionFor resolves the definition a child task runs, from the step that declared it
func (o *orchestrator) childDefinitionFor(stepName string, index int) *Workflow {
	d := o.def.byName[stepName]
	if d == nil {
		return nil
	}

	member := memberDef(d, index)
	if member.child != nil {
		return member.child
	}
	return d.child
}

// cancelOutstanding removes the pending jobs of a step that settled while some of its tasks had not reported
// A job that is already executing is not interrupted, so a task that was running finishes and its late report is recorded like any other result
func (o *orchestrator) cancelOutstanding(ctx context.Context, sr *stepRecord, d *stepDef) {
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]

		// A task an unwind abandoned is done as far as the journal is concerned, but its job may still be waiting to run, and there is no reason to let it
		if tr.Done && !tr.Abandoned {
			continue
		}

		member := memberDef(d, tr.Index)
		client := builtinactor.NewClient[struct{}](o.wf.workerType(member.capability), workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
		jobs, err := client.ListJobs(ctx)
		if err != nil {
			// A listing that fails is not worth failing the turn over, since the abandoned task costs one slot at worst
			continue
		}

		for _, j := range jobs {
			if j.Status.IsTerminal() {
				continue
			}
			_ = client.DeleteJob(ctx, j.JobID)
		}
	}
}

// finish drops the instance's timers and lets go of its activation, once it has terminated
func (o *orchestrator) finish(ctx context.Context) error {
	err := o.client.DeleteAlarm(ctx, alarmDeadline)
	if err != nil && !errors.Is(err, actor.ErrAlarmNotFound) {
		return fmt.Errorf("failed to drop the deadline: %w", err)
	}

	o.client.Halt()
	return nil
}

// armDeadline writes the instance's single deadline alarm, and only when it differs from what this activation already armed
// Alarms are replaceable by name, so recomputing is one write, and skipping the unchanged case stops a wide fan-out's reports from each costing a second write on the same row
// Re-arming from inside the alarm's own handler is safe because Francis completes alarms by lease rather than by name, so a replaced alarm is treated as already handled
func (o *orchestrator) armDeadline(ctx context.Context, st *instanceState) error {
	due := st.DeadlineAt
	if due.IsZero() {
		return nil
	}
	if due.Equal(o.armedDeadline) {
		return nil
	}

	err := o.client.SetAlarm(ctx, alarmDeadline, actor.AlarmProperties{DueTime: due})
	if err != nil {
		return fmt.Errorf("failed to arm the deadline: %w", err)
	}

	o.armedDeadline = due
	return nil
}

// dropDeadline removes the deadline alarm, which is what pausing an instance's deadlines amounts to
func (o *orchestrator) dropDeadline(ctx context.Context) error {
	err := o.client.DeleteAlarm(ctx, alarmDeadline)
	if err != nil && !errors.Is(err, actor.ErrAlarmNotFound) {
		return fmt.Errorf("failed to drop the deadline: %w", err)
	}
	o.armedDeadline = time.Time{}
	return nil
}

// applyElapsedDeadlines resolves a fired deadline against the journal, since one alarm stands for every deadline the instance has
// What a timeout costs depends on the step it hit, which the per-step policies decide exactly as a handler failure would
func (o *orchestrator) applyElapsedDeadlines(st *instanceState, now time.Time) {
	// The instance timeout ends the run, whatever it was doing
	instanceDue := instanceDeadline(st, o.def)
	if !instanceDue.IsZero() && !now.Before(instanceDue) {
		if st.Status != StatusCompensating {
			beginUnwind(st, o.def, "instance timeout elapsed", StatusFailed, now)
		}
		return
	}

	sr, d := currentRunningStep(st, o.def)
	if sr == nil || d == nil || sr.Status != StepRunning {
		return
	}

	stepDue := stepDeadline(sr, d)
	if stepDue.IsZero() || now.Before(stepDue) {
		return
	}

	// A wait step that never got its event is the one case where the step's timeout ends the run rather than failing a task
	if d.kind == KindWait {
		beginUnwind(st, o.def, fmt.Sprintf("event %q timed out", d.effectiveEventName()), StatusFailed, now)
		return
	}

	// Every outstanding attempt of the timed-out step is failed, and the step's own policy decides what that costs the instance
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Done {
			continue
		}
		tr.Error = fmt.Sprintf("step %q timed out", sr.Name)
		tr.LastError = tr.Error
		tr.Done = true
		tr.CompletedAt = now
		if sr.Remaining > 0 {
			sr.Remaining--
		}
	}
}

// handleUnknownVersionDeadline runs the deadline on a host that does not have the instance's version, working from the journal alone
func (o *orchestrator) handleUnknownVersionDeadline(ctx context.Context) error {
	st, err := o.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the workflow journal: %w", err)
	}
	if st.Status == "" || st.Status.IsTerminal() {
		return nil
	}

	now := time.Now()
	instanceDue := instanceDeadline(&st, o.def)
	timedOut := !instanceDue.IsZero() && !now.Before(instanceDue)

	// Failing an instance no host can serve is only worth doing once its own timeout has elapsed, and it is done without compensation, since no host can run the compensations either
	if o.def.unknownVersion == FailUnknownVersion && timedOut {
		st.Status = StatusFailed
		st.Compensation = CompensationNone
		st.Cause = fmt.Sprintf("unknown version %d", st.Version)
		st.CompletedAt = now
		for i := range st.Steps {
			if st.Steps[i].Status == StepPending || st.Steps[i].Status == StepRunning {
				st.Steps[i].Status = StepSkipped
			}
		}
		err = o.persist(ctx, &st, now)
		if err != nil {
			return err
		}
		return o.finish(ctx)
	}

	// Parking re-arms the alarm and waits for a host that can serve the version, which is what List(Version: v) shows an operator
	if o.log != nil {
		o.log.WarnContext(ctx, "Workflow deadline fired on a host without the instance's version; parking",
			slog.Int("instanceVersion", st.Version), slog.Int("hostVersion", o.def.version))
	}
	return o.client.SetAlarm(ctx, alarmDeadline, actor.AlarmProperties{DueTime: now.Add(defaultParkInterval)})
}

// memberDef resolves the definition that governs one task of a step, which for a parallel group is the member that runs it
func memberDef(d *stepDef, index int) *stepDef {
	if d.kind == KindParallel && index >= 0 && index < len(d.members) {
		return d.members[index]
	}
	return d
}

// decodePayload reads a job's payload, treating an absent one as the zero value so a method that carries no data needs no special case
func decodePayload(data actor.Envelope, into any) error {
	if data == nil {
		return nil
	}

	err := data.Decode(into)
	if err != nil {
		// A payload that cannot be decoded fails the same way on every attempt, so retrying it would only waste attempts
		return fmt.Errorf("%w: failed to decode the job payload: %w", actor.ErrJobPermanentFailure, err)
	}
	return nil
}

// journalSize returns the encoded size of a journal, which is what the cap is measured against
func journalSize(st *instanceState) (int, error) {
	enc, err := json.Marshal(st)
	if err != nil {
		return 0, err
	}
	return len(enc), nil
}

// failForOversizedJournal ends an instance that outgrew what it can store, without compensation, since the unwind would need more journal than there is room for
func failForOversizedJournal(st *instanceState, size int, limit int, now time.Time) {
	st.Status = StatusFailed
	st.Compensation = CompensationNone
	st.Cause = fmt.Sprintf("%s: %d bytes exceeds the %d byte limit", ErrJournalTooLarge.Error(), size, limit)
	st.CompletedAt = now
	st.Stack = nil
	for i := range st.Steps {
		if st.Steps[i].Status == StepPending {
			st.Steps[i].Status = StepSkipped
		}
	}
}

// stepStatuses snapshots the status of every step, so a turn can tell which steps it settled
func stepStatuses(st *instanceState) map[string]StepStatus {
	out := make(map[string]StepStatus, len(st.Steps))
	for i := range st.Steps {
		out[st.Steps[i].Name] = st.Steps[i].Status
	}
	return out
}

// boolToInt renders a condition as the counter increment it stands for, which keeps a conditional Add to one line
func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
