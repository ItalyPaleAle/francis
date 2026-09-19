package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/italypaleale/go-kit/utils"
	msgpack "github.com/vmihailenco/msgpack/v5"
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
// It orchestrates and performs nothing: it reads and writes its own state, arms and drops its timers, and dispatches jobs
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
	// Starts carry registry authorization while later deliveries are fenced against the immutable identity in the journal
	ev, err := o.decodeEvent(method, data)
	if err != nil {
		return err
	}
	if ev.kind == evStart {
		err = o.authorizeStart(ctx, ev.start)
		if errors.Is(err, errVersionNotServed) || errors.Is(err, ErrDefinitionConflict) {
			return actor.ErrJobRejected
		}
		if err != nil {
			return err
		}
	}

	// A job for a version this host does not serve goes back to Francis to be re-routed, without counting an attempt
	err = o.turn(ctx, ev)
	if errors.Is(err, errVersionNotServed) || errors.Is(err, ErrDefinitionConflict) {
		return actor.ErrJobRejected
	}
	return err
}

// authorizeStart upgrades legacy start payloads while preserving the identity captured by a new dispatch
func (o *orchestrator) authorizeStart(ctx context.Context, p *startPayload) error {
	if p.Version > 0 && p.Version != o.def.version {
		return errVersionNotServed
	}
	if p.DefinitionFingerprint != "" && p.DefinitionFingerprint != o.def.fingerprint {
		o.wf.recordDefinitionConflict(ctx, o.def.version, o.def.fingerprint, registerResponse{Fingerprint: p.DefinitionFingerprint})
		return errVersionNotServed
	}
	if p.DefinitionFingerprint != "" && p.RegistryGeneration > 0 {
		return nil
	}

	// A retried legacy start must retain a journal identity granted before a possible registry reset
	st, err := o.client.GetState(ctx)
	if err != nil {
		return err
	}
	if st.DefinitionFingerprint != "" && st.RegistryGeneration > 0 {
		p.DefinitionFingerprint = st.DefinitionFingerprint
		p.RegistryGeneration = st.RegistryGeneration
		return nil
	}
	identity, err := o.wf.authorizeDefinition(ctx, o.svc)
	if err != nil {
		return err
	}
	p.Version = o.def.version
	p.DefinitionFingerprint = identity.Fingerprint
	p.RegistryGeneration = identity.Generation
	return nil
}

// Alarm handles the instance's deadline and retains its recurrence when a transient failure prevents the turn from finishing
func (o *orchestrator) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	if name != alarmDeadline {
		return nil
	}

	// A delivered occurrence must be replaced even when its journal deadline has not changed
	o.armedDeadline = time.Time{}

	// Legacy one-shot alarms acquire a durable fallback before reaching registry or journal operations that may fail
	var p deadlinePayload
	err := decodePayload(data, &p)
	if err != nil {
		return err
	}
	if !p.Recurring {
		err = o.client.SetAlarm(ctx, alarmDeadline, deadlineAlarmProperties(time.Now().Add(orchestratorRetryDelay)))
		if err != nil {
			return fmt.Errorf("failed to migrate the workflow deadline to a recurring alarm: %w", err)
		}
	}

	// A failed turn leaves the recurring row intact, so runtime completion schedules its next occurrence without consuming a finite retry budget
	err = o.runDeadline(ctx)
	if err != nil && o.log != nil {
		o.log.WarnContext(ctx, "Workflow deadline turn failed; its recurring alarm will retry", slog.Any("error", err))
	}
	return nil
}

// runDeadline repairs durable event delivery before evaluating timeouts so recovery retains the original request payloads
func (o *orchestrator) runDeadline(ctx context.Context) error {
	// The dead-letter store survives a failed best-effort callback, including a start that has not created its journal yet
	jobs, err := o.client.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("failed to inspect workflow deliveries: %w", err)
	}
	var recovered, pendingStart bool
	for _, job := range jobs {
		pendingStart = pendingStart || (job.Method == methodStart && !job.Status.IsTerminal())
		if job.Status != actor.JobStatusDeadLettered || !retryWorkflowDelivery(job.Method, job.LastError) {
			continue
		}
		_, err = o.client.RetryJob(ctx, job.JobID)
		if errors.Is(err, actor.ErrJobNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to recover workflow delivery %s: %w", job.JobID, err)
		}
		recovered = true
	}
	if recovered {
		return o.client.SetAlarm(ctx, alarmDeadline, deadlineAlarmProperties(time.Now().Add(orchestratorRetryDelay)))
	}

	// Completed, missing, and suspended journals must explicitly stop the fallback recurrence
	st, err := o.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the workflow journal: %w", err)
	}
	if st.Status == "" {
		if pendingStart {
			return nil
		}
		return o.dropDeadline(ctx)
	}
	if st.Status.IsTerminal() {
		st = st.clone()
		// A start may have become terminal in its first write, but its parent must not see an outcome before the registry decision is durable
		err = o.confirmStart(ctx, &st, time.Now())
		if err != nil {
			return err
		}
		err = o.reportToParent(ctx, &st, false)
		if err != nil {
			return err
		}
		return o.finish(ctx)
	}
	if st.Status == StatusSuspended {
		return o.dropDeadline(ctx)
	}

	// The journal may be stamped with a version this host does not serve even when this host's own version is fine, and the deadline follows the configured policy rather than being declined forever
	err = o.turn(ctx, &event{kind: evDeadline})
	if errors.Is(err, errVersionNotServed) || errors.Is(err, ErrDefinitionConflict) {
		return o.handleUnknownVersionDeadline(ctx)
	}
	return err
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

// JobFailed restores an exhausted delivery from its durable record so starts, control requests, external events, and reports retain their original payloads
func (o *orchestrator) JobFailed(ctx context.Context, jobID string, method string, _ actor.Envelope, jobErr error) error {
	o.wf.metrics.transportFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", o.def.name),
		attribute.String("method", method),
	))

	if o.log != nil {
		o.log.WarnContext(ctx, "Workflow job was dead-lettered", slog.String("jobID", jobID), slog.String("method", method), slog.Any("error", jobErr))
	}

	// Preserve a recurring repair path before trying the best-effort replay itself
	err := o.client.SetAlarm(ctx, alarmDeadline, deadlineAlarmProperties(time.Now()))
	if err != nil {
		return fmt.Errorf("failed to arm the deadline after a dead-letter: %w", err)
	}
	o.armedDeadline = time.Time{}

	// Permanent payload failures remain inspectable while transient delivery failures replay atomically with their original bytes
	lastError := ""
	if jobErr != nil {
		lastError = jobErr.Error()
	}
	if !retryWorkflowDelivery(method, lastError) {
		return nil
	}
	_, err = o.client.RetryJob(ctx, jobID)
	if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
		return fmt.Errorf("failed to recover workflow delivery %s: %w", jobID, err)
	}
	return nil
}

// retryWorkflowDelivery excludes malformed or unknown requests while preserving engine-owned transient failures
func retryWorkflowDelivery(method string, lastError string) bool {
	if strings.Contains(lastError, actor.ErrJobPermanentFailure.Error()) {
		return false
	}
	switch method {
	case methodStart, methodDone, methodCompensated, methodEvent, methodCancel, methodUnwind, methodSuspend, methodResume:
		return true
	default:
		return false
	}
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

// turn is the instance's single write path, and every event runs the same four phases
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
	st = st.clone()

	if st.Version == o.def.version && st.DefinitionFingerprint != "" && st.DefinitionFingerprint != o.def.fingerprint {
		o.wf.recordDefinitionConflict(ctx, o.def.version, o.def.fingerprint, registerResponse{Fingerprint: st.DefinitionFingerprint})
	}
	run, err := o.admits(&st, ev)
	if err != nil || !run {
		return err
	}
	if st.Status != "" && st.DefinitionFingerprint == "" {
		// A legacy journal can bind once to its registered graph because Forget checks journal existence under the same registry lock
		identity, authErr := o.wf.authorizeDefinition(ctx, o.svc)
		if authErr != nil {
			return authErr
		}
		st.DefinitionFingerprint = identity.Fingerprint
		st.RegistryGeneration = identity.Generation
		st.RegistryConfirmed = true
	}
	if st.Status != "" {
		// Matching code can fill missing legacy policies once, while mismatched hosts must never invent them
		st.Timeout = utils.PositiveOr(st.Timeout, o.def.timeout)
		if st.UnknownVersion == "" {
			st.UnknownVersion = o.def.unknownVersion
		}
	}

	now := st.deadlineTurnTime(ev, time.Now())
	statusBefore := st.Status

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
	before := st.stepStatuses()

	// Phase 3: advance the cursor as far as the journal allows, which is a pure function of the journal and the definition
	advance(&st, o.def, o.instanceID, now)

	// A terminal status is not published until queued forward work has been removed, so a cleanup failure leaves the triggering event retryable against the prior journal
	if st.Status.IsTerminal() {
		err = o.cancelAllOutstanding(ctx, &st)
		if err != nil {
			return err
		}
	}

	// Phase 4a: the journal is durable before anything is scheduled, so a lost dispatch is always recoverable and an orphan result never is
	// The workflow labels are written in the same operation as the state, so the listing index can never disagree with the journal
	err = o.persist(ctx, &st, now)
	if err != nil {
		return err
	}

	// Confirmation is ordered after the first journal write and before any work dispatch, closing the reset race without cross-actor transactions
	err = o.confirmStart(ctx, &st, now)
	if err != nil {
		return err
	}

	o.recordTransitions(ctx, &st, ev, statusBefore, before)

	// Phase 4b: everything the journal says should be running is dispatched, idempotently
	// This runs on every turn, including the ones that recorded nothing, so a turn that persisted a result and then failed to dispatch cannot stall the instance forever
	forceParentReport := ev.kind == evStart && st.Status.IsTerminal() && st.Parent != nil
	err = o.reconcile(ctx, &st, now, forceParentReport)
	if err != nil {
		return err
	}

	return nil
}

// admits reports whether this turn should run at all, given what the journal says about the instance
// It returns false with no error for an event there is simply nothing to do about, and an error for one this host should not be the one to handle
func (o *orchestrator) admits(st *instanceState, ev *event) (bool, error) {
	// Identity fences precede terminal shortcuts because a late result can reopen a journal and dispatch compensations
	if st.Version > 0 && st.Version != o.def.version {
		return false, errVersionNotServed
	}
	if st.DefinitionFingerprint != "" && st.DefinitionFingerprint != o.def.fingerprint {
		return false, errVersionNotServed
	}
	if ev.kind == evStart && ev.start != nil {
		if ev.start.Version > 0 && ev.start.Version != o.def.version {
			return false, errVersionNotServed
		}
		if ev.start.DefinitionFingerprint != "" && ev.start.DefinitionFingerprint != o.def.fingerprint {
			return false, errVersionNotServed
		}
		if st.RegistryGeneration > 0 && ev.start.RegistryGeneration > 0 && st.RegistryGeneration != ev.start.RegistryGeneration {
			return false, errVersionNotServed
		}
	}

	// A terminated instance ignores everything but the unwind a parent sends, which is what moves a completed child back into compensating
	// The exception is a child that still owes its parent a report: nothing else will drive that journal again, so the delivery being retried is the only thing left that can get the report out
	if st.Status.IsTerminal() && ev.kind != evUnwind {
		if st.RegistryGeneration > 0 && !st.RegistryConfirmed && !st.RegistryRejected {
			return true, nil
		}
		if isLateAbandonedSuccess(st, ev) {
			return true, nil
		}
		return st.Parent != nil && (!st.Reported || ev.kind == evStart), nil
	}

	// The journal is created by the start job alone, so a control job that raced ahead of it waits rather than inventing an instance out of nothing
	// Returning an ordinary error has Francis retry the job, which is how a Suspend issued the moment after Start still lands
	if st.Status == "" && ev.kind != evStart {
		return false, errWaitingForStart
	}

	return true, nil
}

// confirmStart records the registry's decision for a newly published journal before any forward work or child result is dispatched
func (o *orchestrator) confirmStart(ctx context.Context, st *instanceState, now time.Time) error {
	if st.RegistryGeneration == 0 || st.RegistryConfirmed || st.RegistryRejected {
		return nil
	}
	_, err := o.wf.confirmDefinition(ctx, o.svc, registerRequest{
		Version: st.Version, Fingerprint: st.DefinitionFingerprint, Generation: st.RegistryGeneration,
	})
	if err != nil && !errors.Is(err, ErrDefinitionConflict) {
		return err
	}

	// The first persistence owns the cached snapshot, so the confirmation outcome must mutate an independent journal
	*st = st.clone()
	if err == nil {
		st.RegistryConfirmed = true
	} else {
		// No task was dispatched before this check, so a revoked start fails without compensation and leaves a durable parent outcome
		st.RegistryRejected = true
		st.Status = StatusFailed
		st.Compensation = CompensationNone
		st.Cause = "workflow definition authorization was revoked before the instance started"
		st.CompletedAt = now
		st.Output = nil
		st.Stack = nil
		st.Cursor = ""
		st.DeadlineAt = time.Time{}
		for i := range st.Steps {
			st.Steps[i].Status = StepSkipped
			st.Steps[i].CompletedAt = now
			st.Steps[i].Remaining = 0
			st.Steps[i].Tasks = nil
		}
	}
	return o.persist(ctx, st, now)
}

// recover is the part of a turn only the deadline drives: resolving an elapsed deadline against the journal
// One alarm stands for every deadline the instance has, so which one elapsed is resolved from the journal rather than carried on the event
func (o *orchestrator) recover(st *instanceState, ev *event, now time.Time) {
	if ev.kind != evDeadline {
		return
	}

	o.applyElapsedDeadlines(st, now)
}

// terminalStateTTL returns the expiry a terminated instance's journal is written with
// A journal is given twice its retention, so an instance whose sweep never runs still expires while the sweep can still find what it needs to clean up
// A child gets none: its parent may ask it to undo itself for as long as the parent is running, and an expiry the parent cannot see would take that journal out from under it
// A child's journal is removed by its parent's own purge, which reaches its children first, or by the sweep once the parent's journal is gone
func (o *orchestrator) terminalStateTTL(st *instanceState) time.Duration {
	if !st.Status.IsTerminal() || st.Parent != nil {
		return 0
	}
	return 2 * o.def.retention.forStatus(st.Status)
}

// persist writes the journal, its workflow labels, and its retention TTL in one operation, and fails the instance rather than letting it outgrow what it can store
func (o *orchestrator) persist(ctx context.Context, st *instanceState, now time.Time) error {
	opts := &actor.SetStateOpts{}
	opts.SetWorkflowLabels(builtinkey.Key{}, o.labels(st))
	opts.TTL = o.terminalStateTTL(st)
	st.encoded = nil

	// The size is checked before the write, because an instance that can no longer persist can no longer progress, and failing it is a much better outcome
	size, err := journalSize(st)
	if err != nil {
		return fmt.Errorf("failed to measure the workflow journal: %w", err)
	}
	if size > o.def.maxJournalSize {
		failForOversizedJournal(st, size, o.def.maxJournalSize, now)
		err = o.cancelAllOutstanding(ctx, st)
		if err != nil {
			return err
		}
		opts.SetWorkflowLabels(builtinkey.Key{}, o.labels(st))
		opts.TTL = o.terminalStateTTL(st)
		st.encoded = nil
		_, err = journalSize(st)
		if err != nil {
			return fmt.Errorf("failed to encode the terminal workflow journal: %w", err)
		}
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
func (o *orchestrator) recordTransitions(ctx context.Context, st *instanceState, ev *event, statusBefore Status, before map[string]StepStatus) {
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
			o.wf.metrics.compensationsFailed.Add(ctx, utils.BoolToInt64(sr.Status == StepCompensationFailed), metric.WithAttributes(
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
	if statusBefore.IsTerminal() && !st.Status.IsTerminal() {
		o.wf.metrics.instancesRunning.Add(ctx, 1, attrs)
	}

	if !st.Status.IsTerminal() || statusBefore.IsTerminal() || st.CompletedAt.IsZero() {
		return
	}

	o.wf.metrics.instancesRunning.Add(ctx, -1, attrs)
	if !st.Reopened {
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
func (o *orchestrator) reconcile(ctx context.Context, st *instanceState, now time.Time, forceParentReport bool) error {
	// A terminated instance reports to its parent, drops its timers, and lets go of its activation, so a wide fan-out does not hold one per instance after it is done
	if st.Status.IsTerminal() {
		err := o.reportToParent(ctx, st, forceParentReport)
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

	// Dispatch acknowledgements are written afterward, so they must not mutate the state client's last durable snapshot
	*st = st.clone()
	var changed bool
	for i := range st.Steps {
		sr := &st.Steps[i]
		d := o.def.byName[sr.Name]
		if d == nil {
			continue
		}

		var dispatched bool
		switch sr.Status {
		case StepRunning:
			dispatched, err = o.dispatchForward(ctx, st, sr, d, now)
		case StepCompensating:
			dispatched, err = o.dispatchCompensations(ctx, st, sr, d, now)
		default:
			// A step that settled while tasks were still outstanding leaves pending jobs behind, which are cancelled so the work that has not started never does
			err = o.cancelOutstanding(ctx, st, sr)
			if err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		changed = changed || dispatched
	}

	// An accepted attempt stays acknowledged across activations while a failed acknowledgement write safely repeats its idempotent dispatch
	if changed {
		statusBefore := st.Status
		before := st.stepStatuses()
		err = o.persist(ctx, st, now)
		if err != nil {
			return err
		}
		if st.Status.IsTerminal() {
			o.recordTransitions(ctx, st, &event{}, statusBefore, before)
			return o.reconcile(ctx, st, now, forceParentReport)
		}
	}

	return nil
}

// dispatchForward dispatches every forward task the journal says should be running, within the fan-out's sliding window
func (o *orchestrator) dispatchForward(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, now time.Time) (bool, error) {
	// A wait step has no task to dispatch: it is completed by RaiseEvent or failed by its own timeout
	if d.kind == KindWait {
		return false, nil
	}

	// The window admits the first tasks in index order that are not yet done, and slides as results arrive
	// Acknowledged jobs still occupy the window but require no further provider calls until their attempt changes
	window := d.maxParallel
	var admitted int
	var changed bool
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Done {
			continue
		}
		if window > 0 && admitted >= window {
			break
		}
		admitted++
		if tr.DispatchedAttempt >= tr.Attempts {
			continue
		}

		err := o.dispatchTask(ctx, st, sr, d, tr, now)
		if err != nil {
			return changed, err
		}
		tr.DispatchedAttempt = tr.Attempts
		changed = true
	}
	return changed, nil
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

	workerType := tr.WorkerType
	if workerType == "" {
		workerType = o.wf.workerType(member.capability)
	}
	client := builtinactor.NewClient[struct{}](workerType, workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
	_, _, err := client.Dispatch(ctx, methodRun, payload, opts...)
	if err != nil {
		return fmt.Errorf("failed to dispatch task %s[%d]: %w", sr.Name, tr.Index, err)
	}
	return nil
}

// startChild starts the child instance a child task runs, at an ID derived from the parent's so a retried turn finds the same child rather than starting a second
func (o *orchestrator) startChild(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, member *stepDef, tr *taskRecord) error {
	child := o.def.binding(member).child
	if child == nil {
		child = o.def.binding(d).child
	}
	if child == nil {
		return fmt.Errorf("step %q is a child step with no definition", sr.Name)
	}
	identity, err := child.authorizeDefinition(ctx, o.svc)
	if err != nil {
		return err
	}

	// The depth travels with the start, and the child refuses it if its own definition says the chain is too deep
	depth := 1
	if st.Parent != nil {
		depth = st.Parent.Depth + 1
	}

	// The child's input is what this task would have received, so a child step reads its parent's data exactly as a plain step does
	payload := startPayload{
		Input:                 o.childInput(st, sr, d, tr),
		Version:               child.def.version,
		DefinitionFingerprint: identity.Fingerprint,
		RegistryGeneration:    identity.Generation,
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
	_, _, err = client.Dispatch(ctx, methodStart, payload, actor.WithIdempotencyKey(methodStart))
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
func (o *orchestrator) dispatchCompensations(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, now time.Time) (bool, error) {
	var changed bool
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Comp == nil || tr.Comp.Done || tr.Comp.DispatchedAttempt >= tr.Comp.Attempts {
			continue
		}

		member := memberDef(d, tr.Index)

		// Compensating a child means asking the child to undo itself, rather than running a handler here
		if member.kind == KindChild || (d.kind == KindForEach && d.child != nil) {
			err := o.unwindChild(ctx, st, sr.Name, tr)
			if err != nil {
				return changed, err
			}
			tr.Comp.DispatchedAttempt = tr.Comp.Attempts
			changed = true
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
		undoType := tr.UndoType
		if undoType == "" {
			undoType = o.wf.undoType(member.capability)
		}
		client := builtinactor.NewClient[struct{}](undoType, workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
		_, _, err := client.Dispatch(ctx, methodCompensate, payload, opts...)
		if err != nil {
			return changed, fmt.Errorf("failed to dispatch compensation %s[%d]: %w", sr.Name, tr.Index, err)
		}
		tr.Comp.DispatchedAttempt = tr.Comp.Attempts
		changed = true

		o.wf.metrics.compensationsRun.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow", o.def.name),
			attribute.String("step", sr.Name),
		))
	}
	return changed, nil
}

// unwindChild asks a child instance to undo itself, which cancels one that is still running and reopens one that already completed
func (o *orchestrator) unwindChild(ctx context.Context, st *instanceState, stepName string, tr *taskRecord) error {
	child := o.childDefinitionFor(stepName, tr.Index)
	if child == nil {
		return nil
	}

	// The unwind covers both cases, and it is the one verb a terminated instance still accepts: a child that finished just as the parent decided to unwind reports a compensation instead of ignoring the request and leaving the frame outstanding forever
	// The parent's own compensation attempt number travels with it, so the child's report lands on the record being unwound
	attempt := 1
	if tr.Comp != nil {
		attempt = tr.Comp.Attempts
	}

	client := builtinactor.NewClient[struct{}](child.baseType, tr.ChildID, o.svc)
	_, _, err := client.Dispatch(ctx, methodUnwind, reasonPayload{Reason: st.Cause, FromParent: true, CompAttempt: attempt},
		actor.WithIdempotencyKey(methodUnwind+idDelimiter+strconv.Itoa(attempt)))
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
	child := o.def.binding(member).child
	if child != nil {
		return child
	}
	return o.def.binding(d).child
}

// cancelAllOutstanding removes queued forward and compensation work before a terminal journal becomes visible
func (o *orchestrator) cancelAllOutstanding(ctx context.Context, st *instanceState) error {
	targets := make([]jobCleanupTarget, 0)
	for i := range st.Steps {
		sr := &st.Steps[i]
		stepTargets, err := o.outstandingJobTargets(st, sr)
		if err != nil {
			return err
		}
		targets = append(targets, stepTargets...)
	}
	return o.cancelJobTargets(ctx, targets)
}

// cancelOutstanding removes the pending jobs of a step that settled while some of its tasks had not reported
// A job that is already executing is not interrupted, so a task that was running finishes and its late report is recorded like any other result
func (o *orchestrator) cancelOutstanding(ctx context.Context, st *instanceState, sr *stepRecord) error {
	targets, err := o.outstandingJobTargets(st, sr)
	if err != nil {
		return err
	}
	return o.cancelJobTargets(ctx, targets)
}

// outstandingJobTargets resolves the live forward and compensation actors a settled step can still have
func (o *orchestrator) outstandingJobTargets(st *instanceState, sr *stepRecord) ([]jobCleanupTarget, error) {
	targets := make([]jobCleanupTarget, 0)
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.ChildID != "" {
			continue
		}

		// A task an unwind abandoned is done as far as the journal is concerned, but its job may still be waiting to run, and there is no reason to let it
		if !tr.Done || tr.Abandoned {
			workerType := tr.WorkerType
			if workerType == "" {
				var err error
				workerType, err = o.legacyTaskActorType(st, sr.Name, tr.Index, false)
				if err != nil {
					return nil, err
				}
			}
			targets = append(targets, jobCleanupTarget{
				actorType: workerType,
				actorID:   workerActorID(o.instanceID, sr.Name, tr.Index),
			})
		}

		if tr.Comp != nil && !tr.Comp.Done {
			undoType := tr.UndoType
			if undoType == "" {
				var err error
				undoType, err = o.legacyTaskActorType(st, sr.Name, tr.Index, true)
				if err != nil {
					return nil, err
				}
			}
			targets = append(targets, jobCleanupTarget{
				actorType: undoType,
				actorID:   workerActorID(o.instanceID, sr.Name, tr.Index),
			})
		}
	}
	return targets, nil
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

	err := o.client.SetAlarm(ctx, alarmDeadline, deadlineAlarmProperties(due))
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
		// The compensation phase has its own instance-sized budget, so a second expiration abandons work that still has not unwound
		// The frames left on the stack are what name the effects nothing undid, which is what an operator needs to finish by hand
		if st.Status == StatusCompensating {
			st.Cause = unwindAbandonedCause(st.Cause)
			st.Compensation = CompensationFailed
			terminate(st, o.def, now)
			return
		}

		st.beginUnwind(o.def, "instance timeout elapsed", StatusFailed, now)
		// Compensation receives a fresh instance-sized budget because the forward budget has already elapsed
		st.StartedAt = now
		return
	}

	sr, d := st.currentRunningStep(o.def)
	if sr == nil || d == nil || sr.Status != StepRunning {
		return
	}

	stepDue := d.stepDeadline(sr)
	if stepDue.IsZero() || now.Before(stepDue) {
		return
	}

	// A wait step that never got its event is the one case where the step's timeout ends the run rather than failing a task
	if d.kind == KindWait {
		st.beginUnwind(o.def, fmt.Sprintf("event %q timed out", d.eventName), StatusFailed, now)
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
		tr.Abandoned = true
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
	st = st.clone()
	if st.Status == "" || st.Status.IsTerminal() {
		return nil
	}

	// An alarm on another version must settle the start's registry decision before publishing any terminal child outcome
	now := time.Now()
	err = o.confirmStart(ctx, &st, now)
	if err != nil {
		return err
	}
	if st.Status.IsTerminal() {
		return o.reconcile(ctx, &st, now, false)
	}
	if st.Status == StatusSuspended {
		return o.dropDeadline(ctx)
	}

	instanceDue := instanceDeadline(&st, o.def)
	timedOut := st.Timeout > 0 && !instanceDue.IsZero() && !now.Before(instanceDue)

	// Failing an instance no host can serve is only worth doing once its own timeout has elapsed, and it is done without compensation, since no host can run the compensations either
	if st.UnknownVersion == FailUnknownVersion && timedOut {
		st.Status = StatusFailed
		st.Compensation = CompensationNone
		st.Cause = fmt.Sprintf("unknown version %d", st.Version)
		st.CompletedAt = now
		for i := range st.Steps {
			if st.Steps[i].Status == StepPending || st.Steps[i].Status == StepRunning {
				st.Steps[i].Status = StepSkipped
			}
		}
		err = o.cancelAllOutstanding(ctx, &st)
		if err != nil {
			return err
		}
		err = o.persist(ctx, &st, now)
		if err != nil {
			return err
		}
		return o.reconcile(ctx, &st, now, false)
	}

	// Parking re-arms the alarm and waits for a host that can serve the version, which is what List(Version: v) shows an operator
	if o.log != nil {
		o.log.WarnContext(ctx, "Workflow deadline fired on a host without the instance's version; parking",
			slog.Int("instanceVersion", st.Version), slog.Int("hostVersion", o.def.version))
	}
	return o.client.SetAlarm(ctx, alarmDeadline, deadlineAlarmProperties(now.Add(defaultParkInterval)))
}

// deadlineAlarmProperties keeps the exact first deadline and supplies a short recurrence only when its turn cannot replace or remove it
func deadlineAlarmProperties(due time.Time) actor.AlarmProperties {
	return actor.AlarmProperties{DueTime: due, Interval: "PT5S", Data: deadlinePayload{Recurring: true}}
}

// memberDef resolves the definition that governs one task of a step, which for a parallel group is the member that runs it
func memberDef(d *stepDef, index int) *stepDef {
	if d == nil {
		return nil
	}
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
	enc, err := msgpack.Marshal(instanceStateWire(*st))
	if err != nil {
		return 0, err
	}
	st.encoded = enc
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
func (st *instanceState) stepStatuses() map[string]StepStatus {
	out := make(map[string]StepStatus, len(st.Steps))
	for i := range st.Steps {
		out[st.Steps[i].Name] = st.Steps[i].Status
	}
	return out
}
