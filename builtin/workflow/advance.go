package workflow

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// minAdvanceIterations is the floor the fixed-point loop's bound never goes below, so a graph of one or two steps still has room for the passes a turn genuinely needs
const minAdvanceIterations = 16

// advanceIterations bounds the fixed-point loop in advance, derived from the graph rather than fixed
// Each iteration opens or settles at least one step, and a step is opened once and settled once going forward and again while unwinding, so four passes over the graph is more than the transitions a journal can make in one turn
// A fixed bound would silently stop short on a graph longer than it, leaving pending steps with nothing left to open them: the instance would then run to its timeout rather than to its end
func advanceIterations(def *definition) int {
	return 4*len(def.steps) + minAdvanceIterations
}

// eventKind discriminates the things that drive a Workflow turn
type eventKind string

const (
	evStart       eventKind = "start"
	evDone        eventKind = "done"
	evCompensated eventKind = "compensated"
	evRaise       eventKind = "event"
	evCancel      eventKind = "cancel"
	evUnwind      eventKind = "unwind"
	evSuspend     eventKind = "suspend"
	evResume      eventKind = "resume"
	evDeadline    eventKind = "deadline"
)

// event is one thing to fold into the journal, built by the orchestrator from the job or alarm that triggered the turn
type event struct {
	kind   eventKind
	start  *startPayload
	report *reportPayload
	comp   *compReportPayload
	raise  *eventPayload
	reason string
	// fromParent and compAttempt carry what a parent said when it asked this instance to stop and undo itself
	fromParent  bool
	compAttempt int
}

// apply folds an event into the journal
// It is pure, and it records nothing for a duplicate or for a report whose outcome the journal already has, so every report is safe to redeliver
// The returned value reports whether the event changed nothing, so the turn can count how often ordering invariant 2 is doing its job
func apply(st *instanceState, def *definition, ev *event, now time.Time) (duplicate bool) {
	// A terminated instance ignores everything, so a late report or a repeated cancel cannot revive it
	// An unwind may reopen a completed child, and a late report may reveal an abandoned task whose effect still needs compensation
	if st.Status.IsTerminal() && ev.kind != evUnwind && !isLateAbandonedSuccess(st, ev) {
		return true
	}

	switch ev.kind {
	case evStart:
		return st.applyStart(def, ev.start, now)
	case evDone:
		return st.applyReport(def, ev.report, now)
	case evCompensated:
		return st.applyCompReport(def, ev.comp, now)
	case evRaise:
		return st.applyRaisedEvent(def, ev.raise, now)
	case evCancel:
		return st.applyCancel(def, ev, now)
	case evUnwind:
		return st.applyUnwind(def, ev, now)
	case evSuspend:
		return st.applySuspend(def, ev.reason, now)
	case evResume:
		return st.applyResume(def, now)
	case evDeadline:
		// A deadline is resolved against the journal by the orchestrator, which folds the outcome as a failure rather than as its own event
		return true
	default:
		return true
	}
}

// applyStart initializes the journal of a new instance, recording every step of the definition so status and the unknown-version path are answerable from the journal alone
func (st *instanceState) applyStart(def *definition, p *startPayload, now time.Time) bool {
	// A repeated start finds the instance already here, so the second call's input is discarded rather than overwriting the first's
	if st.Status != "" {
		return true
	}

	st.Workflow = def.name
	st.Version = p.Version
	st.DefinitionFingerprint = p.DefinitionFingerprint
	if st.DefinitionFingerprint == "" {
		st.DefinitionFingerprint = def.fingerprint
	}
	st.RegistryGeneration = p.RegistryGeneration
	st.Status = StatusRunning
	st.Input = p.Input
	st.Timeout = def.timeout
	st.UnknownVersion = def.unknownVersion
	st.MaxEventSize = def.maxOutputSize
	st.TraceParent = p.TraceParent
	st.Parent = p.Parent
	if st.Parent != nil && p.Attempt > 0 {
		st.Parent.Attempt = p.Attempt
	}
	st.CreatedAt = p.CreatedAt
	if st.CreatedAt.IsZero() {
		st.CreatedAt = now
	}
	st.StartedAt = now

	// Persist the event contract so callers with a newer graph can still drive this instance
	st.EventNames = nil
	st.Steps = make([]stepRecord, len(def.steps))
	for i, d := range def.steps {
		st.Steps[i] = stepRecord{
			Name:   d.name,
			Kind:   d.kind,
			Status: StepPending,
		}
		if d.kind == KindWait {
			st.EventNames = append(st.EventNames, d.effectiveEventName())
		}
	}

	// A start whose parent chain is deeper than the definition allows is refused here, which is the only thing that stops a definition referencing itself
	// Refusing it as a terminated instance rather than as an error is what gets the failure reported back to the parent, whose step policy then decides what it costs
	var failure string
	if st.Parent != nil && st.Parent.Depth > def.maxDepth {
		failure = fmt.Sprintf("%s: depth %d exceeds the limit of %d", ErrMaxDepthExceeded.Error(), st.Parent.Depth, def.maxDepth)
	}

	// Child starts bypass the public service encoder, so enforce the receiving definition's bound before any task can run
	if len(p.Input) > def.maxInputSize {
		failure = fmt.Sprintf("%s: %d bytes exceeds the %d byte limit", ErrInputTooLarge.Error(), len(p.Input), def.maxInputSize)
		st.Input = nil
	}

	// Rejected starts keep a small terminal journal that can reliably report the failure to their parent
	if failure != "" {
		st.Status = StatusFailed
		st.Compensation = CompensationNone
		st.Cause = failure
		st.CompletedAt = now
		for i := range st.Steps {
			st.Steps[i].Status = StepSkipped
			st.Steps[i].CompletedAt = now
		}
	}

	return false
}

// applyReport folds a worker's (or a child's) outcome for one task into the journal
func (st *instanceState) applyReport(def *definition, p *reportPayload, now time.Time) bool {
	sr := st.step(p.Step)
	if sr == nil {
		return true
	}
	d := def.byName[p.Step]
	if d == nil {
		return true
	}
	tr := sr.task(p.Index)
	if tr == nil {
		return true
	}

	// The journal's outcome is what decides whether a report counts, never whether this delivery has been seen before
	// The one exception is a task an unwind abandoned: it was never interrupted, so a success it reports late is real, and recording it is what gets the effect compensated rather than stranded
	if tr.Done {
		if !tr.Abandoned || p.Error != "" {
			return true
		}

		tr.Abandoned = false
		tr.Error = ""
		tr.LastError = ""
		tr.Output = p.Output
		tr.CompletedAt = now
		recordChildOutcome(tr, p)

		// An undo dispatched before this success cannot cover an effect the forward attempt may have produced afterward
		if tr.Comp != nil {
			attempt := tr.Comp.Attempts + 1
			tr.Comp = &compRecord{Attempts: attempt, GenerationStart: attempt}
			tr.Compensated = false
		}

		// A frame that had already closed must be eligible to open again for the newly discovered effect
		if sr.Status == StepCompensated || sr.Status == StepCompensationFailed {
			sr.Status = StepCompleted
		}

		// Forward completion retains the effect for a possible parent unwind without rolling back committed optional work
		if st.TerminalStatus == "" {
			pushFrame(st, sr, d)
			return false
		}

		// Only a journal that actually began unwinding resumes rollback when an abandoned task reports late
		if st.Status.IsTerminal() && memberCompensable(d, p.Index) {
			st.Status = StatusCompensating
			st.CompletedAt = time.Time{}
			st.Reported = false
			st.Reopened = true
			st.StartedAt = now
			st.Compensation = currentCompensationFailure(st, def)
		}
		return false
	}

	// A late report from an earlier attempt is accepted when it succeeded, since the work really was done, and ignored when it failed, since a newer attempt is already in flight
	if p.Attempt < tr.Attempts && p.Error != "" {
		return true
	}

	// Success: record the output and close the task
	if p.Error == "" {
		tr.Output = p.Output
		recordChildOutcome(tr, p)
		tr.Done = true
		tr.CompletedAt = now
		tr.LastError = ""
		tr.RetryAt = time.Time{}
		if sr.Remaining > 0 {
			sr.Remaining--
		}
		return false
	}

	// A child that terminated failed or cancelled fails the parent's task, and restarting it would only find the instance that already terminated
	if p.ChildStatus != "" && p.ChildStatus != StatusCompleted {
		recordChildOutcome(tr, p)
		p.Retryable = false
	}

	// Failure: the policy of the step that ran the task decides whether another attempt follows, which for a group's task is the member rather than the group
	member := memberDef(d, p.Index)
	tr.LastError = p.Error
	maxAttempts := effectiveMaxAttempts(member)
	if !p.Retryable || tr.Attempts >= maxAttempts {
		tr.Error = p.Error
		tr.Done = true
		tr.CompletedAt = now
		if sr.Remaining > 0 {
			sr.Remaining--
		}
		return false
	}

	// The next attempt's number is recorded before the job that runs it exists, so a lost dispatch is recoverable and a report can never arrive for an attempt the journal does not know about
	tr.Attempts++
	tr.RetryAt = now.Add(backoff(member.retryInitial, member.retryMax, defaultRetryInitial, defaultRetryMax, tr.Attempts-1))
	return false
}

// applyCompReport folds an undo worker's outcome for one task's compensation into the journal
func (st *instanceState) applyCompReport(def *definition, p *compReportPayload, now time.Time) bool {
	sr := st.step(p.Step)
	if sr == nil {
		return true
	}
	d := def.byName[p.Step]
	if d == nil {
		return true
	}
	tr := sr.task(p.Index)
	if tr == nil || tr.Comp == nil {
		return true
	}

	if tr.Comp.Done {
		return true
	}

	// Reports from a prior effect generation cannot satisfy compensation of a later forward success
	if p.Attempt < tr.Comp.GenerationStart {
		return true
	}
	if p.Attempt < tr.Comp.Attempts && p.Error != "" {
		return true
	}
	if p.ChildStatus != "" {
		tr.ChildStatus = p.ChildStatus
		tr.ChildCompensation = p.ChildCompensation
	}

	// Success: the effect is undone
	if p.Error == "" {
		tr.Comp.Done = true
		tr.Comp.LastError = ""
		tr.Comp.RetryAt = time.Time{}
		tr.Compensated = true
		return false
	}

	// Failure: compensations get their own, more generous attempt policy, because a failed rollback leaves the system inconsistent
	// It belongs to the step whose compensation ran, which for a group's task is the member rather than the group
	member := memberDef(d, p.Index)
	tr.Comp.LastError = p.Error
	maxAttempts := effectiveCompMaxAttempts(member)
	attempts := tr.Comp.Attempts
	if tr.Comp.GenerationStart > 0 {
		attempts -= tr.Comp.GenerationStart - 1
	}
	if !p.Retryable || attempts >= maxAttempts {
		tr.Comp.Error = p.Error
		tr.Comp.Done = true
		return false
	}

	tr.Comp.Attempts++
	tr.Comp.RetryAt = now.Add(backoff(member.compInitial, member.compMax, defaultCompInitial, defaultCompMax, attempts))
	return false
}

// applyRaisedEvent records an external event against the WaitForEvent step listening for it
// An event is accepted while suspended, and the step completes on resume
func (st *instanceState) applyRaisedEvent(def *definition, p *eventPayload, now time.Time) bool {
	for i := range st.Steps {
		sr := &st.Steps[i]
		if sr.Kind != KindWait {
			continue
		}

		d := def.byName[sr.Name]
		if d == nil || d.effectiveEventName() != p.Name {
			continue
		}

		// Only the open wait accepts its event, so an event raised before the step is reached, or after it completed, records nothing
		if sr.Status != StepRunning {
			return true
		}

		sr.Event = p.Payload
		if sr.Event == nil {
			sr.Event = json.RawMessage("null")
		}
		sr.Remaining = 0
		sr.CompletedAt = now
		return false
	}

	return true
}

// applyCancel asks a running or suspended instance to stop and unwind
func (st *instanceState) applyCancel(def *definition, ev *event, now time.Time) bool {
	// Cancel takes precedence over a suspension, so it resumes the instance straight into the unwind
	if st.Status == StatusCompensating {
		return true
	}

	reason := ev.reason
	if reason == "" {
		reason = "cancelled"
	}
	st.Suspended = nil
	st.recordUnwoundBy(ev)
	st.beginUnwind(def, reason, StatusCancelled, now)
	return false
}

// applyUnwind moves a completed child back into compensating at its parent's request, which is the one verb only a parent may send
// A completed child is kept rather than purged for as long as its parent is running for precisely this reason
func (st *instanceState) applyUnwind(def *definition, ev *event, now time.Time) bool {
	if st.Status == StatusCompensating {
		// A parent joining an existing rollback still needs its newest compensation attempt acknowledged
		return !st.recordUnwoundBy(ev)
	}
	if st.Status == StatusSuspended && st.Suspended != nil && st.Suspended.ResumeTo == StatusCompensating {
		// A parent waiting on this rollback resumes it without erasing failures already recorded by earlier frames
		st.applyResume(def, now)
		st.recordUnwoundBy(ev)
		st.Reported = false
		return false
	}

	reason := ev.reason
	if reason == "" {
		reason = "unwound by parent"
	}

	// A child that already finished an unwind reports its recorded outcome to the parent without relabeling a failed rollback as successful
	if st.Status.IsTerminal() && st.TerminalStatus != "" {
		st.recordUnwoundBy(ev)
		st.Reported = false
		return false
	}

	// A completed instance is reopened: its terminal outcome is cleared so the unwind can run and report a compensation of its own
	if st.Status.IsTerminal() {
		st.Reopened = true
	}
	st.CompletedAt = time.Time{}
	st.Compensation = ""
	st.Reported = false
	st.Suspended = nil

	// The unwind gets the instance timeout as its own budget, since the forward run's is long since spent
	st.StartedAt = now

	st.recordUnwoundBy(ev)
	st.beginUnwind(def, reason, StatusCancelled, now)
	return false
}

// recordUnwoundBy retains the highest parent compensation attempt so an older request cannot redirect the eventual acknowledgement
// It reports whether the parent attribution changed
func (st *instanceState) recordUnwoundBy(ev *event) bool {
	if !ev.fromParent || st.Parent == nil {
		return false
	}
	attempt := max(ev.compAttempt, 1)
	if attempt <= st.Parent.UnwoundBy {
		return false
	}
	st.Parent.UnwoundBy = attempt
	return true
}

// currentCompensationFailure derives the aggregate failure from current effect generations after a late effect replaces an obsolete undo record
func currentCompensationFailure(st *instanceState, def *definition) CompensationOutcome {
	for i := range st.Steps {
		for j := range st.Steps[i].Tasks {
			comp := st.Steps[i].Tasks[j].Comp
			if comp == nil || comp.Error == "" {
				continue
			}
			if def.compensationFailurePolicy == AbortUnwinding {
				return CompensationFailed
			}
			return CompensationPartial
		}
	}
	return ""
}

// applySuspend pauses an instance, recording what is left of each deadline so resuming does not eat the remainder
func (st *instanceState) applySuspend(def *definition, reason string, now time.Time) bool {
	if st.Status == StatusSuspended {
		return true
	}

	rec := &suspendRecord{
		At:               now,
		Reason:           reason,
		ResumeTo:         st.Status,
		RemainingTimeout: until(instanceDeadline(st, def), now),
	}

	// The current step's own deadline is paused alongside the instance's, so a long suspension does not consume a short step timeout either
	sr, d := st.currentRunningStep(def)
	if sr != nil && d != nil && sr.Status == StepRunning {
		stepDue := d.stepDeadline(sr)
		if !stepDue.IsZero() {
			rec.RemainingStepTimeout = until(stepDue, now)
		}
	}

	st.Suspended = rec
	st.Status = StatusSuspended
	return false
}

// applyResume continues a suspended instance, putting the deadlines back where the suspension found them
// The remainders are restored by shifting the recorded start times forward, so every deadline recomputes from the journal exactly as it did before
func (st *instanceState) applyResume(def *definition, now time.Time) bool {
	if st.Status != StatusSuspended || st.Suspended == nil {
		return true
	}

	rec := st.Suspended
	st.Status = rec.ResumeTo
	if st.Status == "" {
		st.Status = StatusRunning
	}

	st.StartedAt = now.Add(rec.RemainingTimeout - def.timeout)

	sr, d := st.currentRunningStep(def)
	if sr != nil && d != nil && rec.RemainingStepTimeout > 0 {
		budget := d.stepBudget()
		if budget > 0 {
			sr.StartedAt = now.Add(rec.RemainingStepTimeout - budget)
		}
	}

	st.Suspended = nil
	return false
}

// beginUnwind opens the compensation phase, recording the cause every compensation receives and the status the unwind terminates into
func (st *instanceState) beginUnwind(def *definition, cause string, terminal Status, now time.Time) {
	st.Cause = cause
	st.TerminalStatus = terminal
	st.Status = StatusCompensating

	for i := range st.Steps {
		sr := &st.Steps[i]
		switch sr.Status {
		case StepPending:
			// A step that was never reached is recorded as skipped rather than left pending, so the journal says plainly that it will not run
			sr.Status = StepSkipped
			sr.CompletedAt = now
		case StepRunning:
			// A step in flight is closed out so the unwind can proceed, rather than waiting on attempts nobody is going to re-drive
			abandonRunningStep(st, sr, def.byName[sr.Name], now)
		}
	}
}

// abandonOutstandingTasks closes out the tasks of a step that have not reported, marking them abandoned rather than failed
// The distinction matters: nothing interrupts an attempt that is already executing, so a success one of them reports late is real work that still has to be compensated
func abandonOutstandingTasks(sr *stepRecord, reason string, now time.Time) {
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if tr.Done {
			continue
		}

		tr.Abandoned = true
		tr.Done = true
		tr.Error = reason
		tr.LastError = reason
		tr.CompletedAt = now
		if sr.Remaining > 0 {
			sr.Remaining--
		}
	}
}

// abandonRunningStep closes out the step that was in flight when the unwind opened
// The tasks that had not reported are abandoned rather than failed, because an attempt already executing is never interrupted and a success it reports late still has to be compensated
func abandonRunningStep(st *instanceState, sr *stepRecord, d *stepDef, now time.Time) {
	if d == nil {
		return
	}

	// A wait step has no task at all, and nobody is going to raise its event now
	if d.kind == KindWait {
		sr.Status = StepSkipped
		sr.CompletedAt = now
		sr.Remaining = 0
		return
	}

	abandonOutstandingTasks(sr, "abandoned: "+st.Cause, now)

	// The step settles on the records as they now stand, so whatever completed before the unwind opened still enters the compensation stack
	outcome, errMsg := stepOutcome(sr, d)
	if outcome == StepFailed {
		sr.Status = StepFailed
		sr.Error = errMsg
	} else {
		sr.Status = StepCompleted
	}
	sr.CompletedAt = now
	pushFrame(st, sr, d)
}

// advance derives the next journal from the current one, as a pure function of the journal and the definition
// It performs no I/O, runs no user code, and cannot block, so the orchestration boundary is enforced by the code rather than by convention
// Cursor is one of its outputs, never one of its inputs
func advance(st *instanceState, def *definition, instanceID string, now time.Time) {
	if st.Status.IsTerminal() || st.Status == "" {
		return
	}

	// A suspended instance still records the truth about work that was already in flight, but opens nothing new and never changes its own status
	// Its deadlines are paused, which is what the empty DeadlineAt says: reconcile drops the alarm and resume puts the remainders back
	if st.Status == StatusSuspended {
		settleSteps(st, def, now)
		if st.Status != StatusSuspended && st.Suspended != nil {
			st.Suspended.ResumeTo = st.Status
			st.Status = StatusSuspended
		}
		st.DeadlineAt = time.Time{}
		st.Cursor = deriveCursor(st)
		return
	}

	// Settling a step opens the next one, which may settle immediately (a skipped step, an empty fan-out), so this runs to a fixed point
	for range advanceIterations(def) {
		changed := settleSteps(st, def, now)

		if st.Status == StatusCompensating {
			changed = refreshFrames(st, def) || changed
			changed = unwindNextFrame(st, def, now) || changed
		} else {
			changed = openNextStep(st, def, instanceID, now) || changed
		}

		if !changed {
			break
		}
		if st.Status.IsTerminal() {
			break
		}
	}

	st.DeadlineAt = nextDeadline(st, def)
	st.Cursor = deriveCursor(st)
}

// nextDeadline is the earliest of the instance timeout and the current step's own timeout, which for a wait step is how long it waits for its event
// One alarm stands for every deadline the instance has, and the journal records which time it should carry
func nextDeadline(st *instanceState, def *definition) time.Time {
	if st.Status.IsTerminal() {
		return time.Time{}
	}

	due := instanceDeadline(st, def)

	// A frame being compensated must not keep rearming its expired forward step budget
	sr, d := st.currentRunningStep(def)
	if sr != nil && d != nil && sr.Status == StepRunning {
		stepDue := d.stepDeadline(sr)
		if !stepDue.IsZero() && (due.IsZero() || stepDue.Before(due)) {
			due = stepDue
		}
	}
	return due
}

// settleSteps closes every running step whose tasks have all reported, or that a fail-fast policy has already decided
func settleSteps(st *instanceState, def *definition, now time.Time) bool {
	var changed bool
	for i := range st.Steps {
		sr := &st.Steps[i]
		if sr.Status != StepRunning {
			continue
		}

		d := def.byName[sr.Name]
		if d == nil {
			continue
		}

		// A wait step is settled by its event rather than by a task reporting
		if sr.Kind == KindWait {
			if sr.Event != nil {
				completeStep(st, sr, d, now)
				changed = true
			}
			continue
		}

		// Fail-fast decides the step the moment one task has failed for good, without waiting for the stragglers whose results are still recorded
		failedNow := firstFailedTask(sr) >= 0
		if sr.Remaining > 0 && (!failedNow || groupPolicy(d) != FailFast) {
			continue
		}

		// Fail-fast settles the step while tasks are still outstanding, and those are abandoned rather than failed: an attempt already executing is never interrupted, so a success it reports late still has to be compensated
		if sr.Remaining > 0 {
			abandonOutstandingTasks(sr, "abandoned: step "+sr.Name+" failed", now)
		}

		outcome, errMsg := stepOutcome(sr, d)
		if outcome == StepFailed {
			failStep(st, def, sr, d, errMsg, now)
		} else {
			completeStep(st, sr, d, now)
		}
		changed = true
	}
	return changed
}

// stepOutcome decides whether a step that has finished reporting completed or failed, applying the group policy for a group or a fan-out
func stepOutcome(sr *stepRecord, d *stepDef) (StepStatus, string) {
	switch d.kind {
	case KindStep, KindChild:
		tr := sr.task(0)
		if tr != nil && tr.Error != "" {
			return StepFailed, tr.Error
		}
		return StepCompleted, ""
	default:
		// A group or a fan-out only fails when its policy says a failing member should cost the step
		if groupPolicy(d) == TolerateFailures {
			return StepCompleted, ""
		}
		idx := firstFailedTask(sr)
		if idx >= 0 {
			return StepFailed, sr.Tasks[idx].Error
		}
		return StepCompleted, ""
	}
}

// firstFailedTask returns the index into Tasks of the first task that failed for good, or -1 when none has
func firstFailedTask(sr *stepRecord) int {
	for i := range sr.Tasks {
		if sr.Tasks[i].Done && sr.Tasks[i].Error != "" {
			return i
		}
	}
	return -1
}

// completeStep records a step as completed and pushes its frame onto the compensation stack when there is anything to undo
func completeStep(st *instanceState, sr *stepRecord, d *stepDef, now time.Time) {
	sr.Status = StepCompleted
	sr.CompletedAt = now
	sr.Remaining = 0
	pushFrame(st, sr, d)
}

// failStep records a step as failed, applies WithSkipOnFailure to its named dependents, and opens the unwind unless the step declared otherwise
func failStep(st *instanceState, def *definition, sr *stepRecord, d *stepDef, errMsg string, now time.Time) {
	sr.Status = StepFailed
	sr.Error = errMsg
	sr.CompletedAt = now
	sr.Remaining = 0

	// A frame is still pushed when tasks of the step succeeded, since their effects are real and a fail-fast group is exactly that case
	pushFrame(st, sr, d)

	// The named dependents are pointless without this step, and are recorded as skipped rather than run and failed
	// Skipping is not transitive: a skipped step is not a failed one, so its own list is not applied
	for _, dep := range d.skipOnFailure {
		depRec := st.step(dep)
		if depRec != nil && depRec.Status == StepPending {
			depRec.Status = StepSkipped
			depRec.CompletedAt = now
		}
	}

	// Only the default case unwinds: WithOptional and WithSkipOnFailure both say the workflow continues, and they differ only in what they cost
	if d.optional || len(d.skipOnFailure) > 0 {
		return
	}

	cause := fmt.Sprintf("step %q failed: %s", sr.Name, errMsg)
	st.beginUnwind(def, cause, StatusFailed, now)
}

// pushFrame adds a settled step to the compensation stack when it is compensable and holds at least one task whose effect has to be undone
func pushFrame(st *instanceState, sr *stepRecord, d *stepDef) {
	if !d.isCompensable() {
		return
	}
	if len(compensableTasks(sr, d)) == 0 {
		return
	}

	// A step already on the stack, or already unwound, is not pushed again
	if slices.Contains(st.Stack, sr.Name) || sr.Status == StepCompensating || sr.Status == StepCompensated || sr.Status == StepCompensationFailed {
		return
	}

	// Frames are pushed in completion order and popped in reverse, which is the invariant a saga depends on
	st.Stack = append(st.Stack, sr.Name)
}

// compensableTasks returns the indexes into Tasks of the tasks of a settled step whose effects have to be undone
// A task that succeeded is always one
// A task that failed is one only when the step opted in with WithCompensateOnFailure, since the saga convention is that a step which did not complete did not take effect
// A task that started a child instance and was abandoned is always one, whatever the step opted into: the child is a live instance of its own, and nothing but this frame will stop it
func compensableTasks(sr *stepRecord, d *stepDef) []int {
	var out []int
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		if !tr.Done {
			continue
		}

		if tr.Error == "" {
			// A group's member is only compensable when that member declares a compensation of its own
			if memberCompensable(d, tr.Index) {
				out = append(out, i)
			}
			continue
		}

		// An abandoned task never reported, so a child it started may still be running and producing effects
		if tr.Abandoned && tr.ChildID != "" && memberCompensable(d, tr.Index) {
			out = append(out, i)
			continue
		}

		member := memberDef(d, tr.Index)
		if member.compensateOnFailure && memberCompensable(d, tr.Index) {
			out = append(out, i)
		}
	}
	return out
}

// memberCompensable reports whether the task at an index of a step has anything to undo, resolving a group's task back to the member that ran it
func memberCompensable(d *stepDef, index int) bool {
	if d.kind == KindParallel {
		if index < 0 || index >= len(d.members) {
			return false
		}
		return d.members[index].isCompensable()
	}
	return d.isCompensable()
}

// openNextStep opens the first step that has not been reached yet, or terminates the instance when every step has settled
func openNextStep(st *instanceState, def *definition, instanceID string, now time.Time) bool {
	for i := range st.Steps {
		sr := &st.Steps[i]
		switch sr.Status {
		case StepPending:
			return openStep(st, def, sr, instanceID, now)
		case StepRunning, StepCompensating:
			// The step in flight has to settle before anything after it opens
			return false
		default:
			continue
		}
	}

	terminate(st, def, now)
	return true
}

// openStep creates a step's tasks and marks it running, or records it as skipped when its condition says it should not run
func openStep(st *instanceState, def *definition, sr *stepRecord, instanceID string, now time.Time) bool {
	d := def.byName[sr.Name]
	if d == nil {
		// The definition no longer has this step, which only happens to a journal whose version this host cannot serve, so it is left alone for a host that can
		return false
	}

	// A condition is a recorded output rather than a predicate evaluated here, so an absent output simply means the step runs
	if d.hasSkipIf && conditionMatches(st, def, d) {
		sr.Status = StepSkipped
		sr.CompletedAt = now
		sr.Remaining = 0
		return true
	}

	sr.StartedAt = now
	sr.Status = StepRunning

	switch d.kind {
	case KindStep, KindChild:
		sr.Tasks = []taskRecord{newTask(0, nil)}
		def.configureTaskActors(d, &sr.Tasks[0], instanceID, sr.Name)
		sr.Remaining = 1
	case KindParallel:
		sr.Tasks = make([]taskRecord, len(d.members))
		for i := range d.members {
			sr.Tasks[i] = newTask(i, nil)
			def.configureTaskActors(d, &sr.Tasks[i], instanceID, sr.Name)
		}
		sr.Remaining = len(sr.Tasks)
	case KindForEach:
		items, err := def.fanOutItems(st, d)
		if err != nil {
			// The list is the fan-out's own input, so a list that cannot be read is the step failing rather than the instance crashing
			sr.Tasks = nil
			sr.Remaining = 0
			failStep(st, def, sr, d, err.Error(), now)
			return true
		}

		sr.Tasks = make([]taskRecord, len(items))
		for i, item := range items {
			sr.Tasks[i] = newTask(i, item)
			def.configureTaskActors(d, &sr.Tasks[i], instanceID, sr.Name)
		}
		sr.Remaining = len(sr.Tasks)
	case KindWait:
		// A wait step has no task at all: it is completed by RaiseEvent, or failed by its own timeout
		sr.Tasks = nil
		sr.Remaining = 0
	}

	return true
}

// newTask builds the record of a task that has not run yet, with its first attempt already numbered so it is journaled before it is dispatched
func newTask(index int, item json.RawMessage) taskRecord {
	return taskRecord{
		Index:    index,
		Item:     item,
		Attempts: 1,
	}
}

// conditionMatches reports whether the step named by WithSkipIf recorded the output the condition skips on
func conditionMatches(st *instanceState, def *definition, d *stepDef) bool {
	sr := st.step(d.skipIfStep)
	if sr == nil || sr.Status != StepCompleted {
		return false
	}

	out := def.byName[sr.Name].stepOutput(sr)
	if len(out) == 0 {
		return false
	}

	var v bool
	err := json.Unmarshal(out, &v)
	if err != nil {
		return false
	}
	return v == d.skipIfValue
}

// fanOutItems reads the elements a fan-out iterates from the output of the step named by WithItemsFrom
// The size is decided once, when the upstream step reports, and is then journaled, so a retried turn re-reads the recorded items rather than re-deriving them
func (def *definition) fanOutItems(st *instanceState, d *stepDef) ([]json.RawMessage, error) {
	sr := st.step(d.itemsFrom)
	if sr == nil {
		return nil, fmt.Errorf("fan-out %q reads its items from step %q, which the journal does not have", d.name, d.itemsFrom)
	}
	if sr.Status == StepSkipped {
		// A fan-out over a skipped step has nothing to iterate, which is an empty fan-out rather than an error
		return nil, nil
	}

	var items []json.RawMessage
	err := json.Unmarshal(def.byName[sr.Name].stepOutput(sr), &items)
	if err != nil {
		return nil, fmt.Errorf("fan-out %q requires step %q to output a JSON array: %w", d.name, d.itemsFrom, err)
	}
	return items, nil
}

// refreshFrames adds a frame for a step that gained something to compensate after the unwind had already opened
// That is the straggler case: an attempt the unwind abandoned was never interrupted, so a success it reports late is real work, and it goes on top of the stack because it completed last
func refreshFrames(st *instanceState, def *definition) bool {
	var changed bool
	for i := range st.Steps {
		sr := &st.Steps[i]
		if sr.Status != StepCompleted && sr.Status != StepFailed {
			continue
		}

		d := def.byName[sr.Name]
		if d == nil {
			continue
		}

		before := len(st.Stack)
		pushFrame(st, sr, d)
		if len(st.Stack) != before {
			changed = true
		}
	}
	return changed
}

// unwindNextFrame drives the compensation stack one frame at a time, in reverse order, and terminates the instance once it is empty
// A frame is fully compensated before the next one starts, which is the point of unwinding in reverse
func unwindNextFrame(st *instanceState, def *definition, now time.Time) bool {
	if len(st.Stack) == 0 {
		terminate(st, def, now)
		return true
	}

	name := st.Stack[len(st.Stack)-1]
	sr := st.step(name)
	d := def.byName[name]
	if sr == nil || d == nil {
		// A frame naming a step the journal or the definition no longer has cannot be unwound, so it is dropped rather than blocking the rest
		st.Stack = st.Stack[:len(st.Stack)-1]
		return true
	}

	// Opening the frame gives every task that has to be undone its first compensation attempt, numbered before it is dispatched
	if sr.Status != StepCompensating {
		sr.Status = StepCompensating
		for _, i := range compensableTasks(sr, d) {
			if sr.Tasks[i].Comp != nil {
				continue
			}
			sr.Tasks[i].Comp = &compRecord{Attempts: 1}
		}
		return true
	}

	// A late success can add an effect after the frame opened, so each pass allocates any compensation the journal learned it now owes
	var added bool
	for _, i := range compensableTasks(sr, d) {
		if sr.Tasks[i].Comp != nil {
			continue
		}
		sr.Tasks[i].Comp = &compRecord{Attempts: 1}
		added = true
	}
	if added {
		return true
	}

	// Within a frame the compensations run concurrently, since the tasks had no order between them going forward
	var failed bool
	for i := range sr.Tasks {
		c := sr.Tasks[i].Comp
		if c == nil {
			continue
		}
		if !c.Done {
			return false
		}
		if c.Error != "" {
			failed = true
		}
	}

	if failed {
		sr.Status = StepCompensationFailed
		st.Stack = st.Stack[:len(st.Stack)-1]

		if def.compensationFailurePolicy == AbortUnwinding {
			// Stopping here leaves the frames below untouched, and the stack is what names them for the operator
			st.Compensation = CompensationFailed
			terminate(st, def, now)
			return true
		}

		st.Compensation = CompensationPartial
		return true
	}

	sr.Status = StepCompensated
	st.Stack = st.Stack[:len(st.Stack)-1]
	return true
}

// isLateAbandonedSuccess reports whether a terminal journal still needs to account for work that cancellation or a timeout could not interrupt
func isLateAbandonedSuccess(st *instanceState, ev *event) bool {
	if ev.kind != evDone || ev.report == nil || ev.report.Error != "" {
		return false
	}
	sr := st.step(ev.report.Step)
	if sr == nil {
		return false
	}
	tr := sr.task(ev.report.Index)
	return tr != nil && tr.Done && tr.Abandoned
}

// recordChildOutcome keeps the child's terminal details beside the task so they remain visible after the child journal is purged
func recordChildOutcome(tr *taskRecord, p *reportPayload) {
	if p.ChildStatus == "" {
		return
	}
	tr.ChildStatus = p.ChildStatus
	tr.ChildCompensation = p.ChildCompensation
}

// configureTaskActors records every durable actor reference the task can create so later cleanup is independent of the deployed definition
func (def *definition) configureTaskActors(d *stepDef, tr *taskRecord, instanceID string, stepName string) {
	member := memberDef(d, tr.Index)

	child := member.child
	if child == nil {
		child = d.child
	}
	if child != nil {
		tr.ChildID = workerActorID(instanceID, stepName, tr.Index)
		tr.ChildType = child.baseType
		return
	}

	baseType := workflowActorTypePrefix + def.name
	tr.WorkerType = queueType(baseType+workerTypeSuffix, member.capability)
	if member.isCompensable() {
		tr.UndoType = queueType(baseType+undoTypeSuffix, member.capability)
	}
}

// terminate closes the instance, deciding its terminal status and its output
func terminate(st *instanceState, def *definition, now time.Time) {
	if st.Status.IsTerminal() {
		return
	}

	if st.Status == StatusCompensating {
		st.Status = st.TerminalStatus
		if st.Status == "" {
			st.Status = StatusFailed
		}
		// A partial or failed outcome was already recorded by the frame that produced it, so only the clean cases are decided here
		if st.Compensation == "" {
			if len(st.Stack) == 0 && anyCompensated(st) {
				st.Compensation = CompensationCompleted
			} else {
				st.Compensation = CompensationNone
			}
		}
		st.CompletedAt = now
		return
	}

	// A run that reached the end without unwinding is failed only when a step failed under WithSkipOnFailure, which continues but still costs the run
	st.Status = StatusCompleted
	for i := range st.Steps {
		if st.Steps[i].Status != StepFailed {
			continue
		}

		d := def.byName[st.Steps[i].Name]
		if d != nil && d.optional {
			continue
		}
		st.Status = StatusFailed
		if st.Cause == "" {
			st.Cause = fmt.Sprintf("step %q failed: %s", st.Steps[i].Name, st.Steps[i].Error)
		}
	}

	st.Compensation = CompensationNone
	st.Output = instanceOutput(st, def)
	st.CompletedAt = now
}

// alarmResolution is the resolution a provider stores an alarm's due time at, which is coarser than the nanoseconds the journal keeps
const alarmResolution = time.Millisecond

// deadlineTurnTime returns the instant a turn evaluates the journal at
//
// A deadline alarm fires because the provider reached the time the journal asked for, but the row holds that time truncated to the provider's own resolution, so the handler can run a fraction before the journal's own deadline
// Treating that instant as reached is what stops a deadline turn from finding nothing elapsed: the alarm is a one-shot, so a turn that changes nothing leaves the instance with no timer at all
// The tolerance is bounded to the alarm's resolution, so a deadline genuinely further out is never brought forward by an alarm that fired for something else
func (st *instanceState) deadlineTurnTime(ev *event, now time.Time) time.Time {
	if ev.kind != evDeadline || st.DeadlineAt.IsZero() || !now.Before(st.DeadlineAt) {
		return now
	}
	if st.DeadlineAt.Sub(now) > alarmResolution {
		return now
	}
	return st.DeadlineAt
}

// unwindAbandonedCause annotates the cause that opened an unwind with the fact that the unwind itself did not finish
// The original cause is kept, because why the instance started unwinding is still the first thing an operator asks
func unwindAbandonedCause(cause string) string {
	const abandoned = "unwind abandoned: instance timeout elapsed"
	if cause == "" {
		return abandoned
	}
	return cause + "; " + abandoned
}

// anyCompensated reports whether the unwind actually undid anything, which is what separates a clean rollback from an instance that had nothing on its stack
func anyCompensated(st *instanceState) bool {
	for i := range st.Steps {
		switch st.Steps[i].Status {
		case StepCompensated, StepCompensationFailed:
			return true
		}
	}
	return false
}

// instanceOutput returns the output a completed instance reports, which is the output of the step named by WithOutput or of the last step that produced one
func instanceOutput(st *instanceState, def *definition) json.RawMessage {
	if def.outputStep != "" {
		sr := st.step(def.outputStep)
		if sr == nil {
			return nil
		}
		return def.byName[sr.Name].stepOutput(sr)
	}

	for i := len(st.Steps) - 1; i >= 0; i-- {
		sr := &st.Steps[i]
		if sr.Status != StepCompleted {
			continue
		}
		out := def.byName[sr.Name].stepOutput(sr)
		if out != nil {
			return out
		}
	}
	return nil
}

// deriveCursor names the step the instance is on, purely so status reads and log lines do not have to walk the step list
// Nothing reads it, so if it ever disagreed with the records the records would win
func deriveCursor(st *instanceState) string {
	for i := range st.Steps {
		switch st.Steps[i].Status {
		case StepRunning, StepCompensating:
			return st.Steps[i].Name
		}
	}
	return ""
}

// currentRunningStep returns the step currently in flight and its definition, or nil when nothing is
func (st *instanceState) currentRunningStep(def *definition) (*stepRecord, *stepDef) {
	for i := range st.Steps {
		sr := &st.Steps[i]
		if sr.Status != StepRunning && sr.Status != StepCompensating {
			continue
		}
		return sr, def.byName[sr.Name]
	}
	return nil, nil
}

// effectiveMaxAttempts returns how many attempts a forward task of a step gets
func effectiveMaxAttempts(d *stepDef) int {
	if d.maxAttempts > 0 {
		return d.maxAttempts
	}
	return defaultMaxAttempts
}

// effectiveCompMaxAttempts returns how many attempts a step's compensation gets
func effectiveCompMaxAttempts(d *stepDef) int {
	if d.compMaxAttempt > 0 {
		return d.compMaxAttempt
	}
	return defaultCompMaxAttempts
}

// groupPolicy returns the failure policy of a group or a fan-out, which defaults to failing on the first failure
func groupPolicy(d *stepDef) FailurePolicy {
	if d.failurePolicy != "" {
		return d.failurePolicy
	}
	return FailFast
}

// backoff returns the delay before an attempt, doubling from the initial delay and stopping at the cap
func backoff(initial time.Duration, max time.Duration, defInitial time.Duration, defMax time.Duration, retries int) time.Duration {
	if initial <= 0 {
		initial = defInitial
	}
	if max <= 0 {
		max = defMax
	}

	d := initial
	for range retries - 1 {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

// until returns how much of a deadline is left, never going below zero so a deadline already past resumes as due immediately
func until(deadline time.Time, now time.Time) time.Duration {
	if deadline.IsZero() {
		return 0
	}
	d := deadline.Sub(now)
	if d < 0 {
		return 0
	}
	return d
}

// instanceDeadline returns when the instance timeout elapses
func instanceDeadline(st *instanceState, def *definition) time.Time {
	start := st.StartedAt
	if start.IsZero() {
		start = st.CreatedAt
	}
	if start.IsZero() {
		return time.Time{}
	}
	timeout := st.Timeout
	if timeout <= 0 {
		timeout = def.timeout
	}
	return start.Add(timeout)
}

// stepBudget returns how long a step is allowed to take, which for a wait step is how long it waits for its event
func (d *stepDef) stepBudget() time.Duration {
	if d.kind == KindWait {
		return d.eventTimeout
	}
	return d.stepTimeout
}

// stepDeadline returns when a running step's own timeout elapses, or the zero time when it declared none
func (d *stepDef) stepDeadline(sr *stepRecord) time.Time {
	budget := d.stepBudget()
	if budget <= 0 || sr.StartedAt.IsZero() {
		return time.Time{}
	}
	return sr.StartedAt.Add(budget)
}
