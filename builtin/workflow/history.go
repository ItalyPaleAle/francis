package workflow

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
)

// eventData is the MessagePack body of one history event, which carries no payloads
type eventData struct {
	TimeSource string         `msgpack:"timeSource,omitempty"`
	Step       string         `msgpack:"step,omitempty"`
	TaskIndex  *int           `msgpack:"taskIndex,omitempty"`
	Attempt    int            `msgpack:"attempt,omitempty"`
	Iteration  int            `msgpack:"iteration,omitempty"`
	Undo       bool           `msgpack:"undo,omitempty"`
	Outcome    string         `msgpack:"outcome,omitempty"`
	Error      string         `msgpack:"error,omitempty"`
	Reason     string         `msgpack:"reason,omitempty"`
	EventName  string         `msgpack:"eventName,omitempty"`
	DueTime    time.Time      `msgpack:"dueTime,omitzero"`
	Child      *childLinkData `msgpack:"child,omitempty"`
}

// childLinkData identifies a child instance inside an event body
type childLinkData struct {
	Workflow   string `msgpack:"workflow"`
	InstanceID string `msgpack:"instanceId"`
	ActorType  string `msgpack:"actorType"`
}

// historyEntry is one event before it is numbered
type historyEntry struct {
	kind string
	at   time.Time
	data eventData
}

// taskKey identifies one task, or its compensation, among the events of a turn
type taskKey struct {
	step  string
	index int
	undo  bool
}

// turnHistory holds what a turn learned from its event that the journal does not record, so the next persist can append it
// It is rebuilt by every turn, and dropped once a persist consumes it or fails, so a retried turn produces exactly the same events
type turnHistory struct {
	// requests are events the turn's own request produced, such as a cancel with its reason
	requests []historyEntry
	// workers are the worker-reported start and finish events, by task, which travel with the task report
	workers map[taskKey][]historyEntry
	// report marks a turn driven by a task report, so task outcomes are labelled with report time
	report bool
}

// captureStepHistory keeps an overwritten forward settlement out of the durable journal
func (st *instanceState) captureStepHistory(sr *stepRecord) {
	if st.NoEventHistory {
		return
	}
	switch sr.Status {
	case StepCompleted, StepFailed, StepSkipped:
		st.stepHistory = append(st.stepHistory, sr.clone())
	}
}

// beginTurnHistory records what a non-duplicate event contributes to the history beyond what the journal diff shows
func (o *orchestrator) beginTurnHistory(ev *event, duplicate bool, now time.Time) {
	o.history = nil
	if duplicate {
		return
	}

	h := &turnHistory{}
	switch ev.kind {
	case evDone:
		h.report = true
		if ev.report != nil {
			h.addWorker(
				taskKey{step: ev.report.Step, index: ev.report.Index},
				ev.report.Attempt,
				ev.report.StartedAt,
				ev.report.FinishedAt,
				ev.report.Error,
			)
		}
	case evCompensated:
		h.report = true
		if ev.comp != nil {
			h.addWorker(
				taskKey{step: ev.comp.Step, index: ev.comp.Index, undo: true},
				ev.comp.Attempt,
				ev.comp.StartedAt,
				ev.comp.FinishedAt,
				ev.comp.Error,
			)
		}
	case evCancel:
		h.requests = append(h.requests, historyEntry{
			kind: EventKindCancelRequested,
			at:   now,
			data: eventData{TimeSource: TimeSourceEngine, Reason: ev.reason},
		})
	case evUnwind:
		h.requests = append(h.requests, historyEntry{
			kind: EventKindUnwindRequested,
			at:   now,
			data: eventData{TimeSource: TimeSourceEngine, Reason: ev.reason, Attempt: ev.compAttempt},
		})
	}
	o.history = h
}

// addWorker records the worker's own start and finish times of one attempt, when the report carries them
func (h *turnHistory) addWorker(key taskKey, attempt int, startedAt time.Time, finishedAt time.Time, errMsg string) {
	if startedAt.IsZero() && finishedAt.IsZero() {
		return
	}
	if h.workers == nil {
		h.workers = map[taskKey][]historyEntry{}
	}

	index := key.index
	if !startedAt.IsZero() {
		h.workers[key] = append(h.workers[key], historyEntry{
			kind: EventKindWorkerStarted,
			at:   startedAt,
			data: eventData{TimeSource: TimeSourceWorker, Step: key.step, TaskIndex: &index, Attempt: attempt, Undo: key.undo},
		})
	}
	if !finishedAt.IsZero() {
		outcome := "succeeded"
		if errMsg != "" {
			outcome = "failed"
		}
		h.workers[key] = append(h.workers[key], historyEntry{
			kind: EventKindWorkerFinished,
			at:   finishedAt,
			data: eventData{TimeSource: TimeSourceWorker, Step: key.step, TaskIndex: &index, Attempt: attempt, Undo: key.undo, Outcome: outcome, Error: errMsg},
		})
	}
}

// attachHistory numbers the events a write adds on top of the last committed journal, and attaches them to the write
// The sequence continues from the committed journal rather than from st, so recomputing the events for the same write numbers them the same way
func (o *orchestrator) attachHistory(ctx context.Context, opts *actor.SetStateOpts, st *instanceState, now time.Time, extra ...historyEntry) error {
	if st.NoEventHistory {
		opts.SetAppendEvents(builtinkey.Key{}, nil)
		return nil
	}

	base, err := o.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the committed workflow journal: %w", err)
	}

	entries := o.historyEntries(&base, st, now)
	entries = append(entries, extra...)

	seq := base.LastEventSeq
	if base.Status == "" {
		// The first journal starts the history over, which is what makes the provider drop the events of an earlier instance that had the same ID
		seq = 0
	}
	events := make([]components.WorkflowEvent, 0, len(entries))
	for _, entry := range entries {
		data, encErr := msgpack.Marshal(&entry.data)
		if encErr != nil {
			return fmt.Errorf("failed to encode a workflow event: %w", encErr)
		}
		seq++
		events = append(events, components.WorkflowEvent{
			Seq:  seq,
			Time: entry.at,
			Kind: entry.kind,
			Data: data,
		})
	}

	st.LastEventSeq = seq
	opts.SetAppendEvents(builtinkey.Key{}, events)
	return nil
}

// historyEntries derives the events a write adds by comparing the journal about to be written with the committed one
// Overwritten forward settlements are replayed before the final step state, preserving loop iteration and failure ordering
func (o *orchestrator) historyEntries(base *instanceState, st *instanceState, now time.Time) []historyEntry {
	b := &historyBuilder{
		o:       o,
		now:     now,
		acks:    make(map[dispatchKey]time.Time, len(o.unsavedDispatches)),
		workers: map[taskKey][]historyEntry{},
	}
	for _, ack := range o.unsavedDispatches {
		b.acks[dispatchKey{step: ack.step, index: ack.index, undo: ack.undo, attempt: ack.attempt}] = ack.at
	}
	if o.history != nil {
		b.report = o.history.report
		maps.Copy(b.workers, o.history.workers)
	}

	// Instance-level changes that precede the steps they affect
	if base.Status == "" && st.Status != "" {
		b.add(EventKindInstanceStarted, now, eventData{TimeSource: TimeSourceEngine})
	}
	if o.history != nil {
		b.entries = append(b.entries, o.history.requests...)
	}
	if base.Status.IsTerminal() && base.Status != st.Status {
		b.add(EventKindInstanceReopened, now, eventData{TimeSource: TimeSourceEngine})
	}
	if base.Status == StatusSuspended && st.Status != StatusSuspended {
		b.add(EventKindInstanceResumed, now, eventData{TimeSource: TimeSourceEngine})
	}

	// Forward progress of every step, then the unwind that it may have opened, then the compensation of every step
	for i := range st.Steps {
		before := stepBefore(base, st, i)
		for j := range st.stepHistory {
			transition := &st.stepHistory[j]
			if transition.Name != st.Steps[i].Name {
				continue
			}
			b.forwardStep(before, transition)
			before = transition
		}
		b.forwardStep(before, &st.Steps[i])
	}
	if st.TerminalStatus != "" && (base.TerminalStatus == "" || base.Status.IsTerminal() && !st.Status.IsTerminal()) {
		b.add(EventKindCompensationStarted, now, eventData{TimeSource: TimeSourceEngine, Reason: st.Cause, Outcome: string(st.TerminalStatus)})
	}
	for i := range st.Steps {
		b.compensationStep(stepBefore(base, st, i), &st.Steps[i])
	}

	// Worker events for tasks the journal no longer lists are still recorded, after everything else the report changed
	for _, key := range sortedTaskKeys(b.workers) {
		b.entries = append(b.entries, b.workers[key]...)
	}

	// Instance-level changes that follow the steps they summarize
	if st.Status == StatusSuspended && base.Status != StatusSuspended {
		data := eventData{TimeSource: TimeSourceEngine}
		if st.Suspended != nil {
			data.Reason = st.Suspended.Reason
		}
		b.add(EventKindInstanceSuspended, now, data)
	}
	if st.Status.IsTerminal() && base.Status != st.Status {
		b.add(terminalEventKind(st.Status), now, eventData{TimeSource: TimeSourceEngine, Outcome: string(st.Compensation), Error: st.Cause})
	}
	return b.entries
}

// dispatchKey identifies one accepted dispatch among the markers this activation holds
type dispatchKey struct {
	step    string
	index   int
	undo    bool
	attempt int
}

// historyBuilder accumulates the events of one write in order
type historyBuilder struct {
	o       *orchestrator
	now     time.Time
	report  bool
	acks    map[dispatchKey]time.Time
	workers map[taskKey][]historyEntry
	entries []historyEntry
}

// add appends one event
func (b *historyBuilder) add(kind string, at time.Time, data eventData) {
	b.entries = append(b.entries, historyEntry{kind: kind, at: at, data: data})
}

// outcomeSource is the time source of a task outcome, which is the report's processing time when a report drove the turn
func (b *historyBuilder) outcomeSource() string {
	if b.report {
		return TimeSourceReport
	}

	return TimeSourceEngine
}

// dispatchTime returns when an attempt was dispatched, which this activation knows for the markers it has not saved yet
func (b *historyBuilder) dispatchTime(key dispatchKey) (time.Time, string) {
	at, ok := b.acks[key]
	if !ok || at.IsZero() {
		return b.now, TimeSourceEngine
	}

	return at, TimeSourceDispatch
}

// takeWorkers returns and forgets the worker events of one task
func (b *historyBuilder) takeWorkers(key taskKey) []historyEntry {
	entries := b.workers[key]
	delete(b.workers, key)
	return entries
}

// forwardStep records a step's forward transitions and those of its tasks
func (b *historyBuilder) forwardStep(bs *stepRecord, sr *stepRecord) {
	iterationChanged := bs.Iteration != sr.Iteration
	statusChanged := bs.Status != sr.Status || iterationChanged
	base := eventData{TimeSource: TimeSourceEngine, Step: sr.Name, Iteration: sr.Iteration}
	if sr.Kind == KindWait {
		d := b.o.def.byName[sr.Name]
		if d != nil {
			base.EventName = d.eventName
		}
	}

	// The step opening, and the event a wait step received
	if sr.Status == StepRunning && statusChanged {
		kind := EventKindStepStarted
		if sr.Kind == KindWait {
			kind = EventKindWaitStarted
		}
		b.add(kind, b.now, base)
	}
	if sr.Kind == KindWait && len(sr.Event) > 0 && (len(bs.Event) == 0 || iterationChanged) {
		b.add(EventKindEventReceived, b.now, base)
	}

	// Every task's dispatch, worker activity, and outcome
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		b.forwardTask(sr, bs.task(tr.Index), tr)
	}

	// The step settling
	if !statusChanged {
		return
	}
	switch sr.Status {
	case StepCompleted:
		b.add(EventKindStepCompleted, b.now, base)
	case StepFailed:
		data := base
		data.Error = sr.Error
		b.add(EventKindStepFailed, b.now, data)
	case StepSkipped:
		b.add(EventKindStepSkipped, b.now, base)
	}
}

// forwardTask records one task's dispatch, worker activity, and forward outcome
func (b *historyBuilder) forwardTask(sr *stepRecord, bt *taskRecord, tr *taskRecord) {
	var before taskRecord
	if bt != nil {
		before = *bt
	}
	index := tr.Index
	child := childLink(tr)

	// A newly accepted dispatch, which for a child task is the child's start
	if tr.DispatchedAttempt > before.DispatchedAttempt {
		at, source := b.dispatchTime(dispatchKey{step: sr.Name, index: tr.Index, attempt: tr.DispatchedAttempt})
		kind := EventKindTaskDispatched
		if tr.ChildID != "" {
			kind = EventKindChildStarted
		}

		data := eventData{TimeSource: source, Step: sr.Name, TaskIndex: &index, Attempt: tr.DispatchedAttempt, Child: child}
		if !tr.RetryAt.IsZero() && tr.RetryAt.After(at) {
			data.DueTime = tr.RetryAt
		}

		b.add(kind, at, data)
	}

	b.entries = append(b.entries, b.takeWorkers(taskKey{step: sr.Name, index: tr.Index})...)

	// A retry scheduled after a failed attempt
	if bt != nil && before.Attempts > 0 && tr.Attempts > before.Attempts && !tr.Done {
		b.add(EventKindTaskRetryScheduled, b.now, eventData{
			TimeSource: b.outcomeSource(), Step: sr.Name, TaskIndex: &index, Attempt: tr.Attempts, Error: tr.LastError, DueTime: tr.RetryAt, Child: child,
		})
	}

	// The task's forward outcome, including a success an abandoned attempt reported late
	lateSuccess := before.Done && before.Abandoned && tr.Done && !tr.Abandoned
	if tr.Done && (!before.Done || lateSuccess) {
		data := eventData{TimeSource: b.outcomeSource(), Step: sr.Name, TaskIndex: &index, Attempt: tr.Attempts, Child: child, Outcome: string(tr.ChildStatus)}
		switch {
		case tr.Abandoned:
			data.TimeSource = TimeSourceEngine
			data.Error = tr.Error
			b.add(EventKindTaskAbandoned, b.now, data)
		case tr.Error != "":
			data.Error = tr.Error
			b.add(EventKindTaskFailed, b.now, data)
		default:
			if lateSuccess {
				data.Reason = "late success of an abandoned attempt"
			}
			b.add(EventKindTaskCompleted, b.now, data)
		}
	}
}

// compensationStep records a step's unwind transitions and those of its tasks' compensations
func (b *historyBuilder) compensationStep(bs *stepRecord, sr *stepRecord) {
	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		b.compensationTask(sr, bs.task(tr.Index), tr)
	}

	if bs.Status == sr.Status && bs.Iteration == sr.Iteration {
		return
	}
	base := eventData{TimeSource: TimeSourceEngine, Step: sr.Name, Iteration: sr.Iteration}
	switch sr.Status {
	case StepCompensating:
		b.add(EventKindStepCompensating, b.now, base)
	case StepCompensated:
		b.add(EventKindStepCompensated, b.now, base)
	case StepCompensationFailed:
		b.add(EventKindStepCompensationFailed, b.now, base)
	}
}

// compensationTask records one compensation's dispatch, worker activity, and outcome
func (b *historyBuilder) compensationTask(sr *stepRecord, bt *taskRecord, tr *taskRecord) {
	c := tr.Comp
	var before compRecord
	if bt != nil && bt.Comp != nil {
		before = *bt.Comp
	}
	index := tr.Index
	child := childLink(tr)
	key := taskKey{step: sr.Name, index: tr.Index, undo: true}
	if c == nil {
		b.entries = append(b.entries, b.takeWorkers(key)...)
		return
	}

	// A compensation record that replaced an obsolete one starts its bookkeeping over
	if c.GenerationStart != before.GenerationStart {
		before = compRecord{}
	}

	// A newly accepted dispatch, which for a child task asks the child to unwind itself
	if c.DispatchedAttempt > before.DispatchedAttempt {
		at, source := b.dispatchTime(dispatchKey{step: sr.Name, index: tr.Index, undo: true, attempt: c.DispatchedAttempt})
		kind := EventKindCompensationDispatched
		if tr.ChildID != "" {
			kind = EventKindChildUnwindRequested
		}

		data := eventData{TimeSource: source, Step: sr.Name, TaskIndex: &index, Attempt: c.DispatchedAttempt, Undo: true, Child: child}
		if !c.RetryAt.IsZero() && c.RetryAt.After(at) {
			data.DueTime = c.RetryAt
		}

		b.add(kind, at, data)
	}

	b.entries = append(b.entries, b.takeWorkers(key)...)

	// A retry scheduled after a failed compensation attempt
	if before.Attempts > 0 && c.Attempts > before.Attempts && !c.Done {
		b.add(EventKindCompensationRetryScheduled, b.now, eventData{
			TimeSource: b.outcomeSource(), Step: sr.Name, TaskIndex: &index, Attempt: c.Attempts, Undo: true, Error: c.LastError, DueTime: c.RetryAt, Child: child,
		})
	}

	// The compensation's outcome
	if c.Done && !before.Done {
		data := eventData{TimeSource: b.outcomeSource(), Step: sr.Name, TaskIndex: &index, Attempt: c.Attempts, Undo: true, Child: child, Outcome: string(tr.ChildCompensation)}
		if c.Error != "" {
			data.Error = c.Error
			b.add(EventKindCompensationFailed, b.now, data)
		} else {
			b.add(EventKindCompensationCompleted, b.now, data)
		}
	}
}

// stepBefore returns the committed record of the step at position i of st, or an empty record for a step the committed journal does not have
func stepBefore(base *instanceState, st *instanceState, i int) *stepRecord {
	name := st.Steps[i].Name
	if i < len(base.Steps) && base.Steps[i].Name == name {
		return &base.Steps[i]
	}
	sr := base.step(name)
	if sr == nil {
		return &stepRecord{}
	}
	return sr
}

// childLink identifies the child instance a task started, or returns nil for a task that runs on a worker
func childLink(tr *taskRecord) *childLinkData {
	if tr.ChildID == "" {
		return nil
	}
	return &childLinkData{
		Workflow:   strings.TrimPrefix(tr.ChildType, workflowActorTypePrefix),
		InstanceID: tr.ChildID,
		ActorType:  ref.BuiltInActorTypePrefix + tr.ChildType,
	}
}

// terminalEventKind returns the event recorded when an instance reaches a terminal status
func terminalEventKind(s Status) string {
	switch s {
	case StatusCompleted:
		return EventKindInstanceCompleted
	case StatusCancelled:
		return EventKindInstanceCancelled
	default:
		return EventKindInstanceFailed
	}
}

// sortedTaskKeys orders leftover worker events deterministically, so a retried turn numbers them the same way
func sortedTaskKeys(m map[taskKey][]historyEntry) []taskKey {
	keys := make([]taskKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a taskKey, b taskKey) int {
		c := strings.Compare(a.step, b.step)
		switch {
		case c != 0:
			return c
		case a.index != b.index:
			return a.index - b.index
		case a.undo == b.undo:
			return 0
		case !a.undo:
			return -1
		default:
			return 1
		}
	})
	return keys
}
