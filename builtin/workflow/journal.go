package workflow

import (
	"bytes"
	"encoding/json"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Status is the lifecycle stage of a workflow instance
type Status string

const (
	// StatusPending indicates the start job is durable but has not run yet
	StatusPending Status = "pending"
	// StatusRunning indicates the instance is executing its steps
	StatusRunning Status = "running"
	// StatusSuspended indicates the instance was paused by Suspend, and suspendRecord.ResumeTo records what it was
	StatusSuspended Status = "suspended"
	// StatusCompensating indicates the instance is unwinding its compensation stack
	StatusCompensating Status = "compensating"
	// StatusCompleted is the terminal status of an instance that ran to the end
	StatusCompleted Status = "completed"
	// StatusFailed is the terminal status of an instance that could not
	StatusFailed Status = "failed"
	// StatusCancelled is the terminal status of an instance that was cancelled and unwound
	StatusCancelled Status = "cancelled"
)

// IsTerminal reports whether the instance has reached an end state, after which it ignores everything but a purge
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// CompensationOutcome records how far the unwind got, and is empty until the instance reaches a terminal status
// It is carried alongside Status rather than multiplying terminal statuses, so "failed" always means the same thing and the rollback is reported separately
type CompensationOutcome string

const (
	// CompensationNone indicates nothing needed unwinding
	CompensationNone CompensationOutcome = "none"
	// CompensationCompleted indicates every frame unwound
	CompensationCompleted CompensationOutcome = "completed"
	// CompensationPartial indicates some frames failed and the rest were unwound
	CompensationPartial CompensationOutcome = "partial"
	// CompensationFailed indicates the unwind stopped early at a failed frame
	CompensationFailed CompensationOutcome = "failed"
)

// StepStatus is the lifecycle stage of one step of an instance
type StepStatus string

const (
	// StepPending indicates the step has not been reached yet
	StepPending StepStatus = "pending"
	// StepRunning indicates at least one of the step's tasks is scheduled or in flight
	StepRunning StepStatus = "running"
	// StepCompleted indicates the step produced its output
	StepCompleted StepStatus = "completed"
	// StepFailed indicates the step failed for good
	StepFailed StepStatus = "failed"
	// StepSkipped indicates an upstream failure or condition meant the step was never run
	StepSkipped StepStatus = "skipped"
	// StepCompensating indicates the step's frame is being unwound
	StepCompensating StepStatus = "compensating"
	// StepCompensated indicates the step's frame was unwound
	StepCompensated StepStatus = "compensated"
	// StepCompensationFailed indicates the step's frame could not be unwound
	StepCompensationFailed StepStatus = "compensation-failed"
)

// instanceState is the journal: the Workflow actor's durable state and the single source of truth for an instance
// Every transition rewrites the whole document in one state write, so a step transition is atomic
type instanceState struct {
	Workflow              string `msgpack:"workflow"`
	Version               int    `msgpack:"version"`
	DefinitionFingerprint string `msgpack:"definitionFingerprint,omitempty"`
	RegistryGeneration    uint64 `msgpack:"registryGeneration,omitempty"`
	// RegistryConfirmed and RegistryRejected record the durable decision reached after publishing a generation-bound start
	RegistryConfirmed bool                `msgpack:"registryConfirmed,omitempty"`
	RegistryRejected  bool                `msgpack:"registryRejected,omitempty"`
	Status            Status              `msgpack:"status"`
	Compensation      CompensationOutcome `msgpack:"compensation,omitempty"`
	Input             json.RawMessage     `msgpack:"input,omitempty"`
	// Timeout and UnknownVersion preserve the policies needed when no host has this journal's definition
	Timeout        time.Duration        `msgpack:"timeout,omitempty"`
	UnknownVersion UnknownVersionPolicy `msgpack:"unknownVersion,omitempty"`
	// EventNames and MaxEventSize let callers validate events against the target journal during rolling deployments
	EventNames   []string `msgpack:"eventNames,omitempty"`
	MaxEventSize int      `msgpack:"maxEventSize,omitempty"`
	// Output is set at completion, from the last step or the one named with WithOutput
	Output json.RawMessage `msgpack:"output,omitempty"`
	// Cursor is derived by advance for display and is never read by it
	Cursor string `msgpack:"cursor"`
	// Steps holds every step of the definition, recorded at Start, so status and the unknown-version path are answerable from the journal alone
	Steps []stepRecord `msgpack:"steps"`
	// Stack holds the compensation frames, by step name, oldest first
	Stack []string `msgpack:"stack,omitempty"`
	// Cause records what triggered the unwind, and is handed to every compensation
	Cause string `msgpack:"cause,omitempty"`
	// TerminalStatus is the status an unwind terminates into, decided when the unwind opened, so a cancelled instance is not reported as merely failed
	TerminalStatus Status `msgpack:"terminalStatus,omitempty"`
	// DeadlineAt is the last-armed deadline, so a turn that computes the same time does not re-write the alarm
	DeadlineAt time.Time `msgpack:"deadlineAt,omitzero"`
	// TraceParent is the Start call's trace context, which every span of the instance links to
	TraceParent string         `msgpack:"traceParent,omitempty"`
	Suspended   *suspendRecord `msgpack:"suspended,omitempty"`
	Parent      *parentRef     `msgpack:"parent,omitempty"`
	// Reported records that this instance's terminal outcome has been dispatched to its parent, so a retried turn does not report it twice
	Reported bool `msgpack:"reported,omitempty"`
	// Reopened records that this instance became active after its first termination so lifecycle instruments do not count it as a second instance
	Reopened    bool      `msgpack:"reopened,omitempty"`
	CreatedAt   time.Time `msgpack:"createdAt"`
	StartedAt   time.Time `msgpack:"startedAt"`
	CompletedAt time.Time `msgpack:"completedAt,omitzero"`
	// encoded reuses the exact size-check encoding when the provider serializes this state immediately afterward
	encoded []byte
}

// instanceStateWire avoids recursively calling MarshalMsgpack while encoding the journal fields
type instanceStateWire instanceState

// MarshalMsgpack lets persistence reuse the exact wire encoding already produced by the journal size check
func (st instanceState) MarshalMsgpack() ([]byte, error) {
	if st.encoded != nil {
		return st.encoded, nil
	}
	return msgpack.Marshal(instanceStateWire(st))
}

// stepRecord is one step of the definition as the journal sees it
type stepRecord struct {
	Name   string       `msgpack:"name"`
	Kind   Kind         `msgpack:"kind"`
	Status StepStatus   `msgpack:"status"`
	Tasks  []taskRecord `msgpack:"tasks,omitempty"`
	// Remaining counts the tasks that have not reported, so checking a wide fan-out for completion is O(1)
	Remaining int `msgpack:"remaining"`
	// Event holds the payload a WaitForEvent step was completed with
	Event json.RawMessage `msgpack:"event,omitempty"`
	// Error is the reason the step failed, once it has
	Error       string    `msgpack:"error,omitempty"`
	StartedAt   time.Time `msgpack:"startedAt,omitzero"`
	CompletedAt time.Time `msgpack:"completedAt,omitzero"`
}

// taskRecord is one execution unit of a step: one worker actor, one durable job, and the attempts it took
type taskRecord struct {
	Index int `msgpack:"index"`
	// DispatchedAttempt records the latest forward attempt whose job was durably accepted
	DispatchedAttempt int `msgpack:"dispatchedAttempt,omitempty"`
	// Item is this task's fan-out element
	Item json.RawMessage `msgpack:"item,omitempty"`
	// WorkerType and UndoType persist the queues this task used so purge does not depend on the definition that happens to be deployed later
	WorkerType string `msgpack:"workerType,omitempty"`
	UndoType   string `msgpack:"undoType,omitempty"`
	// ChildID and ChildType identify the child independently of the definition that happens to be deployed when the parent is purged
	ChildID   string `msgpack:"childId,omitempty"`
	ChildType string `msgpack:"childType,omitempty"`
	// ChildStatus and ChildCompensation retain the child's terminal outcome in the parent journal
	ChildStatus       Status              `msgpack:"childStatus,omitempty"`
	ChildCompensation CompensationOutcome `msgpack:"childCompensation,omitempty"`
	// Attempts is the number of the attempt currently scheduled or in flight, recorded before that attempt is dispatched
	Attempts int `msgpack:"attempts"`
	// RetryAt is the earliest the next attempt may run
	RetryAt   time.Time       `msgpack:"retryAt,omitzero"`
	LastError string          `msgpack:"lastError,omitempty"`
	Output    json.RawMessage `msgpack:"output,omitempty"`
	// Error is set once the task has failed for good
	Error string `msgpack:"error,omitempty"`
	Done  bool   `msgpack:"done"`
	// Abandoned marks a task an unwind closed out before it reported, which keeps a late success countable: the work really happened, so it is recorded and compensated like any other
	Abandoned bool `msgpack:"abandoned,omitempty"`
	// Compensation tracks the undo of this task once its frame is being unwound
	Comp        *compRecord `msgpack:"comp,omitempty"`
	Compensated bool        `msgpack:"compensated,omitempty"`
	CompletedAt time.Time   `msgpack:"completedAt,omitzero"`
}

// compRecord is a task's compensation, which gets its own attempts because a failed rollback is worth trying harder than the forward work was
type compRecord struct {
	// GenerationStart fences reports from undo attempts dispatched before the latest forward outcome was recorded
	GenerationStart int `msgpack:"generationStart,omitempty"`
	// DispatchedAttempt records the latest compensation attempt whose job was durably accepted
	DispatchedAttempt int       `msgpack:"dispatchedAttempt,omitempty"`
	Attempts          int       `msgpack:"attempts"`
	RetryAt           time.Time `msgpack:"retryAt,omitzero"`
	LastError         string    `msgpack:"lastError,omitempty"`
	// Error is set once the compensation has failed for good
	Error string `msgpack:"error,omitempty"`
	Done  bool   `msgpack:"done"`
}

// suspendRecord is what a suspended instance remembers, so Resume can put the deadlines back where it found them
type suspendRecord struct {
	At     time.Time `msgpack:"at"`
	Reason string    `msgpack:"reason,omitempty"`
	// ResumeTo is the status the instance goes back to
	ResumeTo Status `msgpack:"resumeTo"`
	// RemainingTimeout is what was left of the instance timeout, so a two-day pause does not eat a thirty-minute budget
	RemainingTimeout time.Duration `msgpack:"remainingTimeout"`
	// RemainingStepTimeout is what was left of the current step's timeout, including a WaitForEvent step's event timeout
	RemainingStepTimeout time.Duration `msgpack:"remainingStepTimeout,omitempty"`
}

// parentRef identifies the parent instance of a child, so a child is locatable from the parent's journal and reports back to it
type parentRef struct {
	InstanceID string `msgpack:"instanceId"`
	Workflow   string `msgpack:"workflow"`
	Step       string `msgpack:"step"`
	Index      int    `msgpack:"index"`
	Depth      int    `msgpack:"depth"`
	// Attempt is the parent task's attempt number, which this instance's report is keyed by
	Attempt int `msgpack:"attempt,omitempty"`
	// UnwoundBy is the parent's compensation attempt number when the parent asked this instance to undo itself, and zero otherwise
	// It is why a child cancelled by its parent reports a compensation, while one cancelled by a caller reports an ordinary failed result
	UnwoundBy int `msgpack:"unwoundBy,omitempty"`
}

// step returns the record of a named step, or nil when the journal has no step by that name
// A journal outlives definition changes, so a name that no longer exists is a normal outcome rather than a programming error
func (st *instanceState) step(name string) *stepRecord {
	for i := range st.Steps {
		if st.Steps[i].Name == name {
			return &st.Steps[i]
		}
	}
	return nil
}

// task returns the record of one task of a step, or nil when the index is outside what the journal recorded
func (sr *stepRecord) task(index int) *taskRecord {
	for i := range sr.Tasks {
		if sr.Tasks[i].Index == index {
			return &sr.Tasks[i]
		}
	}
	return nil
}

// clone returns an independent journal so a failed write cannot mutate the activation's cached committed snapshot through shared slices or pointers
func (st *instanceState) clone() instanceState {
	out := *st
	out.encoded = nil
	out.Input = cloneRawMessage(st.Input)
	out.Output = cloneRawMessage(st.Output)
	out.EventNames = append([]string(nil), st.EventNames...)
	out.Stack = append([]string(nil), st.Stack...)
	if st.Suspended != nil {
		rec := *st.Suspended
		out.Suspended = &rec
	}
	if st.Parent != nil {
		parent := *st.Parent
		out.Parent = &parent
	}

	out.Steps = make([]stepRecord, len(st.Steps))
	for i := range st.Steps {
		out.Steps[i] = st.Steps[i].clone()
	}
	return out
}

// clone returns an independent step record including every task and encoded value it owns
func (sr *stepRecord) clone() stepRecord {
	out := *sr
	out.Event = cloneRawMessage(sr.Event)
	out.Tasks = make([]taskRecord, len(sr.Tasks))
	for i := range sr.Tasks {
		out.Tasks[i] = sr.Tasks[i].clone()
	}
	return out
}

// clone returns an independent task record including its compensation bookkeeping
func (tr *taskRecord) clone() taskRecord {
	out := *tr
	out.Item = cloneRawMessage(tr.Item)
	out.Output = cloneRawMessage(tr.Output)
	if tr.Comp != nil {
		comp := *tr.Comp
		out.Comp = &comp
	}
	return out
}

// cloneRawMessage preserves the distinction between a nil value and an empty non-nil value while breaking ownership of its backing array
func cloneRawMessage(value json.RawMessage) json.RawMessage {
	return bytes.Clone(value)
}
