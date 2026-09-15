package workflow

import (
	"encoding/json"
	"time"
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
// Every transition rewrites the whole document in one state write, so a step transition is atomic (§12.2)
type instanceState struct {
	Workflow     string              `msgpack:"workflow"`
	Version      int                 `msgpack:"version"`
	Status       Status              `msgpack:"status"`
	Compensation CompensationOutcome `msgpack:"compensation,omitempty"`
	Input        json.RawMessage     `msgpack:"input,omitempty"`
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
	Reported    bool      `msgpack:"reported,omitempty"`
	CreatedAt   time.Time `msgpack:"createdAt"`
	StartedAt   time.Time `msgpack:"startedAt"`
	CompletedAt time.Time `msgpack:"completedAt,omitzero"`
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
	// Item is this task's fan-out element
	Item json.RawMessage `msgpack:"item,omitempty"`
	// ChildID is the instance ID of the child a child task runs
	ChildID string `msgpack:"childId,omitempty"`
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

// compRecord is a task's compensation, which gets its own attempts because a failed rollback is worth trying harder than the forward work was (§9.3)
type compRecord struct {
	Attempts  int       `msgpack:"attempts"`
	RetryAt   time.Time `msgpack:"retryAt,omitzero"`
	LastError string    `msgpack:"lastError,omitempty"`
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
