package workflow

import (
	"encoding/json"
	"time"
)

// Job methods, which are what every durable job of the engine is dispatched as
const (
	// methodStart begins an instance, and is dispatched by Service.Start or by a parent
	methodStart = "start"
	// methodDone carries a worker's or a child's outcome for one task back to the orchestrator
	methodDone = "done"
	// methodCompensated carries an undo worker's outcome for one task's compensation back to the orchestrator
	methodCompensated = "compensated"
	// methodEvent delivers an external event to a WaitForEvent step
	methodEvent = "event"
	// methodCancel asks a running or suspended instance to stop and unwind
	methodCancel = "cancel"
	// methodUnwind moves a completed child back into compensating, and is the one verb only a parent may send
	methodUnwind = "unwind"
	// methodSuspend and methodResume pause and continue an instance
	methodSuspend = "suspend"
	methodResume  = "resume"
	// methodRun delivers one attempt of a forward task to a worker
	methodRun = "run"
	// methodCompensate delivers one attempt of a compensation to an undo worker
	methodCompensate = "compensate"
	// methodPurge removes a terminated instance, and is invoked on the orchestrator so the removal is serialized with its own turns
	methodPurge = "purge"
	// methodStatus reads the journal through a Peek, so status reads never queue behind each other
	methodStatus = "status"
	// methodRegister answers the registry's consistency check
	methodRegister = "register"
	// methodDefinitions lists what the registry holds
	methodDefinitions = "definitions"
	// methodForget removes a version from the registry
	methodForget = "forget"
)

// Alarm and job names
const (
	// alarmDeadline is the instance's single deadline alarm, replaceable by name so recomputing it is one write
	alarmDeadline = "deadline"
)

// startPayload begins an instance
type startPayload struct {
	Input   json.RawMessage `msgpack:"input,omitempty"`
	Version int             `msgpack:"version"`
	// Parent identifies the instance this one is a child of, and is nil for a top-level instance
	Parent      *parentRef `msgpack:"parent,omitempty"`
	TraceParent string     `msgpack:"traceParent,omitempty"`
	CreatedAt   time.Time  `msgpack:"createdAt,omitzero"`
	// Attempt is the parent task's attempt number, which the child echoes back so its report lands on the record that started it
	Attempt int `msgpack:"attempt,omitempty"`
}

// runPayload carries one attempt of a task to the worker that performs it
// It holds only what the step declared it needs, so the engine never ships the whole journal to a worker
type runPayload struct {
	InstanceID string `msgpack:"instanceId"`
	Workflow   string `msgpack:"workflow"`
	Version    int    `msgpack:"version"`
	Step       string `msgpack:"step"`
	// Index is the task's index in the journal, which is what every report is keyed by
	Index   int `msgpack:"index"`
	Attempt int `msgpack:"attempt"`
	// Positional reports whether Index is a real position among siblings, which it is for a group or a fan-out and is not for a plain or child step
	// A plain step's handler sees -1 from Task.Index, because it has no siblings to be positioned among
	Positional bool `msgpack:"positional,omitempty"`
	// Handler names the member of a parallel group whose handler runs, and is empty for every other kind
	Handler string          `msgpack:"handler,omitempty"`
	Input   json.RawMessage `msgpack:"input,omitempty"`
	Item    json.RawMessage `msgpack:"item,omitempty"`
	// Outputs holds the outputs this task may read: the preceding step's and those named with WithInputFrom
	Outputs map[string]json.RawMessage `msgpack:"outputs,omitempty"`
	// Skipped names the upstream steps that were skipped, so DecodeOutput can tell an absent output from an empty one
	Skipped []string `msgpack:"skipped,omitempty"`
	// Result is the output the forward task produced, and is only set for a compensation
	Result json.RawMessage `msgpack:"result,omitempty"`
	// Cause is the error that caused the workflow to unwind, and is only set for a compensation
	Cause string `msgpack:"cause,omitempty"`
	// OrchestratorType is the bare actor type the report is dispatched back to
	OrchestratorType string `msgpack:"orchestratorType"`
	// MaxOutputSize caps the encoded output, checked on the worker before it reports
	MaxOutputSize int    `msgpack:"maxOutputSize,omitempty"`
	TraceParent   string `msgpack:"traceParent,omitempty"`
}

// reportPayload carries one task's outcome back to the orchestrator
type reportPayload struct {
	Step    string `msgpack:"step"`
	Index   int    `msgpack:"index"`
	Attempt int    `msgpack:"attempt"`
	// Output is the handler's return value, JSON-encoded
	Output json.RawMessage `msgpack:"output,omitempty"`
	Error  string          `msgpack:"error,omitempty"`
	// Retryable reports whether another attempt could succeed, which an ordinary handler error says and a permanent failure does not
	Retryable bool `msgpack:"retryable,omitempty"`
	// Transport marks an attempt that failed because its report could not be delivered, rather than because the handler failed
	Transport bool `msgpack:"transport,omitempty"`
	// ChildStatus and ChildCompensation are set when a child instance is the one reporting
	ChildStatus       Status              `msgpack:"childStatus,omitempty"`
	ChildCompensation CompensationOutcome `msgpack:"childCompensation,omitempty"`
	TraceParent       string              `msgpack:"traceParent,omitempty"`
}

// compReportPayload carries one compensation's outcome back to the orchestrator
type compReportPayload struct {
	Step        string `msgpack:"step"`
	Index       int    `msgpack:"index"`
	Attempt     int    `msgpack:"attempt"`
	Error       string `msgpack:"error,omitempty"`
	Retryable   bool   `msgpack:"retryable,omitempty"`
	Transport   bool   `msgpack:"transport,omitempty"`
	TraceParent string `msgpack:"traceParent,omitempty"`
}

// eventPayload delivers an external event to a WaitForEvent step
type eventPayload struct {
	Name    string          `msgpack:"name"`
	Payload json.RawMessage `msgpack:"payload,omitempty"`
}

// reasonPayload carries the free-form reason of a cancel, an unwind, or a suspend
type reasonPayload struct {
	Reason string `msgpack:"reason,omitempty"`
	// FromParent marks a cancel or an unwind a parent sent, so the child reports its termination as a compensation rather than as a result
	FromParent bool `msgpack:"fromParent,omitempty"`
	// CompAttempt is the parent's compensation attempt number, which the child echoes back so its report lands on the record being unwound
	CompAttempt int `msgpack:"compAttempt,omitempty"`
}

// stepOutput returns what later steps see as a step's output, which depends on its kind (§5.3)
// A skipped step has no output at all, so DecodeOutput can report ErrStepSkipped
func stepOutput(sr *stepRecord, d *stepDef) json.RawMessage {
	if sr == nil || sr.Status == StepSkipped || sr.Status == StepPending {
		return nil
	}

	switch sr.Kind {
	case KindWait:
		return sr.Event

	case KindParallel:
		// A group's output is an object keyed by member name, so a later step reads one member without knowing its position
		obj := map[string]json.RawMessage{}
		for i := range sr.Tasks {
			name := memberName(d, sr.Tasks[i].Index)
			if name == "" {
				continue
			}
			obj[name] = taskOutput(&sr.Tasks[i])
		}
		enc, err := json.Marshal(obj)
		if err != nil {
			return nil
		}
		return enc

	case KindForEach:
		// A fan-out's output is an array ordered by item index, with the failed slots carrying their error so the next step can decide what to do about them
		arr := make([]json.RawMessage, len(sr.Tasks))
		for i := range sr.Tasks {
			arr[i] = taskOutput(&sr.Tasks[i])
		}
		enc, err := json.Marshal(arr)
		if err != nil {
			return nil
		}
		return enc

	default:
		if len(sr.Tasks) == 0 {
			return nil
		}
		return taskOutput(&sr.Tasks[0])
	}
}

// taskOutput returns one task's contribution to its step's output, standing a failure in for the value it never produced
func taskOutput(tr *taskRecord) json.RawMessage {
	if tr.Error != "" {
		enc, err := json.Marshal(map[string]string{"error": tr.Error})
		if err != nil {
			return json.RawMessage("null")
		}
		return enc
	}
	if len(tr.Output) == 0 {
		return json.RawMessage("null")
	}
	return tr.Output
}

// memberName returns the name of the member of a parallel group that ran the task at an index
func memberName(d *stepDef, index int) string {
	if d == nil || d.kind != KindParallel {
		return ""
	}
	if index < 0 || index >= len(d.members) {
		return ""
	}
	return d.members[index].name
}

// statusResult is the reply a status peek carries back from the orchestrator to the service
// Whether the instance exists travels as a result rather than an error, so the outcome reads the same whether the actor ran on this host or on a peer, where an error would arrive as an opaque protocol failure
type statusResult struct {
	Found  bool           `msgpack:"found"`
	Status InstanceStatus `msgpack:"status,omitempty"`
}

// purgeResult is the reply a purge carries back from the orchestrator to the service, for the same reason
type purgeResult struct {
	Found  bool `msgpack:"found"`
	Active bool `msgpack:"active,omitempty"`
}
