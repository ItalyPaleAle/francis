package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// ActorTypePrefix is the full actor type prefix of every workflow actor type
const ActorTypePrefix = ref.BuiltInActorTypePrefix + workflowActorTypePrefix

// StartJobName is the alarm name (idempotency key) of the start job
const StartJobName = methodStart

// purgeCronTypePrefix and purgeCronTypeSuffix bracket the full actor type of a workflow's auto-purge cron job, which lives in the cron job type space
const (
	purgeCronTypePrefix = ref.BuiltInActorTypePrefix + "cronjob."
	purgeCronTypeSuffix = ".purge"
)

// Time sources of history events, which say how the time of an event was obtained
const (
	// TimeSourceEngine is the time of the orchestrator turn that recorded the event
	TimeSourceEngine = "engine"
	// TimeSourceDispatch is the time the orchestrator's dispatch of a job was accepted
	TimeSourceDispatch = "dispatch"
	// TimeSourceReport is the time of the orchestrator turn that processed a task report
	TimeSourceReport = "report"
	// TimeSourceWorker is the worker's own clock, supplied with the task report
	TimeSourceWorker = "worker"
)

// Kinds of history events
const (
	// EventKindInstanceStarted is the first event of every instance, written with its first journal
	EventKindInstanceStarted = "instance_started"
	// EventKindInstanceSuspended records a suspension, with its reason
	EventKindInstanceSuspended = "instance_suspended"
	// EventKindInstanceResumed records the end of a suspension
	EventKindInstanceResumed = "instance_resumed"
	// EventKindInstanceReopened records a terminated instance becoming active again, because its parent unwound it or an abandoned task reported a late success
	EventKindInstanceReopened = "instance_reopened"
	// EventKindInstanceCompleted, EventKindInstanceFailed and EventKindInstanceCancelled record the terminal status, with the compensation outcome and the cause
	EventKindInstanceCompleted = "instance_completed"
	EventKindInstanceFailed    = "instance_failed"
	EventKindInstanceCancelled = "instance_cancelled"
	// EventKindCancelRequested records an accepted cancel request, with its reason
	EventKindCancelRequested = "cancel_requested"
	// EventKindUnwindRequested records an accepted request from the parent to unwind, with its reason
	EventKindUnwindRequested = "unwind_requested"
	// EventKindCompensationStarted records the unwind opening, with its cause as the reason and the status it will terminate into as the outcome
	EventKindCompensationStarted = "compensation_started"
	// EventKindParentNotified records that the terminal outcome was reported to the parent instance
	EventKindParentNotified = "parent_notified"

	// EventKindStepStarted records a step (or a loop iteration of it) opening
	EventKindStepStarted = "step_started"
	// EventKindStepCompleted, EventKindStepFailed and EventKindStepSkipped record a step settling going forward
	EventKindStepCompleted = "step_completed"
	EventKindStepFailed    = "step_failed"
	EventKindStepSkipped   = "step_skipped"
	// EventKindStepCompensating, EventKindStepCompensated and EventKindStepCompensationFailed record a step's frame being unwound
	EventKindStepCompensating       = "step_compensating"
	EventKindStepCompensated        = "step_compensated"
	EventKindStepCompensationFailed = "step_compensation_failed"
	// EventKindWaitStarted records a wait step opening, with the name of the event it waits for
	EventKindWaitStarted = "wait_started"
	// EventKindEventReceived records a wait step receiving its event
	EventKindEventReceived = "event_received"

	// EventKindTaskDispatched records the job of one task attempt being accepted
	EventKindTaskDispatched = "task_dispatched"
	// EventKindChildStarted records the start job of a child instance being accepted
	EventKindChildStarted = "child_started"
	// EventKindWorkerStarted and EventKindWorkerFinished bracket a handler run, timed by the worker
	EventKindWorkerStarted  = "worker_started"
	EventKindWorkerFinished = "worker_finished"
	// EventKindTaskRetryScheduled records a failed attempt being followed by another, with the error and the time it may run
	EventKindTaskRetryScheduled = "task_retry_scheduled"
	// EventKindTaskCompleted, EventKindTaskFailed and EventKindTaskAbandoned record a task's forward outcome, and for a child task the outcome is the child's status
	EventKindTaskCompleted = "task_completed"
	EventKindTaskFailed    = "task_failed"
	EventKindTaskAbandoned = "task_abandoned"
	// EventKindCompensationDispatched records the job of one compensation attempt being accepted
	EventKindCompensationDispatched = "compensation_dispatched"
	// EventKindChildUnwindRequested records the request asking a child instance to undo itself being accepted
	EventKindChildUnwindRequested = "child_unwind_requested"
	// EventKindCompensationRetryScheduled records a failed compensation attempt being followed by another
	EventKindCompensationRetryScheduled = "compensation_retry_scheduled"
	// EventKindCompensationCompleted and EventKindCompensationFailed record a compensation's outcome, and for a child task the outcome is the child's compensation
	EventKindCompensationCompleted = "compensation_completed"
	EventKindCompensationFailed    = "compensation_failed"
)

// OrchestratorActorType returns the full actor type of a workflow's orchestrator, which is the type its journals are stored under
func OrchestratorActorType(name string) string {
	return ActorTypePrefix + name
}

// RegistryActorType returns the full actor type of a workflow's definition registry
func RegistryActorType(name string) string {
	return ActorTypePrefix + name + registryTypeSuffix
}

// ActorRole is the part a workflow-owned actor type plays
type ActorRole string

const (
	RoleOrchestrator ActorRole = "orchestrator"
	RoleWorker       ActorRole = "worker"
	RoleUndo         ActorRole = "undo"
	RoleRegistry     ActorRole = "registry"
	// RoleOther is any other workflow-owned type, such as the purge cron job
	RoleOther ActorRole = "other"
)

// ParseActorType parses a full actor type; ok is false when it is not a workflow actor type
// The auto-purge cron job lives in the cron job type space, as "francis.builtin.cronjob.<name>.purge", and is reported with RoleOther
func ParseActorType(actorType string) (name string, role ActorRole, capability string, ok bool) {
	// The auto-purge cron job of a workflow
	if strings.HasPrefix(actorType, purgeCronTypePrefix) && strings.HasSuffix(actorType, purgeCronTypeSuffix) {
		name = strings.TrimSuffix(strings.TrimPrefix(actorType, purgeCronTypePrefix), purgeCronTypeSuffix)
		if validateTypeComponent(name) != nil {
			return "", "", "", false
		}
		return name, RoleOther, "", true
	}

	rest, found := strings.CutPrefix(actorType, ActorTypePrefix)
	if !found {
		return "", "", "", false
	}

	// Workflow names and capabilities never contain a dot, so the parts are unambiguous
	parts := strings.Split(rest, ".")
	name = parts[0]
	if validateTypeComponent(name) != nil {
		return "", "", "", false
	}
	switch {
	case len(parts) == 1:
		return name, RoleOrchestrator, "", true
	case "."+parts[1] == workerTypeSuffix && len(parts) <= 3:
		if len(parts) == 3 {
			capability = parts[2]
		}
		return name, RoleWorker, capability, true
	case "."+parts[1] == undoTypeSuffix && len(parts) <= 3:
		if len(parts) == 3 {
			capability = parts[2]
		}
		return name, RoleUndo, capability, true
	case "."+parts[1] == registryTypeSuffix && len(parts) == 2:
		return name, RoleRegistry, "", true
	default:
		return name, RoleOther, "", true
	}
}

// ControlAction is a control operation on a workflow instance
type ControlAction string

const (
	ControlCancel  ControlAction = "cancel"
	ControlSuspend ControlAction = "suspend"
	ControlResume  ControlAction = "resume"
)

// controlPayload returns the job method and payload WorkflowService dispatches for a control action
// Resume carries no payload, so its reason is ignored
func controlPayload(action ControlAction, reason string) (method string, payload any, err error) {
	switch action {
	case ControlCancel:
		return methodCancel, reasonPayload{Reason: reason}, nil
	case ControlSuspend:
		return methodSuspend, reasonPayload{Reason: reason}, nil
	case ControlResume:
		return methodResume, nil, nil
	default:
		return "", nil, fmt.Errorf("unknown workflow control action %q", action)
	}
}

// ControlJob returns exactly what WorkflowService.Cancel/Suspend/Resume dispatch: the job method, the idempotency key (alarm name), and the MessagePack-encoded job data as the actor client encodes it
// Resume ignores the reason, and its data is nil
func ControlJob(action ControlAction, reason string) (method string, key string, data []byte, err error) {
	method, payload, err := controlPayload(action, reason)
	if err != nil {
		return "", "", nil, err
	}

	// The actor client encodes nothing for a nil payload
	if payload != nil {
		data, err = msgpack.Marshal(payload)
		if err != nil {
			return "", "", nil, fmt.Errorf("failed to encode the control job data: %w", err)
		}
	}
	return method, method, data, nil
}

// InstanceView is a definition-independent description of a stored journal or pending placeholder
type InstanceView struct {
	Workflow              string
	Version               int
	DefinitionFingerprint string
	// Status is StatusPending for a placeholder
	Status Status
	// Pending is true for a placeholder, which means there is no journal yet
	Pending        bool
	Compensation   CompensationOutcome
	Cause          string
	TerminalStatus Status
	Input          json.RawMessage
	Output         json.RawMessage
	// HasOutput is len(Output) > 0 in the stored journal, so a stored JSON null counts
	HasOutput   bool
	Parent      *ParentView
	Suspended   *SuspendView
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Steps       []StepView
	// EventHistory is false when the workflow opted out with WithoutEventHistory
	EventHistory bool
	// LastEventSeq is the sequence number of the last history event written with the journal, and zero when there is none
	LastEventSeq int64
}

// StepView describes one step of a journal
type StepView struct {
	Name, Kind, Status     string
	Iteration              int
	StartedAt, CompletedAt time.Time
	Error                  string
	TaskCount              int
	TasksRemaining         int
	// Children are the child workflow instances started by this step
	Children []ChildLink
}

// ChildLink identifies a child workflow instance
type ChildLink struct {
	Workflow   string
	InstanceID string
	// ActorType is the full actor type of the child's orchestrator
	ActorType string
}

// StartView describes the data of a pending start job
type StartView struct {
	Input                 json.RawMessage
	Version               int
	DefinitionFingerprint string
	CreatedAt             time.Time
	Parent                *ParentView
}

// RegistryView describes a workflow's definition registry
type RegistryView struct {
	Versions []RegistryVersionView
}

// RegistryVersionView is one version recorded by the registry
type RegistryVersionView struct {
	Version     int
	Fingerprint string
	FirstSeenAt time.Time
	Generation  uint64
}

// EventView is one decoded history event
type EventView struct {
	Seq  int64
	Time time.Time
	// TimeSource is one of the TimeSource constants: "engine" (orchestrator turn time), "dispatch", "report", or "worker" (worker-supplied)
	TimeSource string
	// Kind is one of the EventKind constants
	Kind      string
	Step      string
	TaskIndex *int
	Attempt   int
	Outcome   string
	Error     string
	Child     *ChildLink
	// Iteration is the loop iteration of the step, and zero outside a loop body
	Iteration int
	// Undo marks an event about a compensation rather than the forward task
	Undo bool
	// Reason is the free-form reason of a cancel, unwind or suspend, or the cause of an unwind
	Reason string
	// EventName is the event a wait step waits for or received
	EventName string
	// DueTime is the earliest time a scheduled attempt may run, when it is later than the event
	DueTime time.Time
}

// DecodeInstance decodes the stored state of a workflow orchestrator, which is either a journal or a pending placeholder
// It needs no definition, so it can describe instances of versions no host serves
func DecodeInstance(data []byte) (*InstanceView, error) {
	if len(data) == 0 {
		return nil, errors.New("workflow state is empty")
	}

	var st instanceStateWire
	err := msgpack.Unmarshal(data, &st)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the workflow state: %w", err)
	}

	// A placeholder only describes the start that is waiting to run
	if st.Status == "" {
		if st.PendingStart == nil {
			return nil, errors.New("workflow state is neither a journal nor a pending placeholder")
		}
		return &InstanceView{
			Workflow:     st.PendingStart.Workflow,
			Version:      st.PendingStart.Version,
			Status:       StatusPending,
			Pending:      true,
			Parent:       parentView(st.PendingStart.Parent),
			CreatedAt:    st.PendingStart.CreatedAt,
			EventHistory: !st.PendingStart.NoEventHistory,
		}, nil
	}

	view := &InstanceView{
		Workflow:              st.Workflow,
		Version:               st.Version,
		DefinitionFingerprint: st.DefinitionFingerprint,
		Status:                st.Status,
		Compensation:          st.Compensation,
		Cause:                 st.Cause,
		TerminalStatus:        st.TerminalStatus,
		Input:                 st.Input,
		Output:                st.Output,
		HasOutput:             len(st.Output) > 0,
		Parent:                parentView(st.Parent),
		CreatedAt:             st.CreatedAt,
		StartedAt:             st.StartedAt,
		CompletedAt:           st.CompletedAt,
		EventHistory:          !st.NoEventHistory,
		LastEventSeq:          st.LastEventSeq,
		Steps:                 make([]StepView, len(st.Steps)),
	}
	if st.Suspended != nil {
		view.Suspended = &SuspendView{
			Reason:   st.Suspended.Reason,
			At:       st.Suspended.At,
			ResumeTo: st.Suspended.ResumeTo,
		}
	}
	for i := range st.Steps {
		sr := &st.Steps[i]
		sv := StepView{
			Name:           sr.Name,
			Kind:           string(sr.Kind),
			Status:         string(sr.Status),
			Iteration:      sr.Iteration,
			StartedAt:      sr.StartedAt,
			CompletedAt:    sr.CompletedAt,
			Error:          sr.Error,
			TaskCount:      len(sr.Tasks),
			TasksRemaining: sr.Remaining,
		}
		for j := range sr.Tasks {
			link := childLink(&sr.Tasks[j])
			if link != nil {
				sv.Children = append(sv.Children, ChildLink(*link))
			}
		}
		view.Steps[i] = sv
	}
	return view, nil
}

// DecodeStartPayload decodes the data of the "start" job of a workflow instance
func DecodeStartPayload(data []byte) (*StartView, error) {
	var p startPayload
	err := decodeManagementData(data, &p)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the workflow start payload: %w", err)
	}

	return &StartView{
		Input:                 p.Input,
		Version:               p.Version,
		DefinitionFingerprint: p.DefinitionFingerprint,
		CreatedAt:             p.CreatedAt,
		Parent:                parentView(p.Parent),
	}, nil
}

// DecodeRegistry decodes the stored state of a workflow's definition registry
func DecodeRegistry(data []byte) (*RegistryView, error) {
	var st registryState
	err := decodeManagementData(data, &st)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the workflow registry: %w", err)
	}

	view := &RegistryView{
		Versions: make([]RegistryVersionView, len(st.Versions)),
	}
	for i, e := range st.Versions {
		view.Versions[i] = RegistryVersionView(e)
	}
	return view, nil
}

// DecodeEvent decodes one stored history event
func DecodeEvent(ev components.WorkflowEvent) (EventView, error) {
	var data eventData
	err := decodeManagementData(ev.Data, &data)
	if err != nil {
		return EventView{}, fmt.Errorf("failed to decode workflow event %d: %w", ev.Seq, err)
	}

	view := EventView{
		Seq:        ev.Seq,
		Time:       ev.Time,
		TimeSource: data.TimeSource,
		Kind:       ev.Kind,
		Step:       data.Step,
		TaskIndex:  data.TaskIndex,
		Attempt:    data.Attempt,
		Outcome:    data.Outcome,
		Error:      data.Error,
		Iteration:  data.Iteration,
		Undo:       data.Undo,
		Reason:     data.Reason,
		EventName:  data.EventName,
		DueTime:    data.DueTime,
	}
	if data.Child != nil {
		link := ChildLink(*data.Child)
		view.Child = &link
	}
	return view, nil
}

// ManagementDefinition returns the identity of this workflow's definition, which hosts report so management can list the definitions each one serves
func (w *Workflow) ManagementDefinition() (name string, version int, fingerprint string) {
	return w.name, w.def.version, w.def.fingerprint
}

// decodeManagementData decodes stored MessagePack, treating absent data as the zero value
func decodeManagementData(data []byte, into any) error {
	if len(data) == 0 {
		return nil
	}
	return msgpack.Unmarshal(data, into)
}

// parentView describes a parent reference, or returns nil when there is none
func parentView(p *parentRef) *ParentView {
	if p == nil {
		return nil
	}
	return &ParentView{
		InstanceID: p.InstanceID,
		Workflow:   p.Workflow,
		Step:       p.Step,
		Index:      p.Index,
		Depth:      p.Depth,
	}
}
