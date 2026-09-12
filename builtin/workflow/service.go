package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"uuid"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// The state labels written with every journal write, which is what makes a filtered listing a range scan rather than a walk of every retained journal
	labelStatus       = "status"
	labelVersion      = "version"
	labelParent       = "parent"
	labelTerminatedAt = "terminatedAt"

	// dayLayout is the granularity of the terminatedAt label, which the sweep only needs to the day
	dayLayout = "2006-01-02"
)

// Service binds the workflow to an actor.Service, returning a WorkflowService that drives instances against that service
// Obtain the service from a host with host.Service()
func (w *Workflow) Service(svc *actor.Service) *WorkflowService {
	return &WorkflowService{
		wf:  w,
		svc: svc,
	}
}

// WorkflowService exposes the operations of a workflow, bound to a specific actor.Service
// Obtain one from Workflow.Service
//
// Authorization is the calling application's responsibility, as it is for every other actor invocation in Francis: the engine offers no hook for it, deliberately
type WorkflowService struct {
	wf  *Workflow
	svc *actor.Service
}

// StartOption configures the start of an instance
type StartOption func(*startOptions)

type startOptions struct {
	instanceID string
}

// WithInstanceID starts the instance under a caller-chosen ID, which makes starting idempotent
// A second Start with the same ID finds the first, reports created as false, and discards the second call's input
func WithInstanceID(id string) StartOption {
	return func(o *startOptions) {
		o.instanceID = id
	}
}

// Start begins an instance and returns its ID
//
// It returns as soon as the start job is durable, so from that point on the work survives a restart of the process
// Without WithInstanceID the engine mints a UUIDv7, which sorts by creation time and therefore lists in creation order
//
// Start is idempotent only for suppressing a duplicate dispatch of the same request: an instance ID that has already terminated is not restarted, so re-driving a failed run means minting a fresh instance ID
// The returned created reports whether this call was the one that started the instance
func (s *WorkflowService) Start(ctx context.Context, input any, opts ...StartOption) (instanceID string, created bool, err error) {
	var so startOptions
	for _, opt := range opts {
		opt(&so)
	}

	instanceID = so.instanceID
	if instanceID == "" {
		instanceID = uuid.NewV7().String()
	} else {
		err = validateInstanceID(instanceID)
		if err != nil {
			return "", false, err
		}
	}

	// The input is capped here because it is shipped in every task's payload, so an oversized one would cost the whole run rather than one write
	encoded, err := encodeInput(input, s.wf.def.maxInputSize)
	if err != nil {
		return "", false, err
	}

	// An instance that already has a journal is not restarted, whatever its status: the repeated start is simply dropped
	client := builtinactor.NewClient[instanceState](s.wf.baseType, instanceID, s.svc)
	existing, err := client.GetState(ctx)
	if err != nil {
		return "", false, fmt.Errorf("failed to read the workflow journal: %w", err)
	}
	if existing.Status != "" {
		return instanceID, false, nil
	}

	payload := startPayload{
		Input:       encoded,
		Version:     s.wf.def.version,
		TraceParent: traceParentFromContext(ctx),
		CreatedAt:   time.Now(),
	}

	_, err = client.Dispatch(ctx, methodStart, payload, actor.WithIdempotencyKey(methodStart))
	if err != nil {
		return "", false, fmt.Errorf("failed to start the workflow instance: %w", err)
	}

	return instanceID, true, nil
}

// GetStatus returns the current status of an instance, without taking the Workflow actor's exclusive turn
// It returns ErrInstanceNotFound when the instance does not exist, or its journal has passed its retention
func (s *WorkflowService) GetStatus(ctx context.Context, instanceID string) (InstanceStatus, error) {
	env, err := retryWhileHalted(ctx, func(ctx context.Context) (actor.Envelope, error) {
		return builtinactor.Peek(ctx, s.svc, s.wf.baseType, instanceID, methodStatus, nil)
	})
	if err != nil {
		return InstanceStatus{}, err
	}

	var out statusResult
	err = env.Decode(&out)
	if err != nil {
		return InstanceStatus{}, fmt.Errorf("failed to decode the workflow status: %w", err)
	}
	if !out.Found {
		return InstanceStatus{}, ErrInstanceNotFound
	}
	return out.Status, nil
}

// RaiseEvent delivers an external event to a WaitForEvent step of an instance
// It returns ErrNoSuchEvent when the definition has no step listening for the name, and is accepted while the instance is suspended, in which case the step completes on resume
func (s *WorkflowService) RaiseEvent(ctx context.Context, instanceID string, name string, payload any) error {
	if !s.wf.hasEvent(name) {
		return fmt.Errorf("%w: %q", ErrNoSuchEvent, name)
	}

	encoded, err := encodeInput(payload, s.wf.def.maxOutputSize)
	if err != nil {
		return err
	}

	client := builtinactor.NewClient[struct{}](s.wf.baseType, instanceID, s.svc)
	_, err = client.Dispatch(ctx, methodEvent, eventPayload{Name: name, Payload: encoded},
		actor.WithIdempotencyKey(methodEvent+idDelimiter+name))
	if err != nil {
		return fmt.Errorf("failed to raise the event: %w", err)
	}
	return nil
}

// Cancel asks a running or suspended instance to stop and unwind, recording the reason as the cause every compensation receives
func (s *WorkflowService) Cancel(ctx context.Context, instanceID string, reason string) error {
	return s.dispatchControl(ctx, instanceID, methodCancel, reasonPayload{Reason: reason})
}

// Suspend pauses an instance without losing its place, and pauses its deadlines with it
// Work already dispatched runs to completion and its report is recorded; nothing new is started until Resume
func (s *WorkflowService) Suspend(ctx context.Context, instanceID string, reason string) error {
	return s.dispatchControl(ctx, instanceID, methodSuspend, reasonPayload{Reason: reason})
}

// Resume continues a suspended instance, re-arming its deadlines from the remainders the suspension recorded
func (s *WorkflowService) Resume(ctx context.Context, instanceID string) error {
	return s.dispatchControl(ctx, instanceID, methodResume, nil)
}

// dispatchControl sends one of the control jobs, each under a constant key so a repeated call coalesces with a pending one
func (s *WorkflowService) dispatchControl(ctx context.Context, instanceID string, method string, payload any) error {
	client := builtinactor.NewClient[struct{}](s.wf.baseType, instanceID, s.svc)
	_, err := client.Dispatch(ctx, method, payload, actor.WithIdempotencyKey(method))
	if err != nil {
		return fmt.Errorf("failed to %s the workflow instance: %w", method, err)
	}
	return nil
}

// Purge removes everything a terminated instance left behind: its children first, recursively, then its dead-letters, then its journal
// It refuses a running or suspended instance with ErrInstanceActive, and it is idempotent, so an interrupted purge is safe to repeat
func (s *WorkflowService) Purge(ctx context.Context, instanceID string) error {
	env, err := retryWhileHalted(ctx, func(ctx context.Context) (actor.Envelope, error) {
		return builtinactor.InvokeActor(ctx, s.svc, s.wf.baseType, instanceID, methodPurge, nil)
	})
	if err != nil {
		return err
	}

	var res purgeResult
	err = env.Decode(&res)
	if err != nil {
		return fmt.Errorf("failed to decode the purge result: %w", err)
	}
	switch {
	case !res.Found:
		return ErrInstanceNotFound
	case res.Active:
		return ErrInstanceActive
	default:
		return nil
	}
}

// PurgeTerminated removes every terminated instance whose retention has elapsed, skipping any that still has a parent, and returns how many it removed
// It pages, so a backlog of a million terminated instances is a long call rather than a large one
func (s *WorkflowService) PurgeTerminated(ctx context.Context) (int, error) {
	var (
		removed int
		cursor  string
	)

	for _, status := range []Status{StatusCompleted, StatusFailed, StatusCancelled} {
		cutoff := time.Now().Add(-s.wf.def.retention.forStatus(status))
		cursor = ""

		for {
			page, err := s.List(ctx, &ListOptions{Status: status, After: cursor, Limit: purgePageSize})
			if err != nil {
				return removed, err
			}

			for _, inst := range page.Instances {
				// A child is never purged from under a parent that might still unwind it, so the parent's own purge is what reaches it
				if inst.Parent != nil {
					continue
				}
				if inst.CompletedAt.IsZero() || inst.CompletedAt.After(cutoff) {
					continue
				}

				pErr := s.Purge(ctx, inst.InstanceID)
				if pErr != nil && !errors.Is(pErr, ErrInstanceNotFound) && !errors.Is(pErr, ErrInstanceActive) {
					return removed, fmt.Errorf("failed to purge instance %s: %w", inst.InstanceID, pErr)
				}
				if pErr == nil {
					removed++
				}
			}

			cursor = page.AfterID()
			if cursor == "" {
				break
			}
		}
	}

	return removed, nil
}

// purgePageSize is how many instances the sweep reads at a time, which bounds what one call holds in memory
const purgePageSize = 100

// ListOptions filters and pages a listing of instances
type ListOptions struct {
	// Status restricts the listing to instances in one status, and an empty value lists every status
	Status Status
	// Version restricts the listing to instances stamped with one version, which is how an operator watches a drain
	Version int
	// Parent restricts the listing to the children of one instance
	Parent string
	// After is the pagination cursor, an instance ID, and only instances sorting strictly after it are returned
	After string
	// Limit is the maximum number of instances to return
	Limit int
}

// InstanceList is a page of instances returned by List
type InstanceList struct {
	// Instances in this page, ordered by instance ID, which for the default UUIDv7 IDs is creation order
	Instances []InstanceStatus
	// HasMore is true when more instances exist after the last one in this page
	HasMore bool
}

// AfterID returns the cursor to set as ListOptions.After to retrieve the page following this one
// It is empty when this page is the last one, so a loop that pages until it gets an empty cursor visits every instance exactly once
func (l InstanceList) AfterID() string {
	if !l.HasMore || len(l.Instances) == 0 {
		return ""
	}
	return l.Instances[len(l.Instances)-1].InstanceID
}

// List returns a page of instances, filtered server-side on the state labels the orchestrator writes with every journal write
// A label filter is an equality on an indexed column, so "every running instance" is a range scan rather than a walk of every retained journal
func (s *WorkflowService) List(ctx context.Context, opts *ListOptions) (InstanceList, error) {
	var o ListOptions
	if opts != nil {
		o = *opts
	}

	labels := map[string]string{}
	if o.Status != "" {
		labels[labelStatus] = string(o.Status)
	}
	if o.Version > 0 {
		labels[labelVersion] = strconv.Itoa(o.Version)
	}
	if o.Parent != "" {
		labels[labelParent] = o.Parent
	}

	client := builtinactor.NewClient[instanceState](s.wf.baseType, "", s.svc)
	page, err := client.ListStates(ctx, &actor.ListStatesOpts{
		IncludeData: true,
		Labels:      labels,
		After:       o.After,
		Limit:       o.Limit,
	})
	if err != nil {
		return InstanceList{}, fmt.Errorf("failed to list workflow instances: %w", err)
	}

	res := InstanceList{
		Instances: make([]InstanceStatus, 0, len(page.States)),
		HasMore:   page.HasMore,
	}
	for i := range page.States {
		st := page.States[i].Data
		if st.Status == "" {
			continue
		}
		res.Instances = append(res.Instances, statusView(page.States[i].ActorID, &st, s.wf.def))
	}
	return res, nil
}

// Definitions returns what the registry holds: which versions exist, their fingerprints, and whether this host's own definition disagrees with any of them
func (s *WorkflowService) Definitions(ctx context.Context) ([]DefinitionInfo, error) {
	env, err := builtinactor.Invoke(ctx, s.svc, s.wf.registryType(), methodDefinitions, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read the workflow definition registry: %w", err)
	}

	var resp definitionsResponse
	err = env.Decode(&resp)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the registry response: %w", err)
	}

	out := make([]DefinitionInfo, 0, len(resp.Entries))
	for version, entry := range resp.Entries {
		out = append(out, DefinitionInfo{
			Version:     version,
			Fingerprint: entry.Fingerprint,
			FirstSeenAt: entry.FirstSeenAt,
			Conflicts:   version == s.wf.def.version && entry.Fingerprint != s.wf.def.fingerprint,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// ForgetVersion removes a version from the registry, which is the operator's reset for one that was registered wrongly
// It refuses a version that still has instances with ErrVersionInUse, since forgetting one under a running instance would let a different graph claim its number
func (s *WorkflowService) ForgetVersion(ctx context.Context, version int) error {
	page, err := s.List(ctx, &ListOptions{Version: version, Limit: 1})
	if err != nil {
		return err
	}
	if len(page.Instances) > 0 {
		return fmt.Errorf("%w: version %d", ErrVersionInUse, version)
	}

	_, err = builtinactor.Invoke(ctx, s.svc, s.wf.registryType(), methodForget, forgetRequest{Version: version})
	if err != nil {
		return fmt.Errorf("failed to forget the definition: %w", err)
	}
	return nil
}

// InstanceStatus is the caller-facing view of an instance's journal
type InstanceStatus struct {
	InstanceID   string
	Workflow     string
	Version      int
	Status       Status
	Compensation CompensationOutcome
	// CurrentStep names the step the instance is on, and is empty when it has terminated
	CurrentStep string
	// Steps is every step of the definition, in declaration order
	Steps []StepStatusView
	// Cause is what triggered an unwind, or why the instance failed
	Cause string
	// Suspended is set while the instance is paused
	Suspended *SuspendView
	// Parent is set when the instance is a child of another
	Parent      *ParentView
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}

// StepStatusView is one step of an instance, as a caller sees it
type StepStatusView struct {
	Name   string
	Kind   Kind
	Status StepStatus
	// Tasks counts the execution units the step materialized
	Tasks int
	// Completed and Failed count the tasks that reported each outcome
	Completed int
	Failed    int
	// Attempts is the total number of attempts made across the step's tasks
	Attempts int
	// Error is the reason the step failed, once it has
	Error string
	// ChildIDs are the instance IDs of the children a child step or a child fan-out started
	ChildIDs    []string
	StartedAt   time.Time
	CompletedAt time.Time
}

// SuspendView says since when an instance has been paused, why, and what it goes back to
type SuspendView struct {
	At       time.Time
	Reason   string
	ResumeTo Status
}

// ParentView identifies the instance a child belongs to
type ParentView struct {
	InstanceID string
	Workflow   string
	Step       string
	Index      int
	Depth      int
}

// statusView builds the caller-facing view of a journal
// A caller never sees completed before every step has reported, including the optional ones, because the view is derived from the same records advance settles
func statusView(instanceID string, st *instanceState, def *definition) InstanceStatus {
	out := InstanceStatus{
		InstanceID:   instanceID,
		Workflow:     st.Workflow,
		Version:      st.Version,
		Status:       st.Status,
		Compensation: st.Compensation,
		CurrentStep:  st.Cursor,
		Cause:        st.Cause,
		CreatedAt:    st.CreatedAt,
		StartedAt:    st.StartedAt,
		CompletedAt:  st.CompletedAt,
		Steps:        make([]StepStatusView, len(st.Steps)),
	}
	if out.Workflow == "" {
		out.Workflow = def.name
	}

	if st.Suspended != nil {
		out.Suspended = &SuspendView{
			At:       st.Suspended.At,
			Reason:   st.Suspended.Reason,
			ResumeTo: st.Suspended.ResumeTo,
		}
	}
	if st.Parent != nil {
		out.Parent = &ParentView{
			InstanceID: st.Parent.InstanceID,
			Workflow:   st.Parent.Workflow,
			Step:       st.Parent.Step,
			Index:      st.Parent.Index,
			Depth:      st.Parent.Depth,
		}
	}

	for i := range st.Steps {
		out.Steps[i] = stepStatusView(&st.Steps[i])
	}
	return out
}

// stepStatusView summarizes one step's records for a caller
func stepStatusView(sr *stepRecord) StepStatusView {
	view := StepStatusView{
		Name:        sr.Name,
		Kind:        sr.Kind,
		Status:      sr.Status,
		Tasks:       len(sr.Tasks),
		Error:       sr.Error,
		StartedAt:   sr.StartedAt,
		CompletedAt: sr.CompletedAt,
	}

	for i := range sr.Tasks {
		tr := &sr.Tasks[i]
		view.Attempts += tr.Attempts
		if tr.Comp != nil {
			view.Attempts += tr.Comp.Attempts
		}
		switch {
		case tr.Done && tr.Error != "":
			view.Failed++
		case tr.Done:
			view.Completed++
		}
		if tr.ChildID != "" {
			view.ChildIDs = append(view.ChildIDs, tr.ChildID)
		}
	}
	return view
}

// hasEvent reports whether the definition has a WaitForEvent step listening for a name, so RaiseEvent can refuse a name nothing waits for rather than dispatching a job nothing reads
func (w *Workflow) hasEvent(name string) bool {
	for _, d := range w.def.steps {
		if d.kind == KindWait && d.effectiveEventName() == name {
			return true
		}
	}
	return false
}

// encodeInput JSON-encodes a caller's value and enforces a size cap on the result
func encodeInput(v any, limit int) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}

	enc, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the workflow input: %w", err)
	}
	if limit > 0 && len(enc) > limit {
		return nil, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrInputTooLarge, len(enc), limit)
	}
	return enc, nil
}

// validateInstanceID rejects an ID that would make a worker's actor ID or a state key ambiguous
func validateInstanceID(id string) error {
	err := ref.ValidateComponents(id)
	if err != nil {
		return fmt.Errorf("invalid instance ID: %w", err)
	}
	if strings.Contains(id, idDelimiter) {
		return fmt.Errorf("invalid instance ID: must not contain %q", idDelimiter)
	}
	return nil
}

// traceParentFromContext renders the caller's trace context in its W3C form, so every span of the instance can link back to the request that started it
// A Francis job does not carry its dispatcher's trace context, so the engine carries it in the payloads it controls
func traceParentFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

// haltedRetryDelay and haltedRetryAttempts bound how long a call waits out an actor that is halting
// An actor halts when it terminates, when it is purged, and when the cluster rebalances, so a caller reaching it right then gets a transient condition rather than an answer
const (
	haltedRetryDelay    = 50 * time.Millisecond
	haltedRetryAttempts = 20
)

// retryWhileHalted runs fn, re-resolving the actor's placement for as long as it reports the actor is halting
func retryWhileHalted(ctx context.Context, fn func(ctx context.Context) (actor.Envelope, error)) (actor.Envelope, error) {
	var (
		env actor.Envelope
		err error
	)

	for range haltedRetryAttempts {
		env, err = fn(ctx)
		if !errors.Is(err, actor.ErrActorHalted) {
			return env, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(haltedRetryDelay):
		}
	}

	return env, err
}
