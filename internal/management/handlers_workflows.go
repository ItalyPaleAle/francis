package management

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// workflowLinkJSON links an actor of a workflow to its workflow instance
type workflowLinkJSON struct {
	Workflow   string `json:"workflow"`
	Role       string `json:"role"`
	Capability string `json:"capability,omitempty"`
	InstanceID string `json:"instanceId,omitempty"`
	Step       string `json:"step,omitempty"`
	TaskIndex  *int   `json:"taskIndex,omitempty"`
}

// newWorkflowLink derives the workflow instance an actor belongs to from the naming conventions, returning nil for an actor that is not a workflow actor
// Worker and undo actor IDs are "<instance>|<step>|<index>", where the instance ID may itself contain "|"
func newWorkflowLink(actorType string, actorID string) *workflowLinkJSON {
	name, role, capability, ok := workflow.ParseActorType(actorType)
	if !ok {
		return nil
	}

	res := &workflowLinkJSON{
		Workflow:   name,
		Role:       string(role),
		Capability: capability,
	}
	switch role {
	case workflow.RoleOrchestrator:
		res.InstanceID = actorID
	case workflow.RoleWorker, workflow.RoleUndo:
		rest, idxStr, ok := cutLast(actorID, "|")
		if !ok {
			return res
		}
		instance, step, ok := cutLast(rest, "|")
		if !ok {
			return res
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return res
		}
		res.InstanceID = instance
		res.Step = step
		res.TaskIndex = &idx
	}

	return res
}

// cutLast slices s around the last instance of sep
func cutLast(s string, sep string) (before string, after string, found bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}

// workflowNameFromPath reads the workflow name path segment
func workflowNameFromPath(r *http.Request) (string, *apiError) {
	name := r.PathValue("name")
	if ref.ValidateComponents(name) != nil || strings.Contains(name, ".") {
		return "", errBadRequest("workflow name must not be empty and must not contain '/' or '.'")
	}
	return name, nil
}

// instanceRefFromPath reads the workflow name and instance ID path segments, returning the orchestrator actor
func instanceRefFromPath(r *http.Request) (string, ref.ActorRef, *apiError) {
	name, apiErr := workflowNameFromPath(r)
	if apiErr != nil {
		return "", ref.ActorRef{}, apiErr
	}
	instanceID := r.PathValue("instanceId")
	if ref.ValidateComponents(instanceID) != nil {
		return "", ref.ActorRef{}, errBadRequest("instance ID must not be empty and must not contain '/'")
	}
	return name, ref.NewActorRef(workflow.OrchestratorActorType(name), instanceID), nil
}

type workflowVersionJSON struct {
	Version     int       `json:"version"`
	Fingerprint string    `json:"fingerprint"`
	FirstSeenAt time.Time `json:"firstSeenAt"`
	Generation  uint64    `json:"generation"`
}

type workflowHostJSON struct {
	HostID   string `json:"hostId"`
	Draining bool   `json:"draining"`
	// Definitions are the versions the host serves, nil when the host could not be queried
	Definitions []workflowHostDefinitionJSON `json:"definitions"`
}

type workflowHostDefinitionJSON struct {
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

// workflowConflictJSON reports a version registered or served with more than one definition fingerprint
type workflowConflictJSON struct {
	Version      int      `json:"version"`
	Fingerprints []string `json:"fingerprints"`
	Hosts        []string `json:"hosts"`
}

type workflowJSON struct {
	Name      string                 `json:"name"`
	Versions  []workflowVersionJSON  `json:"versions"`
	Hosts     []workflowHostJSON     `json:"hosts"`
	Conflicts []workflowConflictJSON `json:"conflicts"`
}

type workflowsJSON struct {
	Items      []workflowJSON  `json:"items"`
	Partial    bool            `json:"partial"`
	Errors     []hostErrorJSON `json:"errors"`
	ObservedAt time.Time       `json:"observedAt"`
}

// handleListWorkflows serves GET /api/v1/workflows
// Workflows are known from their registry state, which exists once an instance started, and from the orchestrator types live hosts serve
func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) *apiError {
	ctx := r.Context()
	provider := s.backend.Provider()

	workflows := map[string]*workflowJSON{}
	get := func(name string) *workflowJSON {
		wf := workflows[name]
		if wf == nil {
			wf = &workflowJSON{
				Name:      name,
				Versions:  []workflowVersionJSON{},
				Hosts:     []workflowHostJSON{},
				Conflicts: []workflowConflictJSON{},
			}
			workflows[name] = wf
		}
		return wf
	}

	// Read the versions of every workflow that has registry state
	types, err := provider.ListStateActorTypes(ctx, workflow.ActorTypePrefix)
	if err != nil {
		return s.fail(r, "failed to list workflow types", err)
	}
	for _, t := range types {
		name, role, _, ok := workflow.ParseActorType(t)
		if !ok || (role != workflow.RoleRegistry && role != workflow.RoleOrchestrator) {
			continue
		}
		wf := get(name)
		if role != workflow.RoleRegistry {
			continue
		}

		data, err := provider.GetState(ctx, ref.NewActorRef(t, actor.SingletonActorID))
		if errors.Is(err, components.ErrNoState) {
			continue
		} else if err != nil {
			return s.fail(r, "failed to read the workflow registry", err)
		}
		reg, err := workflow.DecodeRegistry(data)
		if err != nil {
			return s.fail(r, "failed to decode the workflow registry", err)
		}
		for _, v := range reg.Versions {
			wf.Versions = append(wf.Versions, workflowVersionJSON{
				Version:     v.Version,
				Fingerprint: v.Fingerprint,
				FirstSeenAt: v.FirstSeenAt.UTC(),
				Generation:  v.Generation,
			})
		}
	}

	// Add the workflows live hosts serve, and ask those hosts for their definitions
	hosts, err := s.listAllHosts(ctx)
	if err != nil {
		return s.fail(r, "failed to list hosts", err)
	}
	serving := make([]components.HostDetails, 0, len(hosts))
	for _, h := range hosts {
		found := false
		for _, t := range h.ActorTypes {
			name, role, _, ok := workflow.ParseActorType(t.ActorType)
			if ok && role == workflow.RoleOrchestrator {
				get(name)
				found = true
			}
		}
		if found {
			serving = append(serving, h)
		}
	}

	res := workflowsJSON{
		Items:  make([]workflowJSON, 0, len(workflows)),
		Errors: []hostErrorJSON{},
	}
	for _, sr := range s.snapshotHosts(ctx, serving, protocol.HostSnapshotRequest{SkipActivations: true}) {
		if sr.Err != nil {
			res.Partial = true
			res.Errors = append(res.Errors, newHostError(sr.Host.HostID, sr.Err))
		}

		// Attach the host to every workflow whose orchestrator it serves
		defs := map[string][]workflowHostDefinitionJSON{}
		for _, d := range sr.Snapshot.Workflows {
			defs[d.Name] = append(defs[d.Name], workflowHostDefinitionJSON{Version: d.Version, Fingerprint: d.Fingerprint})
		}
		for _, t := range sr.Host.ActorTypes {
			name, role, _, ok := workflow.ParseActorType(t.ActorType)
			if !ok || role != workflow.RoleOrchestrator {
				continue
			}
			entry := workflowHostJSON{
				HostID:   sr.Host.HostID,
				Draining: sr.Host.Draining,
			}
			if sr.Err == nil {
				entry.Definitions = defs[name]
				if entry.Definitions == nil {
					entry.Definitions = []workflowHostDefinitionJSON{}
				}
			}
			wf := get(name)
			wf.Hosts = append(wf.Hosts, entry)
		}
	}

	for _, wf := range workflows {
		wf.Conflicts = findConflicts(wf)
		res.Items = append(res.Items, *wf)
	}
	slices.SortFunc(res.Items, func(a, b workflowJSON) int {
		return strings.Compare(a.Name, b.Name)
	})
	res.ObservedAt = s.clock.Now().UTC()

	writeJSON(w, http.StatusOK, res)
	return nil
}

// findConflicts reports the versions whose registry entry and host definitions disagree on the fingerprint
func findConflicts(wf *workflowJSON) []workflowConflictJSON {
	type versionInfo struct {
		fingerprints []string
		hosts        []string
	}
	versions := map[int]*versionInfo{}
	add := func(version int, fingerprint string, hostID string) {
		vi := versions[version]
		if vi == nil {
			vi = &versionInfo{}
			versions[version] = vi
		}
		if !slices.Contains(vi.fingerprints, fingerprint) {
			vi.fingerprints = append(vi.fingerprints, fingerprint)
		}
		if hostID != "" && !slices.Contains(vi.hosts, hostID) {
			vi.hosts = append(vi.hosts, hostID)
		}
	}
	for _, v := range wf.Versions {
		add(v.Version, v.Fingerprint, "")
	}
	for _, h := range wf.Hosts {
		for _, d := range h.Definitions {
			add(d.Version, d.Fingerprint, h.HostID)
		}
	}

	res := []workflowConflictJSON{}
	for version, vi := range versions {
		if len(vi.fingerprints) < 2 {
			continue
		}
		slices.Sort(vi.fingerprints)
		slices.Sort(vi.hosts)
		if vi.hosts == nil {
			vi.hosts = []string{}
		}
		res = append(res, workflowConflictJSON{Version: version, Fingerprints: vi.fingerprints, Hosts: vi.hosts})
	}
	slices.SortFunc(res, func(a, b workflowConflictJSON) int {
		return a.Version - b.Version
	})
	return res
}

type instanceItemJSON struct {
	InstanceID string     `json:"instanceId"`
	Status     string     `json:"status"`
	Version    int        `json:"version,omitempty"`
	Parent     string     `json:"parent,omitempty"`
	CreatedAt  *time.Time `json:"createdAt,omitempty"`
}

type instancesCursor struct {
	After string `json:"a"`
}

// validInstanceStatuses lists the statuses an instance listing can filter on
var validInstanceStatuses = []workflow.Status{
	workflow.StatusPending,
	workflow.StatusRunning,
	workflow.StatusSuspended,
	workflow.StatusCompensating,
	workflow.StatusCompleted,
	workflow.StatusFailed,
	workflow.StatusCancelled,
}

// handleListInstances serves GET /api/v1/workflows/{name}/instances
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) *apiError {
	name, apiErr := workflowNameFromPath(r)
	if apiErr != nil {
		return apiErr
	}
	var cursor instancesCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	// Parse the filters, which are matched against the workflow labels
	q := r.URL.Query()
	labels := &components.WorkflowLabels{
		Status: q.Get("status"),
		Parent: q.Get("parent"),
	}
	if labels.Status != "" && !slices.Contains(validInstanceStatuses, workflow.Status(labels.Status)) {
		return errBadRequest("status must be one of: pending, running, suspended, compensating, completed, failed, cancelled")
	}
	versionStr := q.Get("version")
	if versionStr != "" {
		v, err := strconv.Atoi(versionStr)
		if err != nil || v < 1 {
			return errBadRequest("version must be a positive integer")
		}
		labels.Version = v
	}
	if labels.IsZero() {
		// Without a label filter, rows that have no labels are listed too
		labels = nil
	}
	createdFrom, apiErr := timeParam(q.Get("createdFrom"), "createdFrom")
	if apiErr != nil {
		return apiErr
	}
	createdTo, apiErr := timeParam(q.Get("createdTo"), "createdTo")
	if apiErr != nil {
		return apiErr
	}

	res, err := s.backend.Provider().ListStates(r.Context(), components.ListStatesReq{
		ActorType:      workflow.OrchestratorActorType(name),
		WorkflowLabels: labels,
		CreatedFrom:    createdFrom,
		CreatedTo:      createdTo,
		After:          cursor.After,
		Limit:          limit,
	})
	if err != nil {
		return s.fail(r, "failed to list workflow instances", err)
	}

	// The listing reads labels only, so it stays at summary size
	items := make([]instanceItemJSON, len(res.States))
	for i, st := range res.States {
		items[i] = instanceItemJSON{InstanceID: st.ActorID, Status: "unknown"}
		if st.WorkflowLabels == nil {
			continue
		}
		items[i].Status = st.WorkflowLabels.Status
		items[i].Version = st.WorkflowLabels.Version
		items[i].Parent = st.WorkflowLabels.Parent
		if st.WorkflowLabels.Created != "" {
			created, err := components.ParseWorkflowCreated(st.WorkflowLabels.Created)
			if err == nil {
				items[i].CreatedAt = optionalTime(created)
			}
		}
	}

	var next string
	if res.HasMore && len(items) > 0 {
		next = encodeCursor(instancesCursor{After: items[len(items)-1].InstanceID})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

// timeParam parses an optional RFC 3339 query parameter
func timeParam(v string, name string) (time.Time, *apiError) {
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, errBadRequest("%s must be an RFC 3339 timestamp", name)
	}
	return t, nil
}

type instanceParentJSON struct {
	InstanceID string `json:"instanceId"`
	Workflow   string `json:"workflow"`
	Step       string `json:"step,omitempty"`
	Index      int    `json:"index"`
}

type instanceSuspendJSON struct {
	Reason   string     `json:"reason,omitempty"`
	At       *time.Time `json:"at,omitempty"`
	ResumeTo string     `json:"resumeTo,omitempty"`
}

type childLinkJSON struct {
	Workflow   string `json:"workflow"`
	InstanceID string `json:"instanceId"`
}

type stepJSON struct {
	Name           string          `json:"name"`
	Kind           string          `json:"kind"`
	Status         string          `json:"status"`
	Iteration      int             `json:"iteration,omitempty"`
	StartedAt      *time.Time      `json:"startedAt,omitempty"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	Error          string          `json:"error,omitempty"`
	TaskCount      int             `json:"taskCount"`
	TasksRemaining int             `json:"tasksRemaining"`
	Children       []childLinkJSON `json:"children,omitempty"`
}

type instanceJSON struct {
	Workflow              string               `json:"workflow"`
	InstanceID            string               `json:"instanceId"`
	Status                string               `json:"status"`
	Cause                 string               `json:"cause,omitempty"`
	TerminalStatus        string               `json:"terminalStatus,omitempty"`
	Compensation          string               `json:"compensation,omitempty"`
	Version               int                  `json:"version"`
	DefinitionFingerprint string               `json:"definitionFingerprint,omitempty"`
	Parent                *instanceParentJSON  `json:"parent,omitempty"`
	Suspended             *instanceSuspendJSON `json:"suspended,omitempty"`
	CreatedAt             *time.Time           `json:"createdAt,omitempty"`
	StartedAt             *time.Time           `json:"startedAt,omitempty"`
	CompletedAt           *time.Time           `json:"completedAt,omitempty"`
	HasOutput             bool                 `json:"hasOutput"`
	// Input and Output are included only for callers with the workflows:data:read scope
	Input        json.RawMessage `json:"input,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
	DataRedacted bool            `json:"dataRedacted,omitempty"`
	Steps        []stepJSON      `json:"steps"`
	EventHistory bool            `json:"eventHistory"`
	// DeadJobs are the orchestrator's dead-lettered jobs, which the engine retries automatically, so an entry can be transient
	DeadJobs []jobJSON `json:"deadJobs"`
}

// loadInstance reads an instance's journal, or its placeholder and start job when it is pending
// A pending instance is reported only while its start job is live, as WorkflowService.GetStatus does
func (s *Server) loadInstance(r *http.Request, aRef ref.ActorRef) (*workflow.InstanceView, *workflow.StartView, *apiError) {
	data, err := s.backend.Provider().GetState(r.Context(), aRef)
	if errors.Is(err, components.ErrNoState) {
		return nil, nil, errNotFound("workflow instance '%s' does not exist", aRef.ActorID)
	} else if err != nil {
		return nil, nil, s.fail(r, "failed to read the workflow instance", err)
	}
	view, err := workflow.DecodeInstance(data)
	if err != nil {
		return nil, nil, s.fail(r, "failed to decode the workflow instance", err)
	}
	if !view.Pending {
		return view, nil, nil
	}

	alarm, err := s.backend.Provider().GetAlarm(r.Context(), ref.NewAlarmRef(aRef.ActorType, aRef.ActorID, workflow.StartJobName))
	if errors.Is(err, components.ErrNoAlarm) {
		return nil, nil, errNotFound("workflow instance '%s' does not exist", aRef.ActorID)
	} else if err != nil {
		return nil, nil, s.fail(r, "failed to read the workflow start job", err)
	}
	start, err := workflow.DecodeStartPayload(alarm.Data)
	if err != nil {
		return nil, nil, s.fail(r, "failed to decode the workflow start job", err)
	}

	return view, start, nil
}

// handleGetInstance serves GET /api/v1/workflows/{name}/instances/{instanceId}
func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request) *apiError {
	name, aRef, apiErr := instanceRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}

	view, start, apiErr := s.loadInstance(r, aRef)
	if apiErr != nil {
		return apiErr
	}

	res := instanceJSON{
		Workflow:              name,
		InstanceID:            aRef.ActorID,
		Status:                string(view.Status),
		Cause:                 view.Cause,
		TerminalStatus:        string(view.TerminalStatus),
		Compensation:          string(view.Compensation),
		Version:               view.Version,
		DefinitionFingerprint: view.DefinitionFingerprint,
		CreatedAt:             optionalTime(view.CreatedAt),
		StartedAt:             optionalTime(view.StartedAt),
		CompletedAt:           optionalTime(view.CompletedAt),
		HasOutput:             view.HasOutput,
		Steps:                 make([]stepJSON, len(view.Steps)),
		EventHistory:          view.EventHistory,
		DeadJobs:              []jobJSON{},
	}
	input := view.Input
	parent := view.Parent
	if start != nil {
		// A pending instance takes its input and fingerprint from the start job
		input = start.Input
		if res.DefinitionFingerprint == "" {
			res.DefinitionFingerprint = start.DefinitionFingerprint
		}
		if res.Version == 0 {
			res.Version = start.Version
		}
		if res.CreatedAt == nil {
			res.CreatedAt = optionalTime(start.CreatedAt)
		}
		if parent == nil {
			parent = start.Parent
		}
	}
	if parent != nil {
		res.Parent = &instanceParentJSON{InstanceID: parent.InstanceID, Workflow: parent.Workflow, Step: parent.Step, Index: parent.Index}
	}
	if view.Suspended != nil {
		res.Suspended = &instanceSuspendJSON{Reason: view.Suspended.Reason, At: optionalTime(view.Suspended.At), ResumeTo: string(view.Suspended.ResumeTo)}
	}
	for i, st := range view.Steps {
		res.Steps[i] = stepJSON{
			Name:           st.Name,
			Kind:           st.Kind,
			Status:         st.Status,
			Iteration:      st.Iteration,
			StartedAt:      optionalTime(st.StartedAt),
			CompletedAt:    optionalTime(st.CompletedAt),
			Error:          st.Error,
			TaskCount:      st.TaskCount,
			TasksRemaining: st.TasksRemaining,
		}
		for _, c := range st.Children {
			res.Steps[i].Children = append(res.Steps[i].Children, childLinkJSON{Workflow: c.Workflow, InstanceID: c.InstanceID})
		}
	}

	// Input and output require their own scope, and reading them is audited
	if callerFromContext(r.Context()).Has(ScopeWorkflowsDataRead) {
		s.auditRead(r, "workflowInstance.readData", slog.String("workflow", name), slog.String("instanceId", aRef.ActorID))
		res.Input = input
		if view.HasOutput {
			res.Output = view.Output
			if len(res.Output) == 0 {
				res.Output = json.RawMessage("null")
			}
		}
	} else {
		res.DataRedacted = true
	}

	// Link the orchestrator's dead-lettered jobs
	jobs, err := s.backend.Provider().QueryJobs(r.Context(), components.QueryJobsReq{
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		Status:    components.JobStatusDeadLettered,
		Limit:     components.DefaultManagementListLimit,
	})
	if err != nil {
		return s.fail(r, "failed to list the instance's dead-lettered jobs", err)
	}
	for _, j := range jobs.Jobs {
		res.DeadJobs = append(res.DeadJobs, newJob(j))
	}

	writeJSON(w, http.StatusOK, res)
	return nil
}

type eventJSON struct {
	Seq        int64          `json:"seq"`
	Time       time.Time      `json:"time"`
	TimeSource string         `json:"timeSource,omitempty"`
	Kind       string         `json:"kind"`
	Step       string         `json:"step,omitempty"`
	TaskIndex  *int           `json:"taskIndex,omitempty"`
	Attempt    int            `json:"attempt,omitempty"`
	Outcome    string         `json:"outcome,omitempty"`
	Error      string         `json:"error,omitempty"`
	Child      *childLinkJSON `json:"child,omitempty"`
	Iteration  int            `json:"iteration,omitempty"`
	Undo       bool           `json:"undo,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	EventName  string         `json:"eventName,omitempty"`
	DueTime    *time.Time     `json:"dueTime,omitempty"`
}

type eventsCursor struct {
	After int64 `json:"a"`
}

// handleListEvents serves GET /api/v1/workflows/{name}/instances/{instanceId}/events
func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) *apiError {
	_, aRef, apiErr := instanceRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}
	var cursor eventsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	view, _, apiErr := s.loadInstance(r, aRef)
	if apiErr != nil {
		return apiErr
	}
	if !view.Pending && !view.EventHistory {
		return newAPIError(http.StatusNotFound, CodeEventHistoryDisabled, "the workflow does not record event history")
	}

	res, err := s.backend.Provider().ListWorkflowEvents(r.Context(), components.ListWorkflowEventsReq{
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		AfterSeq:  cursor.After,
		Limit:     limit,
	})
	if err != nil {
		return s.fail(r, "failed to list workflow events", err)
	}

	items := make([]eventJSON, 0, len(res.Events))
	for _, ev := range res.Events {
		v, err := workflow.DecodeEvent(ev)
		if err != nil {
			return s.fail(r, "failed to decode a workflow event", err)
		}
		item := eventJSON{
			Seq:        v.Seq,
			Time:       v.Time.UTC(),
			TimeSource: v.TimeSource,
			Kind:       v.Kind,
			Step:       v.Step,
			TaskIndex:  v.TaskIndex,
			Attempt:    v.Attempt,
			Outcome:    v.Outcome,
			Error:      v.Error,
			Iteration:  v.Iteration,
			Undo:       v.Undo,
			Reason:     v.Reason,
			EventName:  v.EventName,
		}
		if !v.DueTime.IsZero() {
			dueTime := v.DueTime.UTC()
			item.DueTime = &dueTime
		}
		if v.Child != nil {
			item.Child = &childLinkJSON{Workflow: v.Child.Workflow, InstanceID: v.Child.InstanceID}
		}
		items = append(items, item)
	}

	var next string
	if res.HasMore && len(items) > 0 {
		next = encodeCursor(eventsCursor{After: items[len(items)-1].Seq})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

type controlRequestJSON struct {
	Reason string `json:"reason"`
}

type controlResponseJSON struct {
	Workflow   string `json:"workflow"`
	InstanceID string `json:"instanceId"`
	Action     string `json:"action"`
	// Coalesced is true when a control job of the same kind was already pending, so this request's reason was dropped
	Coalesced bool `json:"coalesced"`
	// NoEffect is true when the engine would ignore the request, so no job was dispatched
	NoEffect bool   `json:"noEffect"`
	Status   string `json:"status"`
}

// handleControlInstance returns the handler of POST /api/v1/workflows/{name}/instances/{instanceId}/{action}
func (s *Server) handleControlInstance(action workflow.ControlAction) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) *apiError {
		var body controlRequestJSON
		apiErr := decodeBody(r, &body)
		if apiErr == nil {
			apiErr = s.controlInstance(w, r, action, body)
		}
		s.auditAction(r, "workflowInstance."+string(action), apiErr, withAuditReason(body.Reason,
			slog.String("workflow", r.PathValue("name")),
			slog.String("instanceId", r.PathValue("instanceId")),
		)...)
		return apiErr
	}
}

func (s *Server) controlInstance(w http.ResponseWriter, r *http.Request, action workflow.ControlAction, body controlRequestJSON) *apiError {
	name, aRef, apiErr := instanceRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}
	switch {
	case action == workflow.ControlCancel && strings.TrimSpace(body.Reason) == "":
		return errBadRequest("a reason is required to cancel an instance")
	case action == workflow.ControlResume && body.Reason != "":
		return errBadRequest("resume does not take a reason")
	case len(body.Reason) > maxReasonLength:
		return errBadRequest("reason must not exceed %d bytes", maxReasonLength)
	}

	apiErr = s.checkExclusiveLease(r)
	if apiErr != nil {
		return apiErr
	}

	view, _, apiErr := s.loadInstance(r, aRef)
	if apiErr != nil {
		return apiErr
	}
	res := controlResponseJSON{
		Workflow:   name,
		InstanceID: aRef.ActorID,
		Action:     string(action),
		Status:     string(view.Status),
	}

	// The engine ignores a control job for a terminal instance, and a cancel for one that is already compensating
	terminal := view.Status == workflow.StatusCompleted || view.Status == workflow.StatusFailed || view.Status == workflow.StatusCancelled
	if terminal || (action == workflow.ControlCancel && view.Status == workflow.StatusCompensating) {
		res.NoEffect = true
		writeJSON(w, http.StatusOK, res)
		return nil
	}

	// Dispatch the same durable control job WorkflowService sends to the orchestrator
	// The provider checks the exclusive-access lease again atomically with the insert, so a job can't land in a cluster that is being restored
	method, key, data, err := workflow.ControlJob(action, body.Reason)
	if err != nil {
		return s.fail(r, "failed to encode the control job", err)
	}
	created, err := s.backend.DispatchJob(r.Context(), ref.NewAlarmRef(aRef.ActorType, aRef.ActorID, key), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime: s.clock.Now(),
			Data:    data,
		},
		Kind:                  components.AlarmKindJob,
		JobMethod:             method,
		RejectIfClusterLocked: true,
	})
	if err != nil {
		return s.fail(r, "failed to dispatch the control job", err)
	}
	res.Coalesced = !created

	writeJSON(w, http.StatusAccepted, res)
	return nil
}
