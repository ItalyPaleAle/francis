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
	"github.com/italypaleale/francis/internal/utils"
	"github.com/italypaleale/francis/protocol"
)

// workflowLinkJSON links an actor of a workflow to its workflow instance
//
//	@Description	Links an actor of a workflow to its workflow instance, derived from the actor type and ID naming conventions.
type workflowLinkJSON struct {
	Workflow string `json:"workflow"`
	Role     string `json:"role" enums:"orchestrator,worker,undo,registry,other"`
	// The capability of a worker or undo actor type, when it has one
	Capability string `json:"capability,omitempty"`
	// The workflow instance, for orchestrator, worker, and undo actors whose ID could be parsed
	InstanceID string `json:"instanceId,omitempty"`
	// The step, for worker and undo actors
	Step string `json:"step,omitempty"`
	// The task index within the step, for worker and undo actors
	TaskIndex *int `json:"taskIndex,omitempty"`
} //	@name	WorkflowLink

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
		rest, idxStr, ok := strings.CutLast(actorID, "|")
		if !ok {
			return res
		}
		instance, step, ok := strings.CutLast(rest, "|")
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
	FirstSeenAt time.Time `json:"firstSeenAt" format:"date-time"`
	Generation  uint64    `json:"generation"`
} //	@name	WorkflowVersion

type workflowHostJSON struct {
	HostID   string `json:"hostId"`
	Draining bool   `json:"draining"`
	// The versions the host serves, `null` when the host could not be queried
	Definitions []workflowHostDefinitionJSON `json:"definitions" extensions:"x-nullable"`
} //	@name	WorkflowHost

type workflowHostDefinitionJSON struct {
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
} //	@name	WorkflowHostDefinition

// workflowConflictJSON is a version registered or served with more than one definition fingerprint
//
//	@Description	A version registered or served with more than one definition fingerprint.
type workflowConflictJSON struct {
	Version      int      `json:"version"`
	Fingerprints []string `json:"fingerprints"`
	// The hosts serving the version, not counting the registry itself
	Hosts []string `json:"hosts"`
} //	@name	WorkflowConflict

type workflowJSON struct {
	Name string `json:"name"`
	// The versions recorded by the workflow's definition registry
	Versions []workflowVersionJSON `json:"versions"`
	// The live hosts serving the workflow's orchestrator
	Hosts     []workflowHostJSON     `json:"hosts"`
	Conflicts []workflowConflictJSON `json:"conflicts"`
} //	@name	Workflow

type workflowsJSON struct {
	Items []workflowJSON `json:"items"`
	// True when some hosts could not be queried, so their definitions are missing
	Partial    bool            `json:"partial"`
	Errors     []hostErrorJSON `json:"errors"`
	ObservedAt time.Time       `json:"observedAt" format:"date-time"`
} //	@name	Workflows

// handleListWorkflows serves GET /api/v1/workflows
// Workflows are known from their registry state, which exists once an instance started, and from the orchestrator types live hosts serve
//
//	@Summary		List workflows
//	@ID				listWorkflows
//	@Description	Requires scope `workflows:read`.
//	@Description
//	@Description	Lists the workflows known to the cluster, sorted by name.
//	@Description	A workflow is known from its definition registry (which exists once an instance started), from stored orchestrator state, and from the orchestrator types live hosts serve.
//	@Description	For each workflow, `versions` comes from the registry, `hosts` from the live hosts serving it, and `conflicts` lists versions registered or served with more than one definition fingerprint.
//	@Description	Not paginated.
//	@Description
//	@Description		This is a fan-out endpoint: a host that cannot be queried is reported in `errors`, sets `partial: true`, and is still listed under the workflows it serves with `definitions: null`.
//	@Tags				Workflows
//	@Security			bearerAuth
//	@x-required-scope	"workflows:read"
//	@Produce			json
//	@Success			200	{object}	workflowsJSON		"The workflows"
//	@Failure			401	{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403	{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500	{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504	{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all	{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401	{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows [get]
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
	InstanceID string `json:"instanceId"`
	// The instance status from its labels, or `unknown` when the instance has no labels
	Status string `json:"status"`
	// Omitted when unknown
	Version int `json:"version,omitempty"`
	// The parent label, for child instances
	Parent string `json:"parent,omitempty"`
	// Omitted when unknown
	CreatedAt *time.Time `json:"createdAt,omitempty" format:"date-time"`
} //	@name	InstanceItem

type instancesCursor struct {
	After string `json:"a"`
}

// isInstanceStatus reports whether s is a status a workflow instance can have, which makes it a valid filter of an instance listing
// TestInstanceStatuses checks the statuses against every one declared by the workflow package, so a new status can't be left out
func isInstanceStatus(s workflow.Status) bool {
	switch s {
	case workflow.StatusPending,
		workflow.StatusRunning,
		workflow.StatusSuspended,
		workflow.StatusCompensating,
		workflow.StatusCompleted,
		workflow.StatusFailed,
		workflow.StatusCancelled:
		return true
	default:
		return false
	}
}

// handleListInstances serves GET /api/v1/workflows/{name}/instances
//
//	@Summary		List workflow instances
//	@ID				listWorkflowInstances
//	@Description	Requires scope `workflows:read`.
//	@Description
//	@Description		Lists the instances of a workflow, ordered by instance ID, from the labels stored with each instance (the journals are not read, so items stay at summary size).
//	@Description		When no label filter (`status`, `version`, `parent`) is set, instances without labels are listed too, with `status: "unknown"`.
//	@Tags				Workflows
//	@Security			bearerAuth
//	@x-required-scope	"workflows:read"
//	@Produce			json
//	@Param				name		path		string					true	"The workflow name, which must not contain a slash or a dot"
//	@Param				limit		query		int						false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor		query		string					false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				status		query		string					false	"Only return instances in this status"					Enums(pending, running, suspended, compensating, completed, failed, cancelled)
//	@Param				version		query		int						false	"Only return instances running this definition version"	minimum(1)
//	@Param				parent		query		string					false	"Only return child instances whose parent label equals this value"
//	@Param				createdFrom	query		string					false	"Only return instances created at or after this time (RFC 3339)"		format(date-time)
//	@Param				createdTo	query		string					false	"Only return instances created strictly before this time (RFC 3339)"	format(date-time)
//	@Success			200			{object}	page[instanceItemJSON]	"A page of instances"
//	@Failure			400			{object}	apiError				"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError				"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError				"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500			{object}	apiError				"`internal`: an unexpected server error"
//	@Failure			504			{object}	apiError				"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id			"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate		"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances [get]
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
	if labels.Status != "" && !isInstanceStatus(workflow.Status(labels.Status)) {
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
				items[i].CreatedAt = utils.OptionalTimeUTC(created)
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
	// The parent's step that started this instance
	Step string `json:"step,omitempty"`
	// The task index within the parent's step
	Index int `json:"index"`
} //	@name	InstanceParent

type instanceSuspendJSON struct {
	Reason string     `json:"reason,omitempty"`
	At     *time.Time `json:"at,omitempty" format:"date-time"`
	// The status the instance returns to when resumed
	ResumeTo string `json:"resumeTo,omitempty"`
} //	@name	InstanceSuspension

type childLinkJSON struct {
	Workflow   string `json:"workflow"`
	InstanceID string `json:"instanceId"`
} //	@name	ChildLink

type stepJSON struct {
	Name string `json:"name"`
	// The node kind, such as `step`, `parallel`, `foreach`, `child`, `wait`, or `loop`
	Kind string `json:"kind"`
	// The step status, such as `pending`, `running`, `completed`, `failed`, `skipped`, `compensating`, `compensated`, or `compensation-failed`
	Status string `json:"status"`
	// The loop iteration, omitted when zero
	Iteration      int        `json:"iteration,omitempty"`
	StartedAt      *time.Time `json:"startedAt,omitempty" format:"date-time"`
	CompletedAt    *time.Time `json:"completedAt,omitempty" format:"date-time"`
	Error          string     `json:"error,omitempty"`
	TaskCount      int        `json:"taskCount"`
	TasksRemaining int        `json:"tasksRemaining"`
	// The child workflow instances started by this step, omitted when none
	Children []childLinkJSON `json:"children,omitempty"`
} //	@name	Step

type instanceJSON struct {
	Workflow   string `json:"workflow"`
	InstanceID string `json:"instanceId"`
	Status     string `json:"status" enums:"pending,running,suspended,compensating,completed,failed,cancelled"`
	// Why the instance failed, was cancelled, or is compensating
	Cause string `json:"cause,omitempty"`
	// The status a compensating instance will terminate into
	TerminalStatus string `json:"terminalStatus,omitempty"`
	// The compensation outcome
	Compensation          string `json:"compensation,omitempty" enums:"none,completed,partial,failed"`
	Version               int    `json:"version"`
	DefinitionFingerprint string `json:"definitionFingerprint,omitempty"`
	// Present for child instances
	Parent *instanceParentJSON `json:"parent,omitempty"`
	// Present while the instance is suspended
	Suspended   *instanceSuspendJSON `json:"suspended,omitempty"`
	CreatedAt   *time.Time           `json:"createdAt,omitempty" format:"date-time"`
	StartedAt   *time.Time           `json:"startedAt,omitempty" format:"date-time"`
	CompletedAt *time.Time           `json:"completedAt,omitempty" format:"date-time"`
	// True when the instance stored an output, where a stored JSON `null` counts
	HasOutput bool `json:"hasOutput"`
	// The instance input as JSON, present only with the `workflows:data:read` scope and when an input was stored; any JSON value
	Input json.RawMessage `json:"input,omitempty" swaggertype:"object"`
	// The instance output as JSON, present only with the `workflows:data:read` scope and when `hasOutput` is true; any JSON value
	Output json.RawMessage `json:"output,omitempty" swaggertype:"object"`
	// Present and `true` when `input` and `output` were withheld because the token lacks `workflows:data:read`
	DataRedacted bool       `json:"dataRedacted,omitempty"`
	Steps        []stepJSON `json:"steps"`
	// False when the workflow opted out of event history
	EventHistory bool `json:"eventHistory"`
	// The orchestrator's dead-lettered jobs (at most 100), which the engine retries automatically, so an entry can be transient
	DeadJobs []jobJSON `json:"deadJobs"`
} //	@name	Instance

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
//
//	@Summary		Get a workflow instance
//	@ID				getWorkflowInstance
//	@Description	Requires scope `workflows:read`.
//	@Description	The instance's `input` and `output` are included only when the token also grants `workflows:data:read`; that read is audited.
//	@Description	Without it, `dataRedacted` is `true` and `input`/`output` are omitted.
//	@Description
//	@Description		Returns the instance's journal: status, steps, parent, suspension, timestamps, and the orchestrator's dead-lettered jobs (up to 100).
//	@Description		A pending instance (one whose start job has not run yet) is reported only while its start job is live, and takes its input, version, fingerprint, creation time, and parent from the start job.
//	@Tags				Workflows
//	@Security			bearerAuth
//	@x-required-scope	"workflows:read"
//	@Produce			json
//	@Param				name		path		string				true	"The workflow name, which must not contain a slash or a dot"
//	@Param				instanceId	path		string				true	"The workflow instance ID, which must not contain a slash"
//	@Success			200			{object}	instanceJSON		"The instance"
//	@Failure			400			{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404			{object}	apiError			"`notFound`: the instance does not exist"
//	@Failure			500			{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504			{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances/{instanceId} [get]
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
		CreatedAt:             utils.OptionalTimeUTC(view.CreatedAt),
		StartedAt:             utils.OptionalTimeUTC(view.StartedAt),
		CompletedAt:           utils.OptionalTimeUTC(view.CompletedAt),
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
			res.CreatedAt = utils.OptionalTimeUTC(start.CreatedAt)
		}
		if parent == nil {
			parent = start.Parent
		}
	}
	if parent != nil {
		res.Parent = &instanceParentJSON{InstanceID: parent.InstanceID, Workflow: parent.Workflow, Step: parent.Step, Index: parent.Index}
	}
	if view.Suspended != nil {
		res.Suspended = &instanceSuspendJSON{Reason: view.Suspended.Reason, At: utils.OptionalTimeUTC(view.Suspended.At), ResumeTo: string(view.Suspended.ResumeTo)}
	}
	for i, st := range view.Steps {
		res.Steps[i] = stepJSON{
			Name:           st.Name,
			Kind:           st.Kind,
			Status:         st.Status,
			Iteration:      st.Iteration,
			StartedAt:      utils.OptionalTimeUTC(st.StartedAt),
			CompletedAt:    utils.OptionalTimeUTC(st.CompletedAt),
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
	// The event's sequence number within the instance
	Seq  int64     `json:"seq"`
	Time time.Time `json:"time" format:"date-time"`
	// How `time` was obtained
	TimeSource string `json:"timeSource,omitempty" enums:"engine,dispatch,report,worker"`
	// The event kind, one of `instance_started`, `instance_suspended`, `instance_resumed`, `instance_reopened`, `instance_completed`, `instance_failed`, `instance_cancelled`, `cancel_requested`, `unwind_requested`, `compensation_started`, `parent_notified`, `step_started`, `step_completed`, `step_failed`, `step_skipped`, `step_compensating`, `step_compensated`, `step_compensation_failed`, `wait_started`, `event_received`, `task_dispatched`, `child_started`, `worker_started`, `worker_finished`, `task_retry_scheduled`, `task_completed`, `task_failed`, `task_abandoned`, `compensation_dispatched`, `child_unwind_requested`, `compensation_retry_scheduled`, `compensation_completed`, or `compensation_failed`; new kinds may be added
	Kind      string `json:"kind"`
	Step      string `json:"step,omitempty"`
	TaskIndex *int   `json:"taskIndex,omitempty"`
	// Omitted when zero
	Attempt int            `json:"attempt,omitempty"`
	Outcome string         `json:"outcome,omitempty"`
	Error   string         `json:"error,omitempty"`
	Child   *childLinkJSON `json:"child,omitempty"`
	// The loop iteration of the step, omitted outside a loop body
	Iteration int `json:"iteration,omitempty"`
	// Set when the event is about a compensation rather than the forward task, omitted when false
	Undo bool `json:"undo,omitempty"`
	// The reason of a cancel, unwind or suspend, or the cause of an unwind
	Reason string `json:"reason,omitempty"`
	// The event a wait step waits for or received
	EventName string `json:"eventName,omitempty"`
	// The earliest time a scheduled attempt may run, when it is later than the event
	DueTime *time.Time `json:"dueTime,omitempty" format:"date-time"`
} //	@name	Event

type eventsCursor struct {
	After int64 `json:"a"`
}

// handleListEvents serves GET /api/v1/workflows/{name}/instances/{instanceId}/events
//
//	@Summary		List the event history of a workflow instance
//	@ID				listWorkflowInstanceEvents
//	@Description	Requires scope `workflows:read`.
//	@Description
//	@Description		Lists the instance's history events in sequence order.
//	@Description		Returns `404 eventHistoryDisabled` when the workflow opted out of event history, and `404 notFound` when the instance does not exist.
//	@Description		For a pending instance the event-history check is skipped and whatever events are stored (usually none) are listed.
//	@Tags				Workflows
//	@Security			bearerAuth
//	@x-required-scope	"workflows:read"
//	@Produce			json
//	@Param				name		path		string				true	"The workflow name, which must not contain a slash or a dot"
//	@Param				instanceId	path		string				true	"The workflow instance ID, which must not contain a slash"
//	@Param				limit		query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor		query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Success			200			{object}	page[eventJSON]		"A page of events"
//	@Failure			400			{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404			{object}	apiError			"`notFound`: the instance does not exist; or `eventHistoryDisabled`: the workflow opted out of event history"
//	@Failure			500			{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504			{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances/{instanceId}/events [get]
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
		item.DueTime = utils.OptionalTimeUTC(v.DueTime)
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

// controlRequestJSON is the body of a cancel, suspend, or resume request
//
//	@Description	Cancel requires a non-blank reason, suspend takes an optional one, and resume rejects a non-empty one with `400`.
type controlRequestJSON struct {
	// Why the instance is cancelled or suspended, at most 1024 bytes
	Reason string `json:"reason,omitempty" maxLength:"1024"`
} //	@name	ControlRequest

type controlResponseJSON struct {
	Workflow   string `json:"workflow"`
	InstanceID string `json:"instanceId"`
	Action     string `json:"action" enums:"cancel,suspend,resume"`
	// True when a control job of the same kind was already pending, so this request was merged into it and its reason was dropped
	Coalesced bool `json:"coalesced"`
	// True when the engine would ignore the request for the instance's status, so no job was dispatched
	NoEffect bool `json:"noEffect"`
	// The instance status read before dispatching
	Status string `json:"status" enums:"pending,running,suspended,compensating,completed,failed,cancelled"`
} //	@name	ControlResponse

// handleCancelInstance serves POST /api/v1/workflows/{name}/instances/{instanceId}/cancel
//
//	@Summary		Cancel a workflow instance
//	@ID				cancelWorkflowInstance
//	@Description	Requires scope `workflows:manage`. The action is audited, including its reason.
//	@Description
//	@Description	Dispatches the same durable cancel job `WorkflowService.Cancel` sends to the orchestrator; the instance then compensates its completed steps and terminates as cancelled.
//	@Description	A non-empty `reason` is required.
//	@Description
//	@Description	- `202`: the control job was dispatched, or coalesced onto an identical pending one (`coalesced: true`, in which case this request's reason was dropped).
//	@Description	- `200` with `noEffect: true`: the instance is terminal (`completed`, `failed`, `cancelled`) or already `compensating`, so the engine would ignore the request and no job was dispatched.
//	@Description	- While an exclusive-access lease is held on the cluster, the action is refused with `409 exclusiveLeaseHeld`.
//	@Description
//	@Description		`status` in the response is the instance status read before dispatching.
//	@Tags				Actions
//	@Security			bearerAuth
//	@x-required-scope	"workflows:manage"
//	@Accept				json
//	@Produce			json
//	@Param				name		path		string				true	"The workflow name, which must not contain a slash or a dot"
//	@Param				instanceId	path		string				true	"The workflow instance ID, which must not contain a slash"
//	@Param				request		body		controlRequestJSON	true	"Why the instance is cancelled, which is required and must not be blank"
//	@Success			200			{object}	controlResponseJSON	"The engine would ignore the request for the instance's current status, so no job was dispatched (noEffect is true)"
//	@Success			202			{object}	controlResponseJSON	"The control job was dispatched, or coalesced onto an identical pending one"
//	@Failure			400			{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404			{object}	apiError			"`notFound`: the instance does not exist"
//	@Failure			409			{object}	apiError			"`exclusiveLeaseHeld`: an exclusive-access lease is held on the cluster; retryable"
//	@Failure			413			{object}	apiError			"`payloadTooLarge`: the request body exceeds 64 KiB"
//	@Failure			500			{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503			{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504			{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances/{instanceId}/cancel [post]
func (s *Server) handleCancelInstance(w http.ResponseWriter, r *http.Request) *apiError {
	return s.handleControlInstance(w, r, workflow.ControlCancel)
}

// handleSuspendInstance serves POST /api/v1/workflows/{name}/instances/{instanceId}/suspend
//
//	@Summary		Suspend a workflow instance
//	@ID				suspendWorkflowInstance
//	@Description	Requires scope `workflows:manage`. The action is audited, including its reason.
//	@Description
//	@Description	Dispatches the same durable suspend job `WorkflowService.Suspend` sends to the orchestrator.
//	@Description	The `reason` is optional and is recorded on the suspension.
//	@Description	The request body is optional; an empty body is equivalent to `{}`.
//	@Description
//	@Description	- `202`: the control job was dispatched, or coalesced onto an identical pending one (`coalesced: true`, in which case this request's reason was dropped).
//	@Description	- `200` with `noEffect: true`: the instance is terminal (`completed`, `failed`, `cancelled`), so the engine would ignore the request and no job was dispatched.
//	@Description	- While an exclusive-access lease is held on the cluster, the action is refused with `409 exclusiveLeaseHeld`.
//	@Description
//	@Description		`status` in the response is the instance status read before dispatching.
//	@Tags				Actions
//	@Security			bearerAuth
//	@x-required-scope	"workflows:manage"
//	@Accept				json
//	@Produce			json
//	@Param				name		path		string				true	"The workflow name, which must not contain a slash or a dot"
//	@Param				instanceId	path		string				true	"The workflow instance ID, which must not contain a slash"
//	@Param				request		body		controlRequestJSON	false	"An optional reason, recorded on the suspension"
//	@Success			200			{object}	controlResponseJSON	"The engine would ignore the request for the instance's current status, so no job was dispatched (noEffect is true)"
//	@Success			202			{object}	controlResponseJSON	"The control job was dispatched, or coalesced onto an identical pending one"
//	@Failure			400			{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404			{object}	apiError			"`notFound`: the instance does not exist"
//	@Failure			409			{object}	apiError			"`exclusiveLeaseHeld`: an exclusive-access lease is held on the cluster; retryable"
//	@Failure			413			{object}	apiError			"`payloadTooLarge`: the request body exceeds 64 KiB"
//	@Failure			500			{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503			{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504			{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances/{instanceId}/suspend [post]
func (s *Server) handleSuspendInstance(w http.ResponseWriter, r *http.Request) *apiError {
	return s.handleControlInstance(w, r, workflow.ControlSuspend)
}

// handleResumeInstance serves POST /api/v1/workflows/{name}/instances/{instanceId}/resume
//
//	@Summary		Resume a workflow instance
//	@ID				resumeWorkflowInstance
//	@Description	Requires scope `workflows:manage`. The action is audited.
//	@Description
//	@Description	Dispatches the same durable resume job `WorkflowService.Resume` sends to the orchestrator.
//	@Description	Resume does not take a reason: a request with a non-empty `reason` is rejected with `400 badRequest`.
//	@Description	The request body is optional; an empty body is equivalent to `{}`.
//	@Description
//	@Description	- `202`: the control job was dispatched, or coalesced onto an identical pending one (`coalesced: true`).
//	@Description	- `200` with `noEffect: true`: the instance is terminal (`completed`, `failed`, `cancelled`), so the engine would ignore the request and no job was dispatched.
//	@Description	- While an exclusive-access lease is held on the cluster, the action is refused with `409 exclusiveLeaseHeld`.
//	@Description
//	@Description		`status` in the response is the instance status read before dispatching.
//	@Tags				Actions
//	@Security			bearerAuth
//	@x-required-scope	"workflows:manage"
//	@Accept				json
//	@Produce			json
//	@Param				name		path		string				true	"The workflow name, which must not contain a slash or a dot"
//	@Param				instanceId	path		string				true	"The workflow instance ID, which must not contain a slash"
//	@Param				request		body		controlRequestJSON	false	"Must not set a reason"
//	@Success			200			{object}	controlResponseJSON	"The engine would ignore the request for the instance's current status, so no job was dispatched (noEffect is true)"
//	@Success			202			{object}	controlResponseJSON	"The control job was dispatched, or coalesced onto an identical pending one"
//	@Failure			400			{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401			{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403			{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404			{object}	apiError			"`notFound`: the instance does not exist"
//	@Failure			409			{object}	apiError			"`exclusiveLeaseHeld`: an exclusive-access lease is held on the cluster; retryable"
//	@Failure			413			{object}	apiError			"`payloadTooLarge`: the request body exceeds 64 KiB"
//	@Failure			500			{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503			{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504			{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all			{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401			{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/workflows/{name}/instances/{instanceId}/resume [post]
func (s *Server) handleResumeInstance(w http.ResponseWriter, r *http.Request) *apiError {
	return s.handleControlInstance(w, r, workflow.ControlResume)
}

// handleControlInstance serves a cancel, suspend, or resume request, which differ only in the control action they dispatch
// Each action has its own handler so the OpenAPI document can describe its rules
func (s *Server) handleControlInstance(w http.ResponseWriter, r *http.Request, action workflow.ControlAction) *apiError {
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
	// The provider refuses the job while an exclusive-access lease is held, checking it atomically with the insert, so a job can't land in a cluster that is being restored
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
