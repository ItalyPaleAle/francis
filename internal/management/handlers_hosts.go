package management

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// Host states reported by the API
const (
	HostStateConnected   = "connected"
	HostStateDraining    = "draining"
	HostStateUnreachable = "unreachable"
)

type hostJSON struct {
	HostID  string `json:"hostId"`
	Address string `json:"address"`
	// `draining` when the host is draining (regardless of reachability), otherwise `unreachable` when no session to it is held, otherwise `connected`
	State           string    `json:"state" enums:"connected,draining,unreachable"`
	Draining        bool      `json:"draining"`
	LastHealthCheck time.Time `json:"lastHealthCheck" format:"date-time"`
	// The runtime replica holding the host's session, or the runtime the host registered with, omitted when unknown (such as in the local topology)
	OwnerRuntimeID string `json:"ownerRuntimeId,omitempty"`
	ActorTypeCount int    `json:"actorTypeCount"`
	// Total placements on the host across its actor types
	PlacementCount int `json:"placementCount"`
} //	@name	Host

type hostDetailJSON struct {
	hostJSON

	ActorTypes []hostActorTypeJSON `json:"actorTypes"`
	// The host's own report, omitted when the host could not be queried
	Runtime *hostRuntimeJSON `json:"runtime,omitempty"`
	// Why the host could not be queried, omitted on success
	RuntimeError *hostErrorJSON `json:"runtimeError,omitempty"`
	ObservedAt   time.Time      `json:"observedAt" format:"date-time"`
} //	@name	HostDetail

// hostRuntimeJSON is what a host reports about itself in a snapshot
//
//	@Description	What a host reports about itself.
type hostRuntimeJSON struct {
	ObservedAt time.Time `json:"observedAt" format:"date-time"`
	// The number of actors the host holds in memory
	ActiveCount    int                 `json:"activeCount"`
	Draining       bool                `json:"draining"`
	CapacityGroups []capacityGroupJSON `json:"capacityGroups"`
	// The workflow definitions the host serves
	Workflows []workflowDefinition `json:"workflows"`
} //	@name	HostRuntime

type hostActorTypeJSON struct {
	ActorType     string        `json:"actorType"`
	IdleTimeoutMs int64         `json:"idleTimeoutMs"`
	Placements    capacityJSON  `json:"placements"`
	Retention     retentionJSON `json:"jobRetention"`
} //	@name	HostActorType

// capacityJSON reports the placement usage of an actor type on a host
//
//	@Description	The placement usage of an actor type.
type capacityJSON struct {
	Used int `json:"used"`
	// The placement limit, `null` when unlimited
	Limit *int32 `json:"limit" extensions:"x-nullable"`
	// The remaining placements (never negative), `null` when unlimited
	Available *int `json:"available" extensions:"x-nullable"`
	Unlimited bool `json:"unlimited"`
} //	@name	Capacity

// capacityGroupJSON is a host-local execution capacity group
//
//	@Description	A host-local execution capacity group, with its point-in-time usage.
type capacityGroupJSON struct {
	Name       string   `json:"name"`
	Limit      int      `json:"limit"`
	InUse      int      `json:"inUse"`
	ActorTypes []string `json:"actorTypes"`
} //	@name	CapacityGroup

type retentionJSON struct {
	CompletedJobs    retentionPolicyJSON `json:"completedJobs"`
	DeadLetteredJobs retentionPolicyJSON `json:"deadLetteredJobs"`
} //	@name	Retention

// retentionPolicyJSON explains whether terminal jobs leave a record, and for how long
//
//	@Description	Whether terminal jobs leave a record, and for how long.
type retentionPolicyJSON struct {
	// True when a record of the terminal job is kept
	Recorded bool `json:"recorded"`
	// True when the record is kept indefinitely
	Forever bool `json:"forever"`
	// How long the record is kept, in milliseconds, omitted when zero
	RetentionMs int64 `json:"retentionMs,omitempty"`
} //	@name	RetentionPolicy

type workflowDefinition struct {
	Name        string `json:"name"`
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
} //	@name	WorkflowDefinition

func newCapacity(t components.HostActorTypeDetails) capacityJSON {
	res := capacityJSON{
		Used:      t.ActiveCount,
		Unlimited: t.ConcurrencyLimit <= 0,
	}
	if !res.Unlimited {
		limit := t.ConcurrencyLimit
		available := max(int(limit)-t.ActiveCount, 0)
		res.Limit = &limit
		res.Available = &available
	}
	return res
}

func newRetention(t components.HostActorTypeDetails) retentionJSON {
	// The semantics follow components.ActorHostType
	h := components.ActorHostType{
		CompletedJobRetention:    t.CompletedJobRetention,
		DeadLetteredJobRetention: t.DeadLetteredJobRetention,
	}
	var res retentionJSON

	record, retention := h.CompletedJobRecord()
	res.CompletedJobs.Recorded = record
	res.CompletedJobs.Forever = record && retention == 0
	res.CompletedJobs.RetentionMs = retention.Milliseconds()

	dead := h.DeadLetteredJobRecordRetention()
	res.DeadLetteredJobs.Recorded = true
	res.DeadLetteredJobs.Forever = dead == 0
	res.DeadLetteredJobs.RetentionMs = dead.Milliseconds()

	return res
}

func newCapacityGroups(groups []protocol.CapacityGroupInfo) []capacityGroupJSON {
	res := make([]capacityGroupJSON, len(groups))
	for i, g := range groups {
		res[i] = capacityGroupJSON{
			Name:       g.Name,
			Limit:      g.Limit,
			InUse:      g.InUse,
			ActorTypes: g.ActorTypes,
		}
		if res[i].ActorTypes == nil {
			res[i].ActorTypes = []string{}
		}
	}

	return res
}

func newHostRuntime(snap protocol.HostSnapshotResponse) *hostRuntimeJSON {
	res := &hostRuntimeJSON{
		ObservedAt:     time.UnixMilli(snap.ObservedAtUnixMs).UTC(),
		ActiveCount:    snap.ActiveCount,
		Draining:       snap.Draining,
		CapacityGroups: newCapacityGroups(snap.CapacityGroups),
		Workflows:      make([]workflowDefinition, len(snap.Workflows)),
	}
	for i, w := range snap.Workflows {
		res.Workflows[i] = workflowDefinition{Name: w.Name, Version: w.Version, Fingerprint: w.Fingerprint}
	}

	return res
}

// hostState derives the state reported for a host
func hostState(h components.HostDetails, reach HostReachability) string {
	switch {
	case h.Draining:
		return HostStateDraining
	case !reach.Reachable:
		return HostStateUnreachable
	default:
		return HostStateConnected
	}
}

func newHost(h components.HostDetails, reach HostReachability) hostJSON {
	res := hostJSON{
		HostID:          h.HostID,
		Address:         h.Address,
		State:           hostState(h, reach),
		Draining:        h.Draining,
		LastHealthCheck: h.LastHealthCheck.UTC(),
		OwnerRuntimeID:  reach.OwnerRuntimeID,
		ActorTypeCount:  len(h.ActorTypes),
	}
	if res.OwnerRuntimeID == "" {
		res.OwnerRuntimeID = h.RuntimeID
	}
	for _, t := range h.ActorTypes {
		res.PlacementCount += t.ActiveCount
	}
	return res
}

type hostsCursor struct {
	After string `json:"a"`
}

// handleListHosts serves GET /api/v1/hosts
//
//	@Summary		List hosts
//	@ID				listHosts
//	@Description	Requires scope `cluster:read`.
//	@Description
//	@Description		Lists the hosts with a live registration, ordered by host ID.
//	@Description		The `state` filter is applied to each page after it is read, so a filtered page may hold fewer items than `limit` while more pages follow.
//	@Tags				Cluster
//	@Security			bearerAuth
//	@x-required-scope	"cluster:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				state	query		string				false	"Only return hosts in this state"	Enums(connected, draining, unreachable)
//	@Success			200		{object}	page[hostJSON]		"A page of hosts"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/hosts [get]
func (s *Server) handleListHosts(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor hostsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	state := r.URL.Query().Get("state")
	if state != "" && state != HostStateConnected && state != HostStateDraining && state != HostStateUnreachable {
		return errBadRequest("state must be one of: connected, draining, unreachable")
	}

	res, err := s.backend.Provider().ListHostDetails(r.Context(), components.ListHostDetailsReq{
		After: cursor.After,
		Limit: limit,
	})
	if err != nil {
		return s.fail(r, "failed to list hosts", err)
	}

	// The state filter is applied to each page, so a filtered page may hold fewer items than the limit while more pages follow
	reach := s.backend.HostReachability(r.Context(), res.Hosts)
	items := make([]hostJSON, 0, len(res.Hosts))
	for _, h := range res.Hosts {
		item := newHost(h, reach[h.HostID])
		if state != "" && item.State != state {
			continue
		}
		items = append(items, item)
	}

	var next string
	if res.HasMore && len(res.Hosts) > 0 {
		next = encodeCursor(hostsCursor{After: res.Hosts[len(res.Hosts)-1].HostID})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

// handleGetHost serves GET /api/v1/hosts/{hostId}
//
//	@Summary		Get a host
//	@ID				getHost
//	@Description	Requires scope `cluster:read`.
//	@Description
//	@Description		Returns a host's registration, the actor types it serves with their placement usage and job retention, and the host's own report of its execution capacity and served workflow definitions.
//	@Description		The host's report is fetched live; when the host cannot be queried, `runtime` is omitted and `runtimeError` describes the failure, while the rest of the response is still returned with `200`.
//	@Tags				Cluster
//	@Security			bearerAuth
//	@x-required-scope	"cluster:read"
//	@Produce			json
//	@Param				hostId	path		string				true	"The host ID"
//	@Success			200		{object}	hostDetailJSON		"The host"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404		{object}	apiError			"`notFound`: the host is not registered"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/hosts/{hostId} [get]
func (s *Server) handleGetHost(w http.ResponseWriter, r *http.Request) *apiError {
	h, apiErr := s.getHost(r)
	if apiErr != nil {
		return apiErr
	}

	reach := s.backend.HostReachability(r.Context(), []components.HostDetails{h})
	res := hostDetailJSON{
		hostJSON:   newHost(h, reach[h.HostID]),
		ActorTypes: make([]hostActorTypeJSON, len(h.ActorTypes)),
	}
	for i, t := range h.ActorTypes {
		res.ActorTypes[i] = hostActorTypeJSON{
			ActorType:     t.ActorType,
			IdleTimeoutMs: t.IdleTimeout.Milliseconds(),
			Placements:    newCapacity(t),
			Retention:     newRetention(t),
		}
	}

	// Ask the host for its execution capacity, without listing its activations
	snap, err := s.hostSnapshot(r.Context(), h, protocol.HostSnapshotRequest{SkipActivations: true})
	if err != nil {
		hostErr := newHostError(h.HostID, err)
		res.RuntimeError = &hostErr
	} else {
		res.Runtime = newHostRuntime(snap)
	}
	res.ObservedAt = s.clock.Now().UTC()

	writeJSON(w, http.StatusOK, res)
	return nil
}

// getHost loads the host named in the path
func (s *Server) getHost(r *http.Request) (components.HostDetails, *apiError) {
	hostID := r.PathValue("hostId")
	if hostID == "" {
		return components.HostDetails{}, errBadRequest("host ID is required")
	}

	h, err := s.backend.Provider().GetHostDetails(r.Context(), hostID)
	if errors.Is(err, components.ErrHostUnregistered) {
		return components.HostDetails{}, errNotFound("host '%s' is not registered", hostID)
	} else if err != nil {
		return components.HostDetails{}, s.fail(r, "failed to get host", err)
	}

	return h, nil
}

type runtimeJSON struct {
	RuntimeID string `json:"runtimeId"`
	Address   string `json:"address"`
	// True for the replica serving this request
	Self          bool      `json:"self"`
	LastHeartbeat time.Time `json:"lastHeartbeat" format:"date-time"`
	// When the replica's membership expires unless renewed
	ExpiresAt time.Time `json:"expiresAt" format:"date-time"`
	// The number of host sessions the replica holds, `null` when it could not be queried
	ConnectedHosts *int `json:"connectedHosts" extensions:"x-nullable"`
	// Set when the replica could not be queried
	Error string `json:"error,omitempty"`
} //	@name	Runtime

// handleListRuntimes serves GET /api/v1/runtimes
//
//	@Summary		List runtime replicas
//	@ID				listRuntimes
//	@Description	Requires scope `cluster:read`.
//	@Description
//	@Description		Lists the runtime replicas with a live membership, sorted by runtime ID, with the number of host sessions each holds.
//	@Description		The response uses the page envelope, but is never paginated (`nextCursor` is always omitted) and takes no `limit` or `cursor`.
//	@Description		In the local topology there are no runtime replicas, and the endpoint returns `404 notApplicable`.
//	@Tags				Cluster
//	@Security			bearerAuth
//	@x-required-scope	"cluster:read"
//	@Produce			json
//	@Success			200	{object}	page[runtimeJSON]	"The runtime replicas"
//	@Failure			401	{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403	{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404	{object}	apiError			"`notApplicable`: there are no runtime replicas in the local topology"
//	@Failure			500	{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503	{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504	{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all	{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401	{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/runtimes [get]
func (s *Server) handleListRuntimes(w http.ResponseWriter, r *http.Request) *apiError {
	runtimes, err := s.backend.Runtimes(r.Context())
	if err != nil {
		return s.fail(r, "failed to list runtimes", err)
	}

	items := make([]runtimeJSON, len(runtimes))
	for i, rt := range runtimes {
		items[i] = runtimeJSON{
			RuntimeID:      rt.RuntimeID,
			Address:        rt.Address,
			Self:           rt.Self,
			LastHeartbeat:  rt.LastHeartbeat.UTC(),
			ExpiresAt:      rt.ExpiresAt.UTC(),
			ConnectedHosts: rt.ConnectedHosts,
			Error:          rt.Error,
		}
	}
	slices.SortFunc(items, func(a, b runtimeJSON) int {
		return strings.Compare(a.RuntimeID, b.RuntimeID)
	})

	writeJSON(w, http.StatusOK, newPage(items, ""))
	return nil
}
