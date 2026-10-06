package management

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

type activationJSON struct {
	HostID       string    `json:"hostId"`
	ActorType    string    `json:"actorType"`
	ActorID      string    `json:"actorId"`
	ActivatedAt  time.Time `json:"activatedAt" format:"date-time"`
	Deactivating bool      `json:"deactivating"`
	// When the host took the snapshot this activation comes from
	ObservedAt time.Time `json:"observedAt" format:"date-time"`
} //	@name	Activation

func newActivations(hostID string, snap protocol.HostSnapshotResponse) []activationJSON {
	observedAt := time.UnixMilli(snap.ObservedAtUnixMs).UTC()
	res := make([]activationJSON, len(snap.Activations))
	for i, a := range snap.Activations {
		res[i] = activationJSON{
			HostID:       hostID,
			ActorType:    a.ActorType,
			ActorID:      a.ActorID,
			ActivatedAt:  time.UnixMilli(a.ActivatedAtUnixMs).UTC(),
			Deactivating: a.Deactivating,
			ObservedAt:   observedAt,
		}
	}
	return res
}

// snapshotPageLimit returns the page size of a snapshot request, which is bounded by the protocol
func snapshotPageLimit(limit int) int {
	return min(limit, protocol.MaxSnapshotPageSize)
}

type hostActivationsCursor struct {
	After string `json:"a"`
}

type hostActivationsJSON struct {
	HostID     string    `json:"hostId"`
	ObservedAt time.Time `json:"observedAt" format:"date-time"`
	// The number of matching activations on the host, across all pages
	ActiveCount int              `json:"activeCount"`
	Items       []activationJSON `json:"items"`
	// Opaque cursor to pass back as `cursor` to fetch the next page, omitted on the last page
	NextCursor string `json:"nextCursor,omitempty"`
} //	@name	HostActivations

// handleHostActivations serves GET /api/v1/hosts/{hostId}/activations
//
//	@Summary		List the in-memory activations of one host
//	@ID				listHostActivations
//	@Description	Requires scope `actors:read`.
//	@Description
//	@Description		Asks the host directly for a page of the actors it holds in memory, ordered by actor type and then actor ID.
//	@Description		This is the freshest view of a single host; unlike `/activations`, a host that cannot be reached fails the request with `503 hostUnavailable`.
//	@Description		`activeCount` is the number of matching activations on the host, across all pages.
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:read"
//	@Produce			json
//	@Param				hostId	path		string				true	"The host ID"
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		string				false	"Only return activations of this actor type"
//	@Success			200		{object}	hostActivationsJSON	"A page of the host's activations"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404		{object}	apiError			"`notFound`: the host is not registered"
//	@Failure			409		{object}	apiError			"`hostReattached`: the host kept reconnecting, so the request could not be delivered to its current session; retryable"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503		{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/hosts/{hostId}/activations [get]
func (s *Server) handleHostActivations(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor hostActivationsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	h, apiErr := s.getHost(r)
	if apiErr != nil {
		return apiErr
	}

	snap, err := s.hostSnapshot(r.Context(), h, protocol.HostSnapshotRequest{
		ActorType: r.URL.Query().Get("type"),
		After:     cursor.After,
		Limit:     snapshotPageLimit(limit),
	})
	if err != nil {
		return s.fail(r, "failed to query the host", err)
	}

	res := hostActivationsJSON{
		HostID:      h.HostID,
		ObservedAt:  time.UnixMilli(snap.ObservedAtUnixMs).UTC(),
		ActiveCount: snap.ActiveCount,
		Items:       newActivations(h.HostID, snap),
	}
	if snap.Next != "" {
		res.NextCursor = encodeCursor(hostActivationsCursor{After: snap.Next})
	}

	writeJSON(w, http.StatusOK, res)
	return nil
}

type activationsCursor struct {
	// Host is the host the next page starts from
	Host string `json:"h"`
	// After is the position within that host
	After string `json:"a,omitempty"`
}

type activationsJSON struct {
	Items []activationJSON `json:"items"`
	// Opaque cursor to pass back as `cursor` to fetch the next page, omitted on the last page
	NextCursor string `json:"nextCursor,omitempty"`
	// True when some hosts could not be queried, so their activations are missing
	Partial bool            `json:"partial"`
	Errors  []hostErrorJSON `json:"errors"`
	// When collection started
	StartedAt time.Time `json:"startedAt" format:"date-time"`
	// When collection finished
	FinishedAt time.Time `json:"finishedAt" format:"date-time"`
} //	@name	Activations

// handleListActivations serves GET /api/v1/activations
// It walks hosts in ID order, querying a batch of hosts concurrently, until the page is full
//
//	@Summary		List in-memory activations across hosts
//	@ID				listActivations
//	@Description	Requires scope `actors:read`.
//	@Description
//	@Description	Walks the hosts in host ID order, querying a batch of hosts concurrently, until the page is full.
//	@Description	Items are ordered by host, then by actor type and actor ID within each host.
//	@Description
//	@Description		This is a fan-out endpoint: a host that cannot be queried is reported in `errors` and sets `partial: true`, and its activations are missing from the page.
//	@Description		If every queried host failed, the response is `503 noHostsReachable` with the host errors in `details.errors`.
//	@Description		Each host is sampled independently, so the listing is not transactionally consistent: an actor that moved between hosts during collection or between pages may be listed twice or not at all.
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		string				false	"Only return activations of this actor type"
//	@Param				host	query		string				false	"Only query this host, returning 404 notFound when the host is not registered"
//	@Success			200		{object}	activationsJSON		"A page of activations"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404		{object}	apiError			"`notFound`: the host in the host parameter is not registered"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503		{object}	apiError			"`noHostsReachable`: none of the queried hosts could be reached, with the host errors in details.errors; retryable"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/activations [get]
func (s *Server) handleListActivations(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor activationsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	q := r.URL.Query()
	actorType := q.Get("type")
	hostFilter := q.Get("host")

	res := activationsJSON{
		Items:     []activationJSON{},
		Errors:    []hostErrorJSON{},
		StartedAt: s.clock.Now().UTC(),
	}

	// Select the hosts to query, starting from the cursor's host
	hosts, err := s.listAllHosts(r.Context())
	if err != nil {
		return s.fail(r, "failed to list hosts", err)
	}
	hosts = slices.DeleteFunc(hosts, func(h components.HostDetails) bool {
		return (hostFilter != "" && h.HostID != hostFilter) || (cursor.Host != "" && h.HostID < cursor.Host)
	})
	if hostFilter != "" && len(hosts) == 0 && cursor.Host == "" {
		return errNotFound("host '%s' is not registered", hostFilter)
	}

	var (
		queried   int
		succeeded int
		done      bool
	)
	for len(hosts) > 0 && len(res.Items) < limit && !done {
		batch := hosts[:min(len(hosts), s.fanOutConcurrency)]
		hosts = hosts[len(batch):]

		// Every host of the batch is asked for a full page, since it is not known in advance how many items each contributes
		results := make([]snapshotResult, len(batch))
		reqs := make([]protocol.HostSnapshotRequest, len(batch))
		for i, h := range batch {
			reqs[i] = protocol.HostSnapshotRequest{
				ActorType: actorType,
				Limit:     snapshotPageLimit(limit),
			}
			if h.HostID == cursor.Host {
				reqs[i].After = cursor.After
			}
		}
		parallelFor(len(batch), s.fanOutConcurrency, func(i int) {
			results[i].Host = batch[i]
			results[i].Snapshot, results[i].Err = s.hostSnapshot(r.Context(), batch[i], reqs[i])
		})

		// Consume the results in host order until the page is full
		for i, sr := range results {
			queried++
			if sr.Err != nil {
				res.Partial = true
				res.Errors = append(res.Errors, newHostError(sr.Host.HostID, sr.Err))
				continue
			}
			succeeded++

			items := newActivations(sr.Host.HostID, sr.Snapshot)
			room := limit - len(res.Items)
			switch {
			case len(items) > room:
				// The page is full within this host, so the next page continues after the last item taken
				items = items[:room]
				last := items[len(items)-1]
				res.NextCursor = encodeCursor(activationsCursor{Host: sr.Host.HostID, After: last.ActorType + "/" + last.ActorID})
				done = true
			case sr.Snapshot.Next != "":
				// The host has more activations than it returned, so the page ends here even if it is not full, and the next page continues from this host
				res.NextCursor = encodeCursor(activationsCursor{Host: sr.Host.HostID, After: sr.Snapshot.Next})
				done = true
			case len(items) == room:
				// This host is exhausted, so the next page starts from the following host
				nextHost := nextHostID(batch[i+1:], hosts)
				if nextHost != "" {
					res.NextCursor = encodeCursor(activationsCursor{Host: nextHost})
				}
				done = true
			}

			res.Items = append(res.Items, items...)
			if done {
				// Hosts of the batch after this one are served again on the next page
				break
			}
		}
	}

	if queried > 0 && succeeded == 0 {
		return newAPIError(http.StatusServiceUnavailable, CodeNoHostsReachable, "none of the requested hosts could be queried").
			retryable().
			withDetails(map[string]any{"errors": res.Errors})
	}

	res.FinishedAt = s.clock.Now().UTC()
	writeJSON(w, http.StatusOK, res)
	return nil
}

// nextHostID returns the first host ID among the remaining hosts of a batch and the hosts after it
func nextHostID(restOfBatch []components.HostDetails, rest []components.HostDetails) string {
	if len(restOfBatch) > 0 {
		return restOfBatch[0].HostID
	}
	if len(rest) > 0 {
		return rest[0].HostID
	}
	return ""
}

type placementJSON struct {
	ActorType     string `json:"actorType"`
	ActorID       string `json:"actorId"`
	HostID        string `json:"hostId"`
	IdleTimeoutMs int64  `json:"idleTimeoutMs"`
} //	@name	Placement

type placementsCursor struct {
	Type string `json:"t"`
	ID   string `json:"i"`
}

// handleListPlacements serves GET /api/v1/placements
//
//	@Summary		List actor placements
//	@ID				listPlacements
//	@Description	Requires scope `actors:read`.
//	@Description
//	@Description		Lists the placements recorded in the provider, ordered by actor type and then actor ID.
//	@Description		A placement records which host an actor is assigned to; it does not prove the actor is currently in memory (see `/activations`).
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				host	query		string				false	"Only return placements on this host"
//	@Param				type	query		string				false	"Only return placements of this actor type"
//	@Success			200		{object}	page[placementJSON]	"A page of placements"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/placements [get]
func (s *Server) handleListPlacements(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor placementsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}
	q := r.URL.Query()

	res, err := s.backend.Provider().ListPlacements(r.Context(), components.ListPlacementsReq{
		HostID:    q.Get("host"),
		ActorType: q.Get("type"),
		After:     ref.NewActorRef(cursor.Type, cursor.ID),
		Limit:     limit,
	})
	if err != nil {
		return s.fail(r, "failed to list placements", err)
	}

	items := make([]placementJSON, len(res.Placements))
	for i, p := range res.Placements {
		items[i] = placementJSON{
			ActorType:     p.ActorType,
			ActorID:       p.ActorID,
			HostID:        p.HostID,
			IdleTimeoutMs: p.IdleTimeout.Milliseconds(),
		}
	}

	var next string
	if res.HasMore && len(items) > 0 {
		last := items[len(items)-1]
		next = encodeCursor(placementsCursor{Type: last.ActorType, ID: last.ActorID})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

type actorTypeJSON struct {
	ActorType string `json:"actorType"`
	// True for actor types built into Francis, such as workflow actors
	BuiltIn bool `json:"builtIn"`
	// The placement usage summed across the hosts serving the type, unlimited when any host is unlimited
	Placements capacityJSON        `json:"placements"`
	Hosts      []actorTypeHostJSON `json:"hosts"`
	// The job retention, `null` when the hosts serving the type registered different retentions
	Retention *retentionJSON `json:"jobRetention" extensions:"x-nullable"`
	// The capacity groups that include this type, one entry per reporting host
	Capacity []actorTypeGroupJSON `json:"capacityGroups"`
} //	@name	ActorType

type actorTypeHostJSON struct {
	HostID        string        `json:"hostId"`
	Draining      bool          `json:"draining"`
	IdleTimeoutMs int64         `json:"idleTimeoutMs"`
	Placements    capacityJSON  `json:"placements"`
	Retention     retentionJSON `json:"jobRetention"`
} //	@name	ActorTypeHost

type actorTypeGroupJSON struct {
	capacityGroupJSON

	// The host that reported the group
	HostID string `json:"hostId"`
} //	@name	ActorTypeCapacityGroup

type actorTypesJSON struct {
	Items []actorTypeJSON `json:"items"`
	// True when some hosts could not be queried, so their capacity groups are missing
	Partial    bool            `json:"partial"`
	Errors     []hostErrorJSON `json:"errors"`
	ObservedAt time.Time       `json:"observedAt" format:"date-time"`
} //	@name	ActorTypes

// handleListActorTypes serves GET /api/v1/actor-types
//
//	@Summary		List actor types
//	@ID				listActorTypes
//	@Description	Requires scope `actors:read`.
//	@Description
//	@Description	Aggregates the actor types registered by every live host, sorted by actor type, with per-host placement usage and job retention, and the execution capacity groups the hosts report.
//	@Description	Not paginated.
//	@Description
//	@Description		This is a fan-out endpoint for the capacity groups: a host that cannot be queried is reported in `errors`, sets `partial: true`, and contributes no `capacityGroups` entries (its registration is still listed under `hosts`).
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:read"
//	@Produce			json
//	@Success			200	{object}	actorTypesJSON		"The actor types"
//	@Failure			401	{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403	{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500	{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504	{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all	{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401	{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/actor-types [get]
func (s *Server) handleListActorTypes(w http.ResponseWriter, r *http.Request) *apiError {
	hosts, err := s.listAllHosts(r.Context())
	if err != nil {
		return s.fail(r, "failed to list hosts", err)
	}

	// Aggregate the registrations of every host by actor type
	types := map[string]*actorTypeJSON{}
	for _, h := range hosts {
		for _, t := range h.ActorTypes {
			at := types[t.ActorType]
			if at == nil {
				at = &actorTypeJSON{
					ActorType: t.ActorType,
					BuiltIn:   ref.IsBuiltInActorType(t.ActorType),
					Hosts:     []actorTypeHostJSON{},
					Capacity:  []actorTypeGroupJSON{},
				}
				types[t.ActorType] = at
			}
			hostEntry := actorTypeHostJSON{
				HostID:        h.HostID,
				Draining:      h.Draining,
				IdleTimeoutMs: t.IdleTimeout.Milliseconds(),
				Placements:    newCapacity(t),
				Retention:     newRetention(t),
			}
			at.Hosts = append(at.Hosts, hostEntry)

			// The retention is reported at the type level only while every host registered the same one
			if len(at.Hosts) == 1 {
				ret := hostEntry.Retention
				at.Retention = &ret
			} else if at.Retention != nil && *at.Retention != hostEntry.Retention {
				at.Retention = nil
			}
		}
	}

	// Ask every host for its execution capacity groups
	res := actorTypesJSON{
		Items:  make([]actorTypeJSON, 0, len(types)),
		Errors: []hostErrorJSON{},
	}
	for _, sr := range s.snapshotHosts(r.Context(), hosts, protocol.HostSnapshotRequest{SkipActivations: true}) {
		if sr.Err != nil {
			res.Partial = true
			res.Errors = append(res.Errors, newHostError(sr.Host.HostID, sr.Err))
			continue
		}
		for _, g := range newCapacityGroups(sr.Snapshot.CapacityGroups) {
			for _, t := range g.ActorTypes {
				at := types[t]
				if at != nil {
					at.Capacity = append(at.Capacity, actorTypeGroupJSON{HostID: sr.Host.HostID, capacityGroupJSON: g})
				}
			}
		}
	}

	for _, at := range types {
		at.Placements = totalCapacity(at.Hosts)
		res.Items = append(res.Items, *at)
	}
	slices.SortFunc(res.Items, func(a, b actorTypeJSON) int {
		return strings.Compare(a.ActorType, b.ActorType)
	})
	res.ObservedAt = s.clock.Now().UTC()

	writeJSON(w, http.StatusOK, res)
	return nil
}

// totalCapacity sums the placement usage of the hosts serving an actor type, which is unlimited when any host is unlimited
func totalCapacity(hosts []actorTypeHostJSON) capacityJSON {
	var (
		res       capacityJSON
		limit     int32
		available int
	)
	for _, h := range hosts {
		res.Used += h.Placements.Used
		if h.Placements.Unlimited {
			res.Unlimited = true
			continue
		}
		limit += *h.Placements.Limit
		available += *h.Placements.Available
	}
	if !res.Unlimited {
		res.Limit = &limit
		res.Available = &available
	}
	return res
}

type actorStateItemJSON struct {
	ActorID string `json:"actorId"`
	// Present only for state that has workflow labels
	WorkflowLabels *workflowLabelsJSON `json:"workflowLabels,omitempty"`
} //	@name	ActorStateItem

// workflowLabelsJSON holds the labels the workflow engine stores with an orchestrator's state
//
//	@Description	The labels the workflow engine stores with an orchestrator's state. Empty fields are omitted.
type workflowLabelsJSON struct {
	Status  string `json:"status,omitempty"`
	Version int    `json:"version,omitempty"`
	Parent  string `json:"parent,omitempty"`
	// The creation time, in the stored label format (`2006-01-02T15:04:05.000000000Z`)
	Created string `json:"created,omitempty"`
} //	@name	WorkflowLabels

type actorStatesCursor struct {
	After string `json:"a"`
}

// handleListActorStates serves GET /api/v1/actor-states
//
//	@Summary		List the actors of a type that have stored state
//	@ID				listActorStates
//	@Description	Requires scope `actors:read`.
//	@Description
//	@Description		Lists the IDs of the actors of one type that have stored state, ordered by actor ID, with their workflow labels when the state belongs to a workflow orchestrator.
//	@Description		The state itself is not returned; use `GET /api/v1/actor-states/{type}/{id}`.
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:read"
//	@Produce			json
//	@Param				type	query		string						true	"The actor type, which must not be empty and must not contain a slash"	minlength(1)
//	@Param				limit	query		int							false	"Maximum number of items to return"										minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string						false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Success			200		{object}	page[actorStateItemJSON]	"A page of actors with stored state"
//	@Failure			400		{object}	apiError					"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError					"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError					"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500		{object}	apiError					"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError					"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id				"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate			"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/actor-states [get]
func (s *Server) handleListActorStates(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor actorStatesCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	actorType := r.URL.Query().Get("type")
	if ref.ValidateComponents(actorType) != nil {
		return errBadRequest("the type query parameter is required and must not contain '/'")
	}

	res, err := s.backend.Provider().ListStates(r.Context(), components.ListStatesReq{
		ActorType: actorType,
		After:     cursor.After,
		Limit:     limit,
	})
	if err != nil {
		return s.fail(r, "failed to list actor states", err)
	}

	items := make([]actorStateItemJSON, len(res.States))
	for i, st := range res.States {
		items[i] = actorStateItemJSON{ActorID: st.ActorID}
		if st.WorkflowLabels != nil {
			items[i].WorkflowLabels = &workflowLabelsJSON{
				Status:  st.WorkflowLabels.Status,
				Version: st.WorkflowLabels.Version,
				Parent:  st.WorkflowLabels.Parent,
				Created: st.WorkflowLabels.Created,
			}
		}
	}

	var next string
	if res.HasMore && len(items) > 0 {
		next = encodeCursor(actorStatesCursor{After: items[len(items)-1].ActorID})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

// handleGetActorState serves GET /api/v1/actor-states/{type}/{id}
//
//	@Summary		Read an actor's stored state
//	@ID				getActorState
//	@Description	Requires scope `actors:state:read`. Every read is audited (the data itself is never logged).
//	@Description	Reading the state of a workflow's actors, such as an orchestrator's journal, also requires `workflows:data:read`, since it holds the instances' input and output.
//	@Description
//	@Description	Returns the exact stored MessagePack bytes with `Content-Type: application/msgpack`.
//	@Description	The stored value is not decoded or converted; error responses use JSON.
//	@Tags				Actors
//	@Security			bearerAuth
//	@x-required-scope	"actors:state:read"
//	@Produce			application/msgpack
//	@Param				type	path		string				true	"The actor type, which must not contain a slash"
//	@Param				id		path		string				true	"The actor ID, which must not contain a slash"
//	@Success			200		{file}		file				"The exact stored MessagePack bytes"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires, or `workflows:data:read` for a workflow's actor type"
//	@Failure			404		{object}	apiError			"`notFound`: the actor has no stored state"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/actor-states/{type}/{id} [get]
func (s *Server) handleGetActorState(w http.ResponseWriter, r *http.Request) *apiError {
	aRef, apiErr := actorRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}

	// The state of a workflow's actors holds the instances' input and output, which the workflow routes only return with their own scope
	_, _, _, isWorkflow := workflow.ParseActorType(aRef.ActorType)
	if isWorkflow && !callerFromContext(r.Context()).Has(ScopeWorkflowsDataRead) {
		return newAPIErrorf(http.StatusForbidden, CodeForbidden, "reading the state of a workflow's actors requires the '%s' scope too", ScopeWorkflowsDataRead)
	}

	s.auditRead(r, "actorState.read", slog.String("actorType", aRef.ActorType), slog.String("actorId", aRef.ActorID))

	data, err := s.backend.Provider().GetState(r.Context(), aRef)
	if errors.Is(err, components.ErrNoState) {
		return errNotFound("no state is stored for actor '%s'", aRef.String())
	} else if err != nil {
		return s.fail(r, "failed to get actor state", err)
	}

	// Return the stored bytes without decoding potentially untrusted state
	w.Header().Set("Content-Type", "application/msgpack")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) //nolint:gosec // MessagePack is served with nosniff
	return nil
}

// actorRefFromPath reads the actor type and ID path segments
func actorRefFromPath(r *http.Request) (ref.ActorRef, *apiError) {
	aRef := ref.NewActorRef(r.PathValue("type"), r.PathValue("id"))
	if ref.ValidateComponents(aRef.ActorType, aRef.ActorID) != nil {
		return ref.ActorRef{}, errBadRequest("actor type and ID must not be empty and must not contain '/'")
	}
	return aRef, nil
}

type deactivateJSON struct {
	ActorType string `json:"actorType"`
	ActorID   string `json:"actorId"`
	// The host the actor was placed on, omitted when no active placement was found
	HostID string `json:"hostId,omitempty"`
	// True when the actor was not active, so nothing was deactivated
	NotActive bool `json:"notActive"`
} //	@name	DeactivateResponse

// handleDeactivateActor serves POST /api/v1/actors/{type}/{id}/deactivate
//
//	@Summary		Deactivate an actor
//	@ID				deactivateActor
//	@Description	Requires scope `actors:manage`. The action is audited.
//	@Description
//	@Description	Looks up the host the actor is actively placed on (without creating a placement), and asks that host to halt the actor, returning `200` once the host acknowledged.
//	@Description	The actor's stored state is kept; it is re-activated on its next invocation.
//	@Description
//	@Description	- When the actor is not active anywhere, or its host went away since the lookup, the response is `200` with `notActive: true` and no request is sent to a host.
//	@Description	- When the host reports the actor was not active, the response is `200` with `notActive: true` and `hostId` set.
//	@Description	- An exclusive-access lease on the cluster doesn't stop a deactivation, which writes nothing to the provider; the lease makes every host halt its actors anyway.
//	@Description	- If the host reconnected before it received the request, including to another runtime replica, the request is sent again to its new session; only if the host keeps reconnecting is the response `409 hostReattached`, and the request can be sent again.
//	@Description
//	@Description		The request body is not read.
//	@Tags				Actions
//	@Security			bearerAuth
//	@x-required-scope	"actors:manage"
//	@Produce			json
//	@Param				type	path		string				true	"The actor type, which must not contain a slash"
//	@Param				id		path		string				true	"The actor ID, which must not contain a slash"
//	@Success			200		{object}	deactivateJSON		"The actor was deactivated, or was not active"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			409		{object}	apiError			"`hostReattached`: the host kept reconnecting, so the request could not be delivered to its current session; retryable"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503		{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/actors/{type}/{id}/deactivate [post]
func (s *Server) handleDeactivateActor(w http.ResponseWriter, r *http.Request) *apiError {
	aRef, apiErr := actorRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}
	auditAttrs := []any{slog.String("actorType", aRef.ActorType), slog.String("actorId", aRef.ActorID)}

	apiErr = s.deactivateActor(w, r, aRef)
	s.auditAction(r, "actor.deactivate", apiErr, auditAttrs...)
	return apiErr
}

// deactivateActor asks the host an actor is placed on to halt it
// It is not refused while an exclusive-access lease is held, since it writes nothing to the provider
// The lease makes every host halt its actors anyway, and a restore only starts once no host is connected, so it can't overlap a deactivation, which needs a connected host
func (s *Server) deactivateActor(w http.ResponseWriter, r *http.Request, aRef ref.ActorRef) *apiError {
	// Find the host the actor is placed on, without creating a placement
	res := deactivateJSON{
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
	}

	lookup, err := s.backend.Provider().LookupActor(r.Context(), aRef, components.LookupActorOpts{ActiveOnly: true})
	if errors.Is(err, components.ErrNoActor) {
		res.NotActive = true
		writeJSON(w, http.StatusOK, res)
		return nil
	} else if err != nil {
		return s.fail(r, "failed to look up the actor", err)
	}
	res.HostID = lookup.HostID

	h, err := s.backend.Provider().GetHostDetails(r.Context(), lookup.HostID)
	if errors.Is(err, components.ErrHostUnregistered) {
		// The host went away since the lookup, so its placements are about to be removed
		res.NotActive = true
		writeJSON(w, http.StatusOK, res)
		return nil
	} else if err != nil {
		return s.fail(r, "failed to get the actor's host", err)
	}

	// Ask the owner host to halt the actor, and return once it acknowledged
	hostCtx, cancel := s.hostContext(r)
	defer cancel()
	res.NotActive, err = s.backend.DeactivateActor(hostCtx, h, aRef.ActorType, aRef.ActorID)
	if err != nil {
		return s.fail(r, "failed to deactivate the actor", err)
	}

	writeJSON(w, http.StatusOK, res)
	return nil
}
