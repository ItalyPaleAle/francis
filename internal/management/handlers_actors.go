package management

import (
	"encoding/json"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

type activationJSON struct {
	HostID       string    `json:"hostId"`
	ActorType    string    `json:"actorType"`
	ActorID      string    `json:"actorId"`
	ActivatedAt  time.Time `json:"activatedAt"`
	Deactivating bool      `json:"deactivating"`
	ObservedAt   time.Time `json:"observedAt"`
}

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
	HostID      string           `json:"hostId"`
	ObservedAt  time.Time        `json:"observedAt"`
	ActiveCount int              `json:"activeCount"`
	Items       []activationJSON `json:"items"`
	NextCursor  string           `json:"nextCursor,omitempty"`
}

// handleHostActivations serves GET /api/v1/hosts/{hostId}/activations
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
	Items      []activationJSON `json:"items"`
	NextCursor string           `json:"nextCursor,omitempty"`
	Partial    bool             `json:"partial"`
	Errors     []hostErrorJSON  `json:"errors"`
	StartedAt  time.Time        `json:"startedAt"`
	FinishedAt time.Time        `json:"finishedAt"`
}

// handleListActivations serves GET /api/v1/activations
// It walks hosts in ID order, querying a batch of hosts concurrently, until the page is full
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
}

type placementsCursor struct {
	Type string `json:"t"`
	ID   string `json:"i"`
}

// handleListPlacements serves GET /api/v1/placements
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
	BuiltIn   bool   `json:"builtIn"`
	// Placements totals the placement usage across the hosts serving the type
	Placements capacityJSON         `json:"placements"`
	Hosts      []actorTypeHostJSON  `json:"hosts"`
	Retention  *retentionJSON       `json:"jobRetention"`
	Capacity   []actorTypeGroupJSON `json:"capacityGroups"`
}

type actorTypeHostJSON struct {
	HostID        string        `json:"hostId"`
	Draining      bool          `json:"draining"`
	IdleTimeoutMs int64         `json:"idleTimeoutMs"`
	Placements    capacityJSON  `json:"placements"`
	Retention     retentionJSON `json:"jobRetention"`
}

type actorTypeGroupJSON struct {
	capacityGroupJSON

	HostID string `json:"hostId"`
}

type actorTypesJSON struct {
	Items      []actorTypeJSON `json:"items"`
	Partial    bool            `json:"partial"`
	Errors     []hostErrorJSON `json:"errors"`
	ObservedAt time.Time       `json:"observedAt"`
}

// handleListActorTypes serves GET /api/v1/actor-types
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
	ActorID        string              `json:"actorId"`
	WorkflowLabels *workflowLabelsJSON `json:"workflowLabels,omitempty"`
}

type workflowLabelsJSON struct {
	Status  string `json:"status,omitempty"`
	Version int    `json:"version,omitempty"`
	Parent  string `json:"parent,omitempty"`
	Created string `json:"created,omitempty"`
}

type actorStatesCursor struct {
	After string `json:"a"`
}

// handleListActorStates serves GET /api/v1/actor-states
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

type actorStateJSON struct {
	ActorType string          `json:"actorType"`
	ActorID   string          `json:"actorId"`
	Size      int             `json:"size"`
	Lossy     bool            `json:"lossy"`
	State     json.RawMessage `json:"state"`
}

// handleGetActorState serves GET /api/v1/actor-states/{type}/{id}
func (s *Server) handleGetActorState(w http.ResponseWriter, r *http.Request) *apiError {
	aRef, apiErr := actorRefFromPath(r)
	if apiErr != nil {
		return apiErr
	}

	s.auditRead(r, "actorState.read", slog.String("actorType", aRef.ActorType), slog.String("actorId", aRef.ActorID))

	data, err := s.backend.Provider().GetState(r.Context(), aRef)
	if errors.Is(err, components.ErrNoState) {
		return errNotFound("no state is stored for actor '%s'", aRef.String())
	} else if err != nil {
		return s.fail(r, "failed to get actor state", err)
	}

	// The exact stored bytes are returned on request
	if acceptsMsgpack(r) {
		w.Header().Set("Content-Type", "application/msgpack")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		// The body is served as application/msgpack with no-sniff, so it is not rendered as HTML
		_, _ = w.Write(data) //nolint:gosec
		return nil
	}

	state, lossy, err := msgpackToJSON(data)
	if err != nil {
		return newAPIErrorf(http.StatusUnprocessableEntity, CodeStateNotDecodable, "the stored state can't be rendered as JSON (%v); request it with 'Accept: application/msgpack' to read the raw bytes", err)
	}

	writeJSON(w, http.StatusOK, actorStateJSON{
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		Size:      len(data),
		Lossy:     lossy,
		State:     state,
	})
	return nil
}

// acceptsMsgpack reports whether the request asks for the raw MessagePack encoding
// MessagePack must be listed by name with a non-zero quality, and at least as high as the quality JSON gets, so wildcards alone keep the JSON default
func acceptsMsgpack(r *http.Request) bool {
	var (
		// msgpackQ stays negative unless a MessagePack type is listed, and the aliases all name the same format
		msgpackQ = -1.0
		jsonQ    float64
		// jsonRank is how specific the range that set jsonQ is, since the most specific matching range decides a type's quality
		jsonRank int
	)
	for part := range strings.SplitSeq(r.Header.Get("Accept"), ",") {
		mediaType, q, ok := parseAcceptRange(part)
		if !ok {
			continue
		}

		if mediaType == "application/msgpack" || mediaType == "application/x-msgpack" || mediaType == "application/vnd.msgpack" {
			msgpackQ = max(msgpackQ, q)
			continue
		}

		// A range that covers JSON replaces the quality of any less specific one
		rank := jsonRangeRank(mediaType)
		switch {
		case rank == 0:
			// The range doesn't cover JSON
		case rank > jsonRank:
			jsonQ = q
			jsonRank = rank
		case rank == jsonRank:
			jsonQ = max(jsonQ, q)
		}
	}
	return msgpackQ > 0 && msgpackQ >= jsonQ
}

// jsonRangeRank ranks the media ranges that cover JSON from the least to the most specific, and returns 0 for any other range
func jsonRangeRank(mediaType string) int {
	switch mediaType {
	case "*/*":
		return 1
	case "application/*":
		return 2
	case "application/json":
		return 3
	default:
		return 0
	}
}

// parseAcceptRange parses one media range of an Accept header, returning its lowercased media type and quality
// A range that can't be parsed, or has a quality outside 0 to 1, is reported as not ok so it is ignored
func parseAcceptRange(part string) (mediaType string, q float64, ok bool) {
	mediaType, params, err := mime.ParseMediaType(part)
	if err != nil {
		return "", 0, false
	}

	// A range without a quality has the default of 1
	qs, hasQ := params["q"]
	if !hasQ {
		return mediaType, 1, true
	}
	// The range check is written so that NaN fails it too
	q, err = strconv.ParseFloat(qs, 64)
	if err != nil || !(q >= 0 && q <= 1) {
		return "", 0, false
	}
	return mediaType, q, true
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
	HostID    string `json:"hostId,omitempty"`
	NotActive bool   `json:"notActive"`
}

// handleDeactivateActor serves POST /api/v1/actors/{type}/{id}/deactivate
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

func (s *Server) deactivateActor(w http.ResponseWriter, r *http.Request, aRef ref.ActorRef) *apiError {
	// Deactivating writes nothing to the provider, so there's no write to re-check the lease with
	// A lease taken after this check is harmless: the evicted host halts every actor anyway, and a restore waits until no host is connected
	apiErr := s.checkExclusiveLease(r)
	if apiErr != nil {
		return apiErr
	}

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
