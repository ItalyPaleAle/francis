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
	HostID          string    `json:"hostId"`
	Address         string    `json:"address"`
	State           string    `json:"state"`
	Draining        bool      `json:"draining"`
	LastHealthCheck time.Time `json:"lastHealthCheck"`
	OwnerRuntimeID  string    `json:"ownerRuntimeId,omitempty"`
	ActorTypeCount  int       `json:"actorTypeCount"`
	PlacementCount  int       `json:"placementCount"`
}

type hostDetailJSON struct {
	hostJSON

	ActorTypes []hostActorTypeJSON `json:"actorTypes"`
	// Runtime is the host's own report, nil when the host could not be queried
	Runtime      *hostRuntimeJSON `json:"runtime,omitempty"`
	RuntimeError *hostErrorJSON   `json:"runtimeError,omitempty"`
	ObservedAt   time.Time        `json:"observedAt"`
}

// hostRuntimeJSON is what a host reports about itself in a snapshot
type hostRuntimeJSON struct {
	ObservedAt     time.Time            `json:"observedAt"`
	ActiveCount    int                  `json:"activeCount"`
	Draining       bool                 `json:"draining"`
	CapacityGroups []capacityGroupJSON  `json:"capacityGroups"`
	Workflows      []workflowDefinition `json:"workflows"`
}

type hostActorTypeJSON struct {
	ActorType     string        `json:"actorType"`
	IdleTimeoutMs int64         `json:"idleTimeoutMs"`
	Placements    capacityJSON  `json:"placements"`
	Retention     retentionJSON `json:"jobRetention"`
}

// capacityJSON reports the placement usage of an actor type on a host
type capacityJSON struct {
	Used int `json:"used"`
	// Limit and Available are nil when the actor type has no placement limit
	Limit     *int32 `json:"limit"`
	Available *int   `json:"available"`
	Unlimited bool   `json:"unlimited"`
}

type capacityGroupJSON struct {
	Name       string   `json:"name"`
	Limit      int      `json:"limit"`
	InUse      int      `json:"inUse"`
	ActorTypes []string `json:"actorTypes"`
}

type retentionJSON struct {
	CompletedJobs    retentionPolicyJSON `json:"completedJobs"`
	DeadLetteredJobs retentionPolicyJSON `json:"deadLetteredJobs"`
}

// retentionPolicyJSON explains whether terminal jobs leave a record, and for how long
type retentionPolicyJSON struct {
	Recorded    bool  `json:"recorded"`
	Forever     bool  `json:"forever"`
	RetentionMs int64 `json:"retentionMs,omitempty"`
}

type workflowDefinition struct {
	Name        string `json:"name"`
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

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
	RuntimeID      string    `json:"runtimeId"`
	Address        string    `json:"address"`
	Self           bool      `json:"self"`
	LastHeartbeat  time.Time `json:"lastHeartbeat"`
	ExpiresAt      time.Time `json:"expiresAt"`
	ConnectedHosts *int      `json:"connectedHosts"`
	Error          string    `json:"error,omitempty"`
}

// handleListRuntimes serves GET /api/v1/runtimes
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
