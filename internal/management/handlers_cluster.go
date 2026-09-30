package management

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
	"uuid"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// summaryCountCap bounds each count in the cluster summary, since providers have no count methods and counting pages through the rows
const summaryCountCap = 10_000

// boundedCountJSON is a count that stops at summaryCountCap
type boundedCountJSON struct {
	Count int `json:"count"`
	// Truncated is true when the count reached the cap, so the real count is at least Count
	Truncated bool `json:"truncated"`
}

type exclusiveLeaseJSON struct {
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type clusterHostsJSON struct {
	Total       int `json:"total"`
	Connected   int `json:"connected"`
	Draining    int `json:"draining"`
	Unreachable int `json:"unreachable"`
}

type clusterActivationsJSON struct {
	// Live is the number of in-memory activations reported by the hosts that answered
	Live int `json:"live"`
	// Partial is true when some hosts could not be queried, so Live is a lower bound
	Partial bool            `json:"partial"`
	Errors  []hostErrorJSON `json:"errors"`
}

type clusterSummaryJSON struct {
	Topology string `json:"topology"`
	// ExclusiveLease is the holder of the cluster exclusive-access lease, nil when none is held
	ExclusiveLease *exclusiveLeaseJSON `json:"exclusiveLease"`
	// Runtimes is the number of runtime replicas with a live membership, nil in the local topology
	Runtimes    *int                        `json:"runtimes"`
	Hosts       clusterHostsJSON            `json:"hosts"`
	Placements  int                         `json:"placements"`
	Activations clusterActivationsJSON      `json:"activations"`
	Jobs        map[string]boundedCountJSON `json:"jobs"`
	// Workflows counts the instances of every workflow by status
	Workflows  map[string]map[string]boundedCountJSON `json:"workflows"`
	ObservedAt time.Time                              `json:"observedAt"`
}

// handleClusterSummary serves GET /api/v1/cluster/summary
func (s *Server) handleClusterSummary(w http.ResponseWriter, r *http.Request) *apiError {
	ctx := r.Context()
	provider := s.backend.Provider()

	res := clusterSummaryJSON{
		Topology: string(s.backend.Topology()),
		Activations: clusterActivationsJSON{
			Errors: []hostErrorJSON{},
		},
		Jobs:      map[string]boundedCountJSON{},
		Workflows: map[string]map[string]boundedCountJSON{},
	}

	lease, err := provider.GetExclusiveLease(ctx)
	if err != nil {
		return s.fail(r, "failed to read the exclusive lease", err)
	}
	if lease.IsHeld() {
		res.ExclusiveLease = &exclusiveLeaseJSON{Owner: lease.Owner, ExpiresAt: lease.ExpiresAt.UTC()}
	}

	runtimes, err := s.backend.Runtimes(ctx)
	switch {
	case errors.Is(err, ErrNotApplicable):
		// The local topology has no runtime replicas
	case err != nil:
		return s.fail(r, "failed to list runtimes", err)
	default:
		n := len(runtimes)
		res.Runtimes = &n
	}

	// Count hosts by state and placements from the provider
	hosts, err := s.listAllHosts(ctx)
	if err != nil {
		return s.fail(r, "failed to list hosts", err)
	}
	reach := s.backend.HostReachability(ctx, hosts)
	reachable := make([]components.HostDetails, 0, len(hosts))
	res.Hosts.Total = len(hosts)
	for _, h := range hosts {
		item := newHost(h, reach[h.HostID])
		switch item.State {
		case HostStateConnected:
			res.Hosts.Connected++
		case HostStateDraining:
			res.Hosts.Draining++
		case HostStateUnreachable:
			res.Hosts.Unreachable++
		}
		res.Placements += item.PlacementCount
		if item.State != HostStateUnreachable {
			reachable = append(reachable, h)
		}
	}

	// Count live activations from the hosts, without listing them
	for _, sr := range s.snapshotHosts(ctx, reachable, protocol.HostSnapshotRequest{SkipActivations: true}) {
		if sr.Err != nil {
			res.Activations.Partial = true
			res.Activations.Errors = append(res.Activations.Errors, newHostError(sr.Host.HostID, sr.Err))
			continue
		}
		res.Activations.Live += sr.Snapshot.ActiveCount
	}
	if len(reachable) < len(hosts) {
		res.Activations.Partial = true
	}

	// Count jobs by status
	for _, status := range []components.JobStatus{components.JobStatusPending, components.JobStatusActive, components.JobStatusCompleted, components.JobStatusDeadLettered} {
		count, err := countJobs(ctx, provider, status)
		if err != nil {
			return s.fail(r, "failed to count jobs", err)
		}
		res.Jobs[string(status)] = count
	}

	// Count workflow instances by status, through the labels of the orchestrators' state
	types, err := provider.ListStateActorTypes(ctx, workflow.ActorTypePrefix)
	if err != nil {
		return s.fail(r, "failed to list workflow types", err)
	}
	for _, t := range types {
		name, role, _, ok := workflow.ParseActorType(t)
		if !ok || role != workflow.RoleOrchestrator {
			continue
		}
		counts := map[string]boundedCountJSON{}
		for _, status := range validInstanceStatuses {
			count, err := countInstances(ctx, provider, t, status)
			if err != nil {
				return s.fail(r, "failed to count workflow instances", err)
			}
			if count.Count > 0 {
				counts[string(status)] = count
			}
		}
		res.Workflows[name] = counts
	}

	res.ObservedAt = s.clock.Now().UTC()

	writeJSON(w, http.StatusOK, res)
	return nil
}

// countPages counts the items of a paginated listing, up to summaryCountCap
// fetch reads the page after a cursor, returning its number of items, whether more follow, and the cursor after its last item
func countPages[C any](fetch func(after C) (n int, hasMore bool, next C, err error)) (boundedCountJSON, error) {
	var (
		res   boundedCountJSON
		after C
	)
	for {
		n, hasMore, next, err := fetch(after)
		if err != nil {
			return res, err
		}
		res.Count += n
		if !hasMore || n == 0 {
			return res, nil
		}
		if res.Count >= summaryCountCap {
			res.Truncated = true
			return res, nil
		}
		after = next
	}
}

// countJobs counts the jobs in a status, up to summaryCountCap
func countJobs(ctx context.Context, provider components.ManagementProvider, status components.JobStatus) (boundedCountJSON, error) {
	return countPages(func(after components.UUIDCursor) (int, bool, components.UUIDCursor, error) {
		page, err := provider.QueryJobs(ctx, components.QueryJobsReq{
			Status: status,
			After:  after,
			Limit:  components.MaxManagementListLimit,
		})
		if err != nil || len(page.Jobs) == 0 {
			return 0, false, after, err
		}
		next, err := uuid.Parse(page.Jobs[len(page.Jobs)-1].JobID)
		if err != nil {
			return 0, false, after, fmt.Errorf("provider returned a job ID that is not a UUID: %w", err)
		}
		return len(page.Jobs), page.HasMore, next, nil
	})
}

// countInstances counts the instances of an orchestrator type in a status, up to summaryCountCap
func countInstances(ctx context.Context, provider components.ManagementProvider, actorType string, status workflow.Status) (boundedCountJSON, error) {
	return countPages(func(after string) (int, bool, string, error) {
		page, err := provider.ListStates(ctx, components.ListStatesReq{
			ActorType:      actorType,
			WorkflowLabels: &components.WorkflowLabels{Status: string(status)},
			After:          after,
			Limit:          components.MaxListStatesLimit,
		})
		if err != nil || len(page.States) == 0 {
			return 0, false, after, err
		}
		return len(page.States), page.HasMore, page.States[len(page.States)-1].ActorID, nil
	})
}
