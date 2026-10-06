package management

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// summaryCountCap bounds each count in the cluster summary, so a large collection costs the provider a bounded amount of work
const summaryCountCap = 10_000

// boundedCountJSON is a count that stops at summaryCountCap
//
//	@Description	A count that stops at 10,000.
type boundedCountJSON struct {
	Count int `json:"count"`
	// True when the count reached the cap, so the real count is at least `count`
	Truncated bool `json:"truncated"`
} //	@name	BoundedCount

type exclusiveLeaseJSON struct {
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expiresAt" format:"date-time"`
} //	@name	ExclusiveLease

type clusterHostsJSON struct {
	Total       int `json:"total"`
	Connected   int `json:"connected"`
	Draining    int `json:"draining"`
	Unreachable int `json:"unreachable"`
} //	@name	ClusterHosts

type clusterActivationsJSON struct {
	// The number of in-memory activations reported by the hosts that answered
	Live int `json:"live"`
	// True when some hosts were unreachable or could not be queried, so `live` is a lower bound
	Partial bool            `json:"partial"`
	Errors  []hostErrorJSON `json:"errors"`
} //	@name	ClusterActivations

type clusterSummaryJSON struct {
	// `remote` is a cluster of hosts connected to standalone runtime replicas; `local` is a cluster of hosts that embed the provider and talk to each other directly
	Topology string `json:"topology" enums:"remote,local"`
	// The holder of the cluster exclusive-access lease, `null` when none is held; while held, actions are refused
	ExclusiveLease *exclusiveLeaseJSON `json:"exclusiveLease" extensions:"x-nullable"`
	// The number of runtime replicas with a live membership, `null` in the local topology
	Runtimes *int             `json:"runtimes" extensions:"x-nullable"`
	Hosts    clusterHostsJSON `json:"hosts"`
	// Total placements across all hosts, from the provider
	Placements  int                    `json:"placements"`
	Activations clusterActivationsJSON `json:"activations"`
	// Job counts keyed by status (`pending`, `active`, `completed`, `dead`), with every key always present
	Jobs map[string]boundedCountJSON `json:"jobs"`
	// Instance counts keyed by workflow name, then by status, omitting statuses with no instances
	Workflows  map[string]map[string]boundedCountJSON `json:"workflows"`
	ObservedAt time.Time                              `json:"observedAt" format:"date-time"`
} //	@name	ClusterSummary

// handleClusterSummary serves GET /api/v1/cluster/summary
//
//	@Summary		Get a cluster summary
//	@ID				getClusterSummary
//	@Description	Requires scope `cluster:read`.
//	@Description
//	@Description		Summarizes the cluster: topology, exclusive-access lease, runtime replicas, hosts by state, placements, live activations, jobs by status, and workflow instances by status.
//	@Description		Job and workflow instance counts stop at 10,000 per bucket (`truncated: true` then means the real count is at least `count`).
//	@Description		The live activation count is collected from the hosts that are not unreachable; when some host could not be queried, `activations.partial` is `true` and `activations.live` is a lower bound.
//	@Description		The counts are gathered from separate reads and are not transactionally consistent with each other.
//	@Tags				Cluster
//	@Security			bearerAuth
//	@x-required-scope	"cluster:read"
//	@Produce			json
//	@Success			200	{object}	clusterSummaryJSON	"The cluster summary"
//	@Failure			401	{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403	{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500	{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503	{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504	{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all	{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401	{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/cluster/summary [get]
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

		counts, err := countWorkflowInstances(ctx, provider, t)
		if err != nil {
			return s.fail(r, "failed to count workflow instances", err)
		}
		res.Workflows[name] = counts
	}

	res.ObservedAt = s.clock.Now().UTC()

	writeJSON(w, http.StatusOK, res)
	return nil
}

// boundedCount turns a count bounded at summaryCountCap+1 into a count that stops at summaryCountCap
func boundedCount(n int) boundedCountJSON {
	if n > summaryCountCap {
		return boundedCountJSON{Count: summaryCountCap, Truncated: true}
	}

	return boundedCountJSON{Count: n}
}

// countJobs counts the jobs in a status, up to summaryCountCap
func countJobs(ctx context.Context, provider components.ManagementProvider, status components.JobStatus) (boundedCountJSON, error) {
	// Counting one past the cap tells a count that reached the cap apart from one that exceeded it
	n, err := provider.CountJobs(ctx, components.CountJobsReq{
		Status: status,
		Limit:  summaryCountCap + 1,
	})
	if err != nil {
		return boundedCountJSON{}, err
	}

	return boundedCount(n), nil
}

// countWorkflowInstances counts the instances of an orchestrator type by status, leaving out the statuses with no instances
func countWorkflowInstances(ctx context.Context, provider components.ManagementProvider, actorType string) (map[string]boundedCountJSON, error) {
	counts := map[string]boundedCountJSON{}
	for _, status := range []workflow.Status{
		workflow.StatusPending,
		workflow.StatusRunning,
		workflow.StatusSuspended,
		workflow.StatusCompensating,
		workflow.StatusCompleted,
		workflow.StatusFailed,
		workflow.StatusCancelled,
	} {
		n, err := provider.CountStates(ctx, components.CountStatesReq{
			ActorType:      actorType,
			WorkflowLabels: &components.WorkflowLabels{Status: string(status)},
			Limit:          summaryCountCap + 1,
		})
		if err != nil {
			return nil, err
		}

		if n > 0 {
			counts[string(status)] = boundedCount(n)
		}
	}

	return counts, nil
}
