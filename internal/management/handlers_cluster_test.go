package management

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
)

func TestBoundedCount(t *testing.T) {
	assert.Equal(t, boundedCountJSON{Count: 0}, boundedCount(0))
	assert.Equal(t, boundedCountJSON{Count: 2500}, boundedCount(2500))

	// A count that ends exactly at the cap is not truncated, while one past it is
	assert.Equal(t, boundedCountJSON{Count: summaryCountCap}, boundedCount(summaryCountCap))
	assert.Equal(t, boundedCountJSON{Count: summaryCountCap, Truncated: true}, boundedCount(summaryCountCap+1))
}

func TestClusterSummaryCounts(t *testing.T) {
	// expectEmptyCluster sets up a cluster with no lease, runtimes, or hosts, so only the counts are read
	expectEmptyCluster := func(ts *testServer) {
		ts.provider.EXPECT().GetExclusiveLease(mock.Anything).Return(components.ExclusiveLeaseInfo{}, nil)
		ts.provider.EXPECT().ListHostDetails(mock.Anything, mock.Anything).Return(components.ListHostDetailsRes{}, nil)
	}

	t.Run("counts jobs and workflow instances through the provider's bounded counts", func(t *testing.T) {
		ts := newTestServer(t)
		expectEmptyCluster(ts)

		// Every count asks for one more than the cap, so a count past the cap can be told apart from one that reached it
		jobCounts := map[components.JobStatus]int{
			components.JobStatusPending:      summaryCountCap + 1,
			components.JobStatusActive:       3,
			components.JobStatusCompleted:    summaryCountCap,
			components.JobStatusDeadLettered: 0,
		}
		for status, n := range jobCounts {
			ts.provider.EXPECT().CountJobs(mock.Anything, components.CountJobsReq{Status: status, Limit: summaryCountCap + 1}).Return(n, nil).Once()
		}

		// Only the orchestrator types are counted, by status
		orchestrator := workflow.OrchestratorActorType("orders")
		ts.provider.EXPECT().ListStateActorTypes(mock.Anything, workflow.ActorTypePrefix).Return([]string{orchestrator, workflow.RegistryActorType("orders")}, nil)
		ts.provider.EXPECT().CountStates(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req components.CountStatesReq) (int, error) {
			assert.Equal(t, orchestrator, req.ActorType)
			assert.Equal(t, summaryCountCap+1, req.Limit)
			switch req.WorkflowLabels.Status {
			case string(workflow.StatusRunning):
				return 7, nil
			case string(workflow.StatusCompleted):
				return summaryCountCap + 1, nil
			default:
				return 0, nil
			}
		}).Times(7)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/cluster/summary", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, map[string]any{
			"pending":   map[string]any{"count": float64(summaryCountCap), "truncated": true},
			"active":    map[string]any{"count": float64(3), "truncated": false},
			"completed": map[string]any{"count": float64(summaryCountCap), "truncated": false},
			"dead":      map[string]any{"count": float64(0), "truncated": false},
		}, res["jobs"])
		assert.Equal(t, map[string]any{
			"orders": map[string]any{
				"running":   map[string]any{"count": float64(7), "truncated": false},
				"completed": map[string]any{"count": float64(summaryCountCap), "truncated": true},
			},
		}, res["workflows"])
	})

	t.Run("a failed count fails the request", func(t *testing.T) {
		ts := newTestServer(t)
		expectEmptyCluster(ts)
		ts.provider.EXPECT().CountJobs(mock.Anything, mock.Anything).Return(0, errors.New("database is down")).Once()

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/cluster/summary", testReadOnlyToken, ""), http.StatusInternalServerError, CodeInternal)
	})
}
