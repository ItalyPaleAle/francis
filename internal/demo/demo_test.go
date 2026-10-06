package demo

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRun starts the demo cluster and checks that the management API reports the sample data the dashboard relies on
func TestRun(t *testing.T) {
	// The management API listens on a port that is free right now
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bind := ln.Addr().String()
	err = ln.Close()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Run the demo until the test cancels it, and signal once the data is seeded
	ready := make(chan struct{})
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, Options{
			ManagementBind: bind,
			OnReady:        func() { close(ready) },
		})
	}()

	select {
	case <-ready:
	case err = <-runErr:
		require.FailNow(t, "the demo stopped before it was ready", "error: %v", err)
	case <-time.After(seedTimeout):
		require.FailNow(t, "the demo did not become ready")
	}

	// The summary counts the unreachable host, the partial activations, and the seeded workflow instances
	// Some of the data settles in the background, such as the jobs that fail, so the checks retry for a while
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var summary struct {
			Hosts struct {
				Total       int `json:"total"`
				Connected   int `json:"connected"`
				Unreachable int `json:"unreachable"`
			} `json:"hosts"`
			Activations struct {
				Live    int  `json:"live"`
				Partial bool `json:"partial"`
			} `json:"activations"`
			Jobs      map[string]struct{ Count int }            `json:"jobs"`
			Workflows map[string]map[string]struct{ Count int } `json:"workflows"`
		}
		getJSON(c, bind, "/api/v1/cluster/summary", &summary)

		assert.Equal(c, 4, summary.Hosts.Total)
		assert.Equal(c, 3, summary.Hosts.Connected)
		assert.Equal(c, 1, summary.Hosts.Unreachable)
		assert.True(c, summary.Activations.Partial)
		assert.Positive(c, summary.Activations.Live)
		for _, status := range []string{"pending", "active", "completed", "dead"} {
			assert.Positive(c, summary.Jobs[status].Count, "jobs in status %s", status)
		}
		for _, status := range []string{"completed", "failed", "running", "suspended", "cancelled"} {
			assert.Positive(c, summary.Workflows[checkoutWorkflow][status].Count, "checkout instances in status %s", status)
		}
	}, 30*time.Second, 250*time.Millisecond)

	// The two definitions of the export workflow conflict
	var workflows struct {
		Items []struct {
			Name      string `json:"name"`
			Conflicts []struct {
				Version int `json:"version"`
			} `json:"conflicts"`
		} `json:"items"`
	}
	getJSON(t, bind, "/api/v1/workflows", &workflows)
	conflicts := map[string]int{}
	for _, wf := range workflows.Items {
		conflicts[wf.Name] = len(wf.Conflicts)
	}
	assert.Equal(t, map[string]int{
		checkoutWorkflow:   0,
		exportWorkflow:     1,
		kycWorkflow:        0,
		onboardingWorkflow: 0,
		reportWorkflow:     0,
	}, conflicts)

	// The demo stops cleanly once its context is canceled
	cancel()
	select {
	case err = <-runErr:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the demo did not stop")
	}
}

// getJSON fetches a management API path with the read-only token and decodes the response
func getJSON(t require.TestingT, bind string, path string, out any) {
	req, err := http.NewRequest(http.MethodGet, "http://"+bind+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+ReadOnlyToken)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	err = json.NewDecoder(res.Body).Decode(out)
	require.NoError(t, err)
}
