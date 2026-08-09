//go:build integration

package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// exitTimeout bounds how long the scenario waits for a host to give up on its own health checks
	// It is deliberately far longer than the host should need, so a host that fails to stop is reported as such rather than as a timeout of the scenario
	exitTimeout = 45 * time.Second
	// exitSlack is the room allowed for the outage to land mid-interval and for the host to then unwind and return from Run
	exitSlack = 5 * time.Second
)

// healthCheckFailure takes the database away from one host and verifies that the host notices its own health checks failing and shuts itself down, rather than staying up serving actors the rest of the cluster is about to reassign
//
// A busy SQLite database is the everyday cause: writes queue behind a lock they never get, health checks time out, and the host's view of the cluster and the cluster's view of the host drift apart
// Only the affected host's handle is choked, so the other host keeps using the same database file throughout, which is what makes the split observable
type healthCheckFailure struct {
	cluster *cluster.Cluster
}

func (s *healthCheckFailure) Name() string {
	return "faults-healthcheck-failure/local/sqlite"
}

func (s *healthCheckFailure) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Local,
		Variant:                 provider.SQLite,
		Hosts:                   2,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: healthCheckDeadline,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		StallableProvider:       true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *healthCheckFailure) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-healthcheck-1"

	labels := labelHosts(s.cluster)

	// Place the actor and persist some state, then learn which host it landed on
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	placedIdx := hostIndex(labels, placed)
	require.GreaterOrEqual(t, placedIdx, 0, "the probe should have recorded its placement host")
	survivor := (placedIdx + 1) % s.cluster.Len()

	// Take the database away from the host holding the actor, leaving every other host untouched
	stalledAt := time.Now()
	s.cluster.StallProvider(t, placedIdx)

	// The host exhausts its health check retries and stops itself, instead of lingering while the cluster stops counting it as healthy
	exitErr := s.cluster.Host(placedIdx).WaitExit(t, exitTimeout)
	require.Error(t, exitErr, "a host whose health checks keep failing should stop itself")
	require.ErrorContains(t, exitErr, "health check")

	// It gives up within one health check interval plus the full retry budget, rather than retrying indefinitely against a database that is never coming back
	// That bound is what the deadline-derived policy guarantees even for this deliberately short deadline
	policy := components.NewHealthCheckPolicy(healthCheckDeadline)
	maxExit := policy.Interval() + policy.Budget() + exitSlack
	assert.Less(t, time.Since(stalledAt), maxExit, "the host should give up on its health checks within one interval plus the retry budget")

	// Give the database back, which also confirms the outage was the only reason the host went
	s.cluster.UnstallProvider(t, placedIdx)

	// The actor comes back on the surviving host, with the state written before the outage
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil || e.Decode(&out) != nil {
			return false
		}
		return shared.ProbeObserver.LastInvokeHost(actorID) == labels[survivor]
	}, recoveryTimeout, recoveryInterval, "the actor should move to the surviving host after its own host failed its health checks")
	assert.Equal(t, int64(1), out.N, "persisted state should survive a host losing its database")
}
