//go:build integration

package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// providerOutage takes the database away from every host at once, rather than from one, and verifies the cluster comes back intact once it returns
//
// The single-host outage scenarios always leave a survivor to take the work over, so recovery is a matter of re-placing actors
// Here there is no survivor: every host fails its health checks and stops, which is the disaster case where recovery depends on nothing having been lost to the outage rather than on the cluster routing around it
type providerOutage struct {
	cluster *cluster.Cluster
}

func (s *providerOutage) Name() string {
	return "faults-provider-outage/local/sqlite"
}

func (s *providerOutage) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Local,
		Variant:                 provider.SQLite,
		Hosts:                   2,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: healthCheckDeadline,
		HealthCheckPolicy:       healthCheckPolicy,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		AlarmsPollInterval:      alarmsPollInterval,
		AlarmsLeaseDuration:     alarmsLeaseDuration,
		StallableProvider:       true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *providerOutage) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-provider-outage-1"
	const alarmName = "a"

	labelHosts(s.cluster)

	// Persist some state and arm a repeating alarm, so both kinds of durable work are riding on the outage
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	err = s.cluster.Service(0).SetAlarm(ctx, shared.ProbeActorType, actorID, alarmName, actor.AlarmProperties{
		DueTime:  time.Now(),
		Interval: shared.ISOInterval(300 * time.Millisecond),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= 2
	}, 30*time.Second, 100*time.Millisecond, "the alarm should be firing before the outage")

	// Take the database away from the whole cluster at once
	for i := range s.cluster.Len() {
		s.cluster.StallProvider(t, i)
	}

	// With nothing left to report health to, every host gives up and stops rather than carrying on against a database it cannot reach
	for i := range s.cluster.Len() {
		exitErr := s.cluster.Host(i).WaitExit(t, exitTimeout)
		require.Errorf(t, exitErr, "host %d should stop itself once its health checks cannot land", i)
		require.ErrorContainsf(t, exitErr, "health check", "host %d should stop because of its health checks", i)
	}

	// Give the database back and bring the cluster up again, the way an operator would after the outage
	for i := range s.cluster.Len() {
		s.cluster.UnstallProvider(t, i)
	}
	countBeforeRestart := shared.ProbeObserver.AlarmCount(actorID)
	for i := range s.cluster.Len() {
		s.cluster.Host(i).Run(t)
	}

	// Nothing was lost: the actor is reachable, its state survived, and new work builds on it
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, recoveryTimeout, recoveryInterval, "the actor should be reachable once the database is back and the hosts are running")
	assert.Equal(t, int64(1), out.N, "persisted state should survive an outage that stopped every host")

	env, err = s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	assert.Equal(t, int64(2), out.N, "the recovered actor should accept new work")

	// The alarm survived too, rather than being dropped along with the hosts that were executing it
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= countBeforeRestart+2
	}, recoveryTimeout, recoveryInterval, "the repeating alarm should resume after the cluster comes back")

	// Stop the alarm so it cannot leak into later scenarios
	err = s.cluster.Service(0).DeleteAlarm(ctx, shared.ProbeActorType, actorID, alarmName)
	require.NoError(t, err)
}
