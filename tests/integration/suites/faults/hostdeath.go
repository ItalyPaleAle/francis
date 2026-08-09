//go:build integration

package faults

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// silentHostDeath takes a host down in a way the rest of the cluster is never told about, and verifies that callers recover on their own once the dead host's registration expires
//
// The stop is made silent by cutting the host off from its database first, so the deregistration it attempts on the way out never lands and its record is left behind exactly as it would be after a power loss or an OOM kill
// A caller therefore keeps being handed a placement pointing at a host that is not there, and the only thing that can fix it is the health check deadline expiring
type silentHostDeath struct {
	cluster *cluster.Cluster
}

func (s *silentHostDeath) Name() string {
	return "faults-silent-host-death/local/sqlite"
}

func (s *silentHostDeath) Setup(t *testing.T) []framework.Option {
	// A stallable backend gives each host its own database handle, so one host can lose the database while the other keeps using it
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

func (s *silentHostDeath) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-silent-death-1"

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

	// Kill the host holding the actor without letting it say goodbye: with its database gone, the deregistration on its shutdown path cannot land
	stalledAt := time.Now()
	s.cluster.StallProvider(t, placedIdx)
	s.cluster.Host(placedIdx).Stop(t)
	s.cluster.UnstallProvider(t, placedIdx)

	// The cluster still believes the actor lives on the dead host, so a call made inside that window cannot succeed
	// A host can be one interval past its last health check when cut off, so only the remaining retry budget guarantees the registration is still live
	policy := components.NewHealthCheckPolicy(healthCheckDeadline)
	stillWithinGuaranteedWindow := time.Since(stalledAt) < policy.Budget()
	callCtx, cancel := context.WithTimeout(ctx, time.Second)
	_, err = s.cluster.Service(survivor).Invoke(callCtx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
	cancel()
	if stillWithinGuaranteedWindow {
		require.Error(t, err, "the actor must not be reachable while the cluster still believes it lives on the dead host")
	}

	// Once the registration expires the actor is re-placed on the survivor, without anything having told the cluster the host was gone
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil || e.Decode(&out) != nil {
			return false
		}
		return shared.ProbeObserver.LastInvokeHost(actorID) == labels[survivor]
	}, recoveryTimeout, recoveryInterval, "the actor should move to the surviving host once the dead host's registration expires")
	assert.Equal(t, int64(1), out.N, "persisted state should survive a host dying without notice")

	// The recovered actor is fully usable, not just resolvable
	env, err = s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	assert.Equal(t, int64(2), out.N)
}

// alarmAfterSilentDeath kills the host executing a repeating alarm without letting it release anything, and verifies a surviving host picks the alarm up once the lease expires
//
// A clean shutdown hands alarms over, so this covers the other path: the lease is still held by a host that no longer exists and has to time out before anyone else can take it
type alarmAfterSilentDeath struct {
	cluster *cluster.Cluster
}

func (s *alarmAfterSilentDeath) Name() string {
	return "faults-silent-host-death-alarm/local/sqlite"
}

func (s *alarmAfterSilentDeath) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Local,
		Variant:                 provider.SQLite,
		Hosts:                   2,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: healthCheckDeadline,
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

func (s *alarmAfterSilentDeath) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-silent-death-alarm-1"

	labels := labelHosts(s.cluster)

	// A repeating alarm activates the actor on whichever host leases it
	err := s.cluster.Service(0).SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
		DueTime:  time.Now(),
		Interval: shared.ISOInterval(300 * time.Millisecond),
	})
	require.NoError(t, err)

	// Wait until it is firing steadily, then learn which host is executing it
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= 2 && shared.ProbeObserver.LastAlarmHost(actorID) != ""
	}, 30*time.Second, 100*time.Millisecond, "alarm should fire on some host")

	owner := shared.ProbeObserver.LastAlarmHost(actorID)
	ownerIdx := hostIndex(labels, owner)
	require.GreaterOrEqual(t, ownerIdx, 0)
	survivor := (ownerIdx + 1) % s.cluster.Len()

	// Kill the host executing the alarm without letting it release its lease or deregister
	countAtDeath := shared.ProbeObserver.AlarmCount(actorID)
	s.cluster.StallProvider(t, ownerIdx)
	s.cluster.Host(ownerIdx).Stop(t)
	s.cluster.UnstallProvider(t, ownerIdx)

	// The survivor takes the lease over once it expires, so the alarm keeps firing past where the dead host left off
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= countAtDeath+2 &&
			shared.ProbeObserver.LastAlarmHost(actorID) == labels[survivor]
	}, recoveryTimeout, recoveryInterval, "the surviving host should take over an alarm whose owner died without releasing it")

	// Ownership stays put: the dead host's leftover lease must not resurrect execution on the instance it was still holding
	// Alarm execution confirms placement before invoking, so an occurrence that reaches a host the actor has moved off is handed back rather than run
	for range 10 {
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, labels[survivor], shared.ProbeObserver.LastAlarmHost(actorID), "the alarm must keep executing on the surviving host alone")
	}

	// Stop the alarm so it cannot leak into later scenarios
	err = s.cluster.Service(survivor).DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a")
	require.NoError(t, err)
}
