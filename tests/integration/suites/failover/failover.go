//go:build integration

// Package failover holds resilience scenarios that stop hosts mid-test and assert the cluster keeps actors, their state, and their alarms available
package failover

import (
	"strconv"
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
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// variants is the representative set the failover scenarios run against
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the failover scenarios across the representative variants on both runtimes, where two hosts share one backend
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			// Two hosts share one backend, so on the local runtime only providers that coordinate across processes qualify
			if k == cluster.Remote || v.LocalMultiHost() {
				suite.Register(&hostFailover{kind: k, variant: v})
				suite.Register(&alarmMigration{kind: k, variant: v})
			}
		}
	}
}

// hostIndex returns the index of the host carrying the given label, or -1
func hostIndex(labels []string, label string) int {
	for i, l := range labels {
		if l == label {
			return i
		}
	}
	return -1
}

// labelHosts assigns each host a stable label and returns them in host order
func labelHosts(c *cluster.Cluster) []string {
	labels := make([]string, c.Len())
	for i := range c.Len() {
		labels[i] = "h" + strconv.Itoa(i)
		shared.SetHostLabel(c.Service(i), labels[i])
	}
	return labels
}

// hostFailover places an actor, stops the host it landed on, and verifies the actor is re-placed elsewhere with its state intact
type hostFailover struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *hostFailover) Name() string {
	return "failover-host/" + string(s.kind) + "/" + string(s.variant)
}

func (s *hostFailover) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *hostFailover) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "failover-1"

	// Label each host so the probe can report where the actor is placed
	labels := labelHosts(s.cluster)

	// Place the actor and persist some state, then learn which host it landed on
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	require.NotEmpty(t, placed, "the probe should have recorded its placement host")

	// Find the host the actor landed on, and pick a survivor to drive the cluster after it is gone
	placedIdx := hostIndex(labels, placed)
	require.GreaterOrEqual(t, placedIdx, 0)
	survivor := (placedIdx + 1) % s.cluster.Len()

	// Stop the host the actor lives on
	s.cluster.Host(placedIdx).Stop(t)

	// The actor must be re-placed on a surviving host, with its persisted state carried over so the next increment yields two
	// Re-placement and routing settle after the stopped host deregisters, so retry until an invocation succeeds
	require.Eventually(t, func() bool {
		e, invErr := s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		if invErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, 45*time.Second, 500*time.Millisecond, "actor should be reachable again after its host is stopped")

	// State survived the failover, and the actor now runs on a different host
	assert.Equal(t, int64(2), out.N, "persisted state should survive failover")
	assert.NotEqual(t, placed, shared.ProbeObserver.LastInvokeHost(actorID), "actor should have moved to a surviving host")
}

// alarmMigration sets a repeating alarm, stops the host executing it, and verifies another host takes over so the alarm keeps firing
type alarmMigration struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *alarmMigration) Name() string {
	return "failover-alarm/" + string(s.kind) + "/" + string(s.variant)
}

func (s *alarmMigration) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              2,
		Actors:             []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
		AlarmsPollInterval: 250 * time.Millisecond,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *alarmMigration) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "alarm-mig-1"

	labels := labelHosts(s.cluster)

	// A repeating alarm activates the actor on whichever host leases it
	require.NoError(t, s.cluster.Service(0).SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
		DueTime:  time.Now(),
		Interval: shared.ISOInterval(300 * time.Millisecond),
	}))

	// Wait until it is firing steadily, then learn which host is executing it
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= 2 && shared.ProbeObserver.LastAlarmHost(actorID) != ""
	}, 20*time.Second, 100*time.Millisecond, "alarm should fire on some host")

	owner := shared.ProbeObserver.LastAlarmHost(actorID)
	ownerIdx := hostIndex(labels, owner)
	require.GreaterOrEqual(t, ownerIdx, 0)

	// Stop the host executing the alarm
	countAtStop := shared.ProbeObserver.AlarmCount(actorID)
	s.cluster.Host(ownerIdx).Stop(t)

	// Another host must take over the lease so the alarm keeps firing past where the stopped host left off
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= countAtStop+2
	}, 30*time.Second, 100*time.Millisecond, "a surviving host should take over the alarm")

	// The alarm now executes on a different host, confirming ownership migrated
	assert.NotEqual(t, owner, shared.ProbeObserver.LastAlarmHost(actorID), "alarm ownership should have migrated to a surviving host")

	// Stop the alarm so it cannot leak into later scenarios
	survivor := (ownerIdx + 1) % s.cluster.Len()
	require.NoError(t, s.cluster.Service(survivor).DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a"))
}
