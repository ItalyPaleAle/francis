//go:build integration

// Package durability verifies what survives a host process restart on the local runtime, where each host owns its store
// Persistent providers keep an actor's state and alarms across a restart, while the pure in-memory provider does not
package durability

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
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// Register one local-runtime scenario per provider variant
func init() {
	// Only the pure in-memory provider loses data on restart, since every other variant is backed by a store the host reopens
	for _, v := range provider.All() {
		suite.Register(&durability{
			variant:  v,
			persists: v != provider.StandaloneMemory,
		})
	}
}

// durability stops and restarts the single host, then checks whether the actor's state and alarms survived
type durability struct {
	variant  provider.Variant
	persists bool

	cluster *cluster.Cluster
}

func (s *durability) Name() string {
	return "durability/local/" + string(s.variant)
}

func (s *durability) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:               cluster.Local,
		Variant:            s.variant,
		Hosts:              1,
		Actors:             []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
		AlarmsPollInterval: 250 * time.Millisecond,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *durability) Run(t *testing.T) {
	ctx := t.Context()
	actorID := "dur-" + string(s.variant)

	// Persist some state by invoking the actor
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	err = env.Decode(&out)
	require.NoError(t, err)
	require.Equal(t, int64(1), out.N)

	if s.persists {
		s.runPersistent(t, actorID)
		return
	}

	s.runEphemeral(t, actorID)
}

// runPersistent restarts the host and confirms the actor's state and a pending alarm both survived
func (s *durability) runPersistent(t *testing.T, actorID string) {
	ctx := t.Context()

	// Arm a repeating alarm and wait for it to fire at least once before the restart
	err := s.cluster.Service(0).SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
		DueTime:  time.Now(),
		Interval: shared.ISOInterval(300 * time.Millisecond),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) >= 1
	}, 20*time.Second, 100*time.Millisecond, "alarm should fire before the restart")

	// Restart the host: stop it, then bring it back up against the same store on a fresh port
	s.cluster.Host(0).Stop(t)
	countAfterStop := shared.ProbeObserver.AlarmCount(actorID)
	s.cluster.Host(0).Rebind(t)
	s.cluster.Host(0).Run(t)

	// The persisted state survived, so a read returns the prior value and the next increment continues from it
	var got shared.ProbeState
	err = s.cluster.Service(0).GetState(ctx, shared.ProbeActorType, actorID, &got)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.N, "state should survive a host restart")

	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	err = env.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.N)

	// The alarm survived too, so it fires again on the restarted host
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) > countAfterStop
	}, 30*time.Second, 100*time.Millisecond, "a persisted alarm should keep firing after the restart")

	err = s.cluster.Service(0).DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a")
	require.NoError(t, err)
}

// runEphemeral restarts the host and confirms the in-memory provider lost the actor's state
func (s *durability) runEphemeral(t *testing.T, actorID string) {
	ctx := t.Context()

	// Restart the host on a fresh port, which discards the in-memory store
	s.cluster.Host(0).Stop(t)
	s.cluster.Host(0).Rebind(t)
	s.cluster.Host(0).Run(t)

	// The state did not persist, so reading it reports not found
	var got shared.ProbeState
	err := s.cluster.Service(0).GetState(ctx, shared.ProbeActorType, actorID, &got)
	require.ErrorIs(t, err, actor.ErrStateNotFound, "in-memory state should not survive a host restart")
}
