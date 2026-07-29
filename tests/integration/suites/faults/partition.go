//go:build integration

package faults

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// runtimePartition cuts the network between one host and the runtime, leaving both processes running, and verifies that the cluster moves the isolated host's actors to a survivor and that the isolated host rejoins once the network comes back
//
// This is the case a clean shutdown never covers: the runtime is never told anything, and the host never learns it has been replaced, so recovery has to come from the health check deadline expiring on one side and the host's own reconnect loop on the other
type runtimePartition struct {
	cluster *cluster.Cluster
}

func (s *runtimePartition) Name() string {
	return "faults-runtime-partition/remote/sqlite"
}

func (s *runtimePartition) Setup(t *testing.T) []framework.Option {
	// Each host reaches the runtime through its own severable link, so cutting one leaves the other host connected
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Remote,
		Variant:                 provider.SQLite,
		Hosts:                   2,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: healthCheckDeadline,
		HealthCheckPolicy:       healthCheckPolicy,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		RuntimeLinks:            true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *runtimePartition) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-runtime-partition-1"

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

	hostIDBefore := s.cluster.Host(placedIdx).HostID()
	require.NotEmpty(t, hostIDBefore)
	deactivationsBefore := shared.ProbeObserver.DeactivateCount(actorID)

	// Cut the host holding the actor off from the control plane, without stopping either side
	s.cluster.RuntimeLink(t, placedIdx).Sever(t)

	// The runtime stops hearing from the isolated host, expires its registration, and re-places the actor on the survivor with its state intact
	// The isolated host cannot reach the runtime for state or placement either, so calls that still route to it fail until the move settles
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil || e.Decode(&out) != nil {
			return false
		}
		return shared.ProbeObserver.LastInvokeHost(actorID) == labels[survivor]
	}, recoveryTimeout, recoveryInterval, "the actor should be re-placed on the surviving host while its own host is partitioned")
	assert.Equal(t, int64(1), out.N, "persisted state should survive the partition")

	// Restore the network and confirm the isolated host rejoins the cluster rather than staying a zombie
	s.cluster.RuntimeLink(t, placedIdx).Restore(t)
	const rejoinedActorID = "faults-runtime-partition-2"
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(placedIdx).Invoke(ctx, shared.ProbeActorType, rejoinedActorID, shared.ProbeMethodIncrement, nil)
		if rErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, recoveryTimeout, recoveryInterval, "the previously isolated host should reconnect and serve invocations again")
	assert.Positive(t, out.N, "the rejoined host should be able to place an actor and persist its state")

	// Its registration expired while it was away, so it could not reclaim its old identity and had to rejoin as a new host
	assert.NotEqual(t, hostIDBefore, s.cluster.Host(placedIdx).HostID(), "a host whose registration expired must not reattach under its old identity")

	// Coming back under a new identity means everything it was still holding was dropped, including the actor that had already moved
	assert.Greater(t, shared.ProbeObserver.DeactivateCount(actorID), deactivationsBefore, "the stale instance should have been deactivated when the host rejoined with a new identity")

	// The rejoined host must not still be serving the actor it held before the partition, which now lives on the survivor
	// It resolves placement rather than answering from its own actor map, so the invocation is forwarded and both hosts agree on the state
	env, err = s.cluster.Service(placedIdx).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	assert.Equal(t, labels[survivor], shared.ProbeObserver.LastInvokeHost(actorID), "the rejoined host must route to the actor's current host, not to its own stale instance")
	assert.Equal(t, int64(2), out.N, "the increment must build on the state the survivor holds, not on a snapshot from before the partition")

	// The survivor sees the same value, so only one live instance ever answered
	env, err = s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	assert.Equal(t, int64(2), out.N, "both hosts must agree on the actor's state after the partition heals")
}

// peerPartition cuts the network between two hosts while both stay registered and healthy, and verifies that an invocation that can no longer reach the host owning the actor fails within the caller's deadline instead of hanging, then succeeds again once the network is back
//
// Placement does not change here: the owning host keeps reporting healthy to the provider, so the cluster has no reason to move the actor, and what is under test is that the caller is not left waiting forever on a peer that has gone silent
type peerPartition struct {
	cluster *cluster.Cluster
}

func (s *peerPartition) Name() string {
	return "faults-peer-partition/local/sqlite"
}

func (s *peerPartition) Setup(t *testing.T) []framework.Option {
	// Each host advertises a severable link in front of its peer server, so peer traffic to one host can be cut while it keeps talking to the database
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Local,
		Variant:                 provider.SQLite,
		Hosts:                   2,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: healthCheckDeadline,
		HealthCheckPolicy:       healthCheckPolicy,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		PeerLinks:               true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *peerPartition) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-peer-partition-1"
	// callTimeout is the deadline the caller puts on an invocation it expects to fail, and is well short of the transport's own idle timeout so the assertion is about the caller's deadline being honored
	const callTimeout = 10 * time.Second

	labels := labelHosts(s.cluster)

	// Place the actor and learn which host owns it, then pick the other one as the caller
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	placedIdx := hostIndex(labels, placed)
	require.GreaterOrEqual(t, placedIdx, 0, "the probe should have recorded its placement host")
	caller := (placedIdx + 1) % s.cluster.Len()

	// Confirm the cross-host path works before breaking it, which also establishes the pooled peer session the outage then strands
	env, err = s.cluster.Service(caller).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(2), out.N)

	// Cut peer traffic to the host owning the actor, leaving it registered and healthy
	s.cluster.PeerLink(t, placedIdx).Sever(t)

	// The invocation can no longer reach the owner, and must come back as an error within the caller's deadline rather than blocking on a dead connection
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	_, err = s.cluster.Service(caller).Invoke(callCtx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	cancel()
	require.Error(t, err, "an invocation to an unreachable peer must fail rather than hang")

	// Once peer traffic flows again the caller reaches the actor without anything having to be restarted
	s.cluster.PeerLink(t, placedIdx).Restore(t)
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(caller).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, recoveryTimeout, recoveryInterval, "the caller should reach the actor again once peer traffic is restored")

	// State is at least what was written before the outage: the invocation that failed may or may not have been applied before the network went, but nothing may have been lost
	assert.GreaterOrEqual(t, out.N, int64(2), "state written before the outage must survive it")
}
