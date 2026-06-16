//go:build integration

// Package runtimefailover verifies that on the remote topology the cluster survives losing a runtime replica: hosts roll over to a surviving replica and actor state owned by the shared store carries across the outage
//
// The host failover scenarios stop hosts while the runtime keeps running, while this exercises the opposite axis, where part of the control plane goes down and the hosts must continue against what remains
package runtimefailover

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// variants is the set of provider backends whose store two runtime replicas can share
// An outage test rebuilds nothing, but both replicas must read and write the same store, which the in-memory standalone provider cannot do, so it is excluded
var variants = []provider.Variant{provider.SQLite, provider.Postgres}

// Register the runtime-replica-failover scenario for every shareable-store variant
// It is meaningful only on the remote topology, where standalone runtimes own the provider the hosts depend on
func init() {
	for _, v := range variants {
		suite.Register(&runtimeFailover{variant: v})
	}
}

// runtimeFailover places an actor, stops one of two runtime replicas, and verifies the host keeps serving the actor with its state intact through the surviving replica
type runtimeFailover struct {
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *runtimeFailover) Name() string {
	return "runtimefailover/remote/" + string(s.variant)
}

func (s *runtimeFailover) Setup(t *testing.T) []framework.Option {
	// Two runtime replicas share one store, and a single host keeps every invocation local so the test isolates host-to-runtime rollover from cross-host routing
	s.cluster = cluster.New(t, cluster.Options{
		Kind:            cluster.Remote,
		Variant:         s.variant,
		Hosts:           1,
		RuntimeReplicas: 2,
		Actors:          []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *runtimeFailover) Run(t *testing.T) {
	ctx := t.Context()
	actorID := "runtime-failover-" + string(s.variant)

	// Place the actor and persist some state through whichever replica the host is using
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	// Take one replica offline
	// The surviving replica still owns the same shared store
	s.cluster.Runtime(0).Stop(t)

	// The host keeps serving the actor: if it was using the stopped replica it rolls over to the survivor with backoff, so retry until a call lands again
	// Get is read-only, so a request the client retries across the rollover cannot inflate the counter, and the value confirms the pre-outage state carried over
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, 60*time.Second, 500*time.Millisecond, "the host should keep serving the actor through the surviving runtime replica")
	assert.Equal(t, int64(1), out.N, "actor state should survive a runtime replica outage")

	// New work persists on top of the survived state, proving the surviving replica is fully functional
	// Retry until one lands, asserting the counter only grew, since a write retried across the rollover may already have been applied
	var got int64
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		if rErr != nil {
			return false
		}
		var st shared.ProbeState
		if e.Decode(&st) != nil {
			return false
		}
		got = st.N
		return true
	}, 60*time.Second, 500*time.Millisecond, "the surviving replica should accept new work")
	assert.Greater(t, got, int64(1), "a post-outage increment should persist on top of the survived state")
}
