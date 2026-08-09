//go:build integration

package faults

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
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// hostDeathMidInvocation kills the host owning an actor while a cross-host invocation is still running on it, and verifies the caller is given an answer rather than left waiting on a connection that is never coming back
//
// Every other silent-death scenario kills an idle actor, where the caller only ever sees a stale placement
// Here the call is provably inside the actor when its host goes, which is the case where a caller could plausibly hang until the transport's own idle timeout rather than the deadline it set
type hostDeathMidInvocation struct {
	cluster *cluster.Cluster
}

func (s *hostDeathMidInvocation) Name() string {
	return "faults-host-death-mid-invocation/local/sqlite"
}

func (s *hostDeathMidInvocation) Setup(t *testing.T) []framework.Option {
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

func (s *hostDeathMidInvocation) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-mid-invocation-1"

	labels := labelHosts(s.cluster)

	// Place the actor and persist some state, then learn which host owns it
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	placedIdx := hostIndex(labels, placed)
	require.GreaterOrEqual(t, placedIdx, 0, "the probe should have recorded its placement host")
	caller := (placedIdx + 1) % s.cluster.Len()

	// Start an invocation that parks inside the actor, so the host can be killed with the call demonstrably in flight
	shared.ProbeObserver.ArmBlock(actorID)
	t.Cleanup(func() { shared.ProbeObserver.ReleaseBlock(actorID) })

	type invokeResult struct {
		err error
	}
	resultCh := make(chan invokeResult, 1)
	go func() {
		_, iErr := s.cluster.Service(caller).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodBlock, nil)
		resultCh <- invokeResult{err: iErr}
	}()

	// Wait until the call has actually reached the actor, so this is not racing the dispatch
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.BlockEnteredCount(actorID) >= 1
	}, 30*time.Second, 50*time.Millisecond, "the invocation should reach the actor before its host is killed")

	// Kill the owning host without letting it deregister, while it is still inside the invocation
	s.cluster.StallProvider(t, placedIdx)
	s.cluster.Host(placedIdx).Stop(t)
	s.cluster.UnstallProvider(t, placedIdx)

	// The caller is given an outcome rather than being left hanging on a peer that is gone
	// It must be an error: the actor never completed, so reporting success would be reporting work that did not happen
	select {
	case res := <-resultCh:
		require.Error(t, res.err, "an invocation whose host died mid-call must fail rather than report success")
	case <-time.After(recoveryTimeout):
		t.Fatal("the in-flight invocation never returned after its host was killed")
	}

	// The cluster recovers on its own: the actor is re-placed on the survivor with the state it had before the call
	// The blocking method never persists anything, so the counter must still read what the first increment left
	shared.ProbeObserver.ReleaseBlock(actorID)
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(caller).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil || e.Decode(&out) != nil {
			return false
		}
		return shared.ProbeObserver.LastInvokeHost(actorID) == labels[caller]
	}, recoveryTimeout, recoveryInterval, "the actor should be reachable again on the surviving host")
	assert.Equal(t, int64(1), out.N, "state written before the failed call should survive it")
}
