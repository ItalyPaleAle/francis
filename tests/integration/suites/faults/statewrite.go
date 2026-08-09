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

// writeOutageDeadline keeps the host comfortably healthy through the brief outage this scenario induces
// The store is taken away only for as long as one write takes to fail, and a deadline this far above the retry budget means no health check is even due in that window, so the host stays up and the scenario observes the write rather than the host stopping
const writeOutageDeadline = 20 * time.Second

// stateWriteFailure takes the store away between an actor's read and its write, and verifies the failure reaches the caller with nothing left half-written
//
// An actor that loads state, changes it, and saves it is the ordinary shape of Francis work, and the window between the two is where a store outage does the most damage
// What must not happen is the caller being told the call succeeded, or the stored value moving when the write that would have moved it never landed
type stateWriteFailure struct {
	cluster *cluster.Cluster
}

func (s *stateWriteFailure) Name() string {
	return "faults-state-write-failure/local/sqlite"
}

func (s *stateWriteFailure) Setup(t *testing.T) []framework.Option {
	// One host, since the outage is aimed at the store rather than at placement
	s.cluster = cluster.New(t, cluster.Options{
		Kind:                    cluster.Local,
		Variant:                 provider.SQLite,
		Hosts:                   1,
		Actors:                  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
		HostHealthCheckDeadline: writeOutageDeadline,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		StallableProvider:       true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *stateWriteFailure) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-state-write-1"

	// Establish a known stored value the failed write must not be allowed to move
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	// Start an invocation that reads, then parks before writing
	shared.ProbeObserver.ArmBlock(actorID)
	t.Cleanup(func() { shared.ProbeObserver.ReleaseBlock(actorID) })

	resultCh := make(chan error, 1)
	go func() {
		_, iErr := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodBlockingWrite, nil)
		resultCh <- iErr
	}()

	// Wait until the read has happened and the actor is parked, so the outage lands squarely between the read and the write
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.BlockEnteredCount(actorID) >= 1
	}, 30*time.Second, 50*time.Millisecond, "the invocation should reach the actor and park before its write")

	// Take the store away, then let the write go at it
	s.cluster.StallProvider(t, 0)
	shared.ProbeObserver.ReleaseBlock(actorID)

	// The caller is told the call failed, rather than being given a success for work that was never persisted
	select {
	case iErr := <-resultCh:
		require.Error(t, iErr, "an invocation whose state write failed must report the failure")
	case <-time.After(recoveryTimeout):
		t.Fatal("the invocation never returned after its state write was blocked")
	}

	// Give the store back and confirm the actor is usable again
	s.cluster.UnstallProvider(t, 0)

	// The stored value is exactly what it was before the failed call: a write that never landed left nothing behind
	require.Eventually(t, func() bool {
		e, rErr := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
		if rErr != nil {
			return false
		}
		return e.Decode(&out) == nil
	}, recoveryTimeout, recoveryInterval, "the actor should be readable again once the store is back")
	assert.Equal(t, int64(1), out.N, "a failed write must not move the stored value")

	// And the actor still works, so the failure did not leave it wedged
	env, err = s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	assert.Equal(t, int64(2), out.N, "the actor should accept work again after a failed write")
}
