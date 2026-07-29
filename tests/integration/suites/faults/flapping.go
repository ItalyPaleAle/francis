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

const (
	// flapCycles is how many times the link is cut and restored
	// Enough to show the reconnect settling repeatedly rather than degrading, without turning the scenario into an endurance test
	flapCycles = 5
	// flapOutage is how long each cut lasts
	// It is longer than a health check attempt, so the session genuinely drops and has to be re-established, and short enough that the runtime is unlikely to have written the host off in between
	flapOutage = 2 * time.Second
)

// linkFlapping cuts and restores a host's link to the runtime over and over, and verifies the cluster comes all the way back each time rather than degrading as the cycles add up
//
// The partition scenario covers a single outage, which a reconnect loop can survive by luck: the interesting failure mode is the one that only appears after several, where backoff grows without bound, a superseded session is left behind, or each reconnect strands another copy of the actors
// Counting invocations across the cycles is what catches the last of those, since a second live instance would make the total disagree
type linkFlapping struct {
	cluster *cluster.Cluster
}

func (s *linkFlapping) Name() string {
	return "faults-link-flapping/remote/sqlite"
}

func (s *linkFlapping) Setup(t *testing.T) []framework.Option {
	// Each host reaches the runtime through its own link, so only the flapped host is affected
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

func (s *linkFlapping) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-flapping-1"

	labelHosts(s.cluster)

	// Establish the actor and a known counter, which every cycle then adds exactly one to
	env, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	require.NotEmpty(t, placed)
	flapped := 0
	if placed != "h0" {
		flapped = 1
	}
	survivor := (flapped + 1) % s.cluster.Len()

	link := s.cluster.RuntimeLink(t, flapped)

	for cycle := 1; cycle <= flapCycles; cycle++ {
		// Cut the host off from the control plane for long enough that the session has to be rebuilt
		link.Sever(t)
		time.Sleep(flapOutage)
		link.Restore(t)

		// Recovery is proven with a write rather than a read, because a read is answered from the actor's own cached state and would succeed while its host was still cut off
		// Retrying is safe here: a write that cannot reach the control plane persists nothing, so only the attempt that reports success has moved the counter
		require.Eventuallyf(t, func() bool {
			e, rErr := s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
			if rErr != nil {
				return false
			}
			return e.Decode(&out) == nil
		}, recoveryTimeout, recoveryInterval, "the cluster should accept work again after flap %d", cycle)

		// Exactly one increment landed per cycle, so the counter is a running tally of the calls that were actually applied
		require.Equalf(t, int64(1+cycle), out.N, "the counter should have advanced by exactly one over flap %d", cycle)
	}

	// After every cycle the total matches the calls that were made, so no flap ever left a second instance answering alongside the first
	env, err = s.cluster.Service(survivor).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodGet, nil)
	require.NoError(t, err)
	err = env.Decode(&out)
	require.NoError(t, err)
	assert.Equal(t, int64(1+flapCycles), out.N, "the counter should account for exactly the increments that were issued")

	// The flapped host is a working member again rather than something the repeated resets left behind
	require.Eventually(t, func() bool {
		_, rErr := s.cluster.Service(flapped).Invoke(ctx, shared.ProbeActorType, "faults-flapping-2", shared.ProbeMethodPing, nil)
		return rErr == nil
	}, recoveryTimeout, recoveryInterval, "the flapped host should still be able to place and serve actors")
}
