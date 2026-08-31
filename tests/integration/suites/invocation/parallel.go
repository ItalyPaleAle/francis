//go:build integration

package invocation

import (
	"strconv"
	"sync"
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

// parallel verifies that the turn-based lock is per-actor, not global: invocations to distinct actors on one host run concurrently
type parallel struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *parallel) Name() string {
	return "invocation-parallel/" + string(s.kind) + "/" + string(s.variant)
}

func (s *parallel) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *parallel) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	const actors = 8

	// Arm every actor before launching the calls so the first arrivals remain blocked while slower actors finish activating
	actorIDs := make([]string, actors)
	for i := range actors {
		actorIDs[i] = "parallel-" + strconv.Itoa(i)
		shared.ProbeObserver.ArmBlock(actorIDs[i])
	}
	defer func() {
		for _, actorID := range actorIDs {
			shared.ProbeObserver.ReleaseBlock(actorID)
		}
	}()

	// Launch one invocation per actor and retain each result until the barrier is released
	var wg sync.WaitGroup
	errs := make([]error, actors)
	for i, actorID := range actorIDs {
		wg.Go(func() {
			_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodBlock, nil)
		})
	}

	// Every distinct actor must enter while the others remain blocked, proving their turn locks are independent without relying on scheduler timing
	assert.Eventually(t, func() bool {
		for _, actorID := range actorIDs {
			if shared.ProbeObserver.BlockEnteredCount(actorID) == 0 {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond, "invocations to distinct actors should run concurrently")

	// Release every invocation before waiting so a failed overlap assertion cannot wedge test cleanup
	for _, actorID := range actorIDs {
		shared.ProbeObserver.ReleaseBlock(actorID)
	}
	wg.Wait()

	// Verify that synchronization did not hide any invocation errors
	for _, err := range errs {
		require.NoError(t, err)
	}
}
