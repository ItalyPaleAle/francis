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

	// Each distinct actor gets one holding invocation, all launched together
	// If the host serialized across actors, the global peak concurrency would be one
	shared.ProbeObserver.ResetGlobalConcurrency()

	var wg sync.WaitGroup
	errs := make([]error, actors)
	for i := range actors {
		wg.Go(func() {
			actorID := "parallel-" + strconv.Itoa(i)
			_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodHold, nil)
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	// Distinct actors must be able to run at the same time, so the peak crosses one
	assert.Greater(t, shared.ProbeObserver.MaxGlobalConcurrency(), 1, "invocations to distinct actors should run concurrently")
}
