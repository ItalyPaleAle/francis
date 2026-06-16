//go:build integration

package invocation

import (
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

// turnBased verifies that concurrent calls to one actor are serialized, so a single actor never runs two invocations at once and read-modify-write state stays consistent under contention
type turnBased struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *turnBased) Name() string {
	return "invocation-turnbased/" + string(s.kind) + "/" + string(s.variant)
}

func (s *turnBased) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *turnBased) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// Many overlapping calls to one actor must never run concurrently, so the observed peak concurrency stays at one
	t.Run("calls to one actor are serialized", func(t *testing.T) {
		const actorID = "turn-serialize"
		const callers = 8

		var wg sync.WaitGroup
		errs := make([]error, callers)
		for i := range callers {
			wg.Go(func() {
				_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodHold, nil)
			})
		}
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}

		assert.Equal(t, 1, shared.ProbeObserver.MaxHoldConcurrency(actorID), "an actor must process only one invocation at a time")
	})

	// Concurrent read-modify-write increments must not lose updates, since the turn-based lock serializes them
	t.Run("concurrent increments do not lose updates", func(t *testing.T) {
		const actorID = "turn-increment"
		const callers = 20

		var wg sync.WaitGroup
		errs := make([]error, callers)
		for i := range callers {
			wg.Go(func() {
				_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
			})
		}
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}

		var got shared.ProbeState
		err := svc.GetState(ctx, shared.ProbeActorType, actorID, &got)
		require.NoError(t, err)
		assert.Equal(t, int64(callers), got.N, "every increment should be reflected in the final state")
	})
}
