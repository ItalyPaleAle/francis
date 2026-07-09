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

// crossHostSerial verifies that turn-based serialization holds across hosts: an actor is placed on a single host, so concurrent calls from every host route there and never overlap
type crossHostSerial struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *crossHostSerial) Name() string {
	return "invocation-crosshost/" + string(s.kind) + "/" + string(s.variant)
}

func (s *crossHostSerial) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *crossHostSerial) Run(t *testing.T) {
	ctx := t.Context()

	// A holding call to the same actor is issued from both hosts at once, several times over
	// Because the actor lives on exactly one host, both hosts' calls converge on the same turn-based lock and must serialize
	t.Run("same actor from two hosts is serialized", func(t *testing.T) {
		const actorID = "crosshost-serialize"
		const rounds = 6

		var wg sync.WaitGroup
		errs := make([]error, rounds*2)
		for r := range rounds {
			for h := range 2 {
				wg.Go(func() {
					_, errs[r*2+h] = s.cluster.Service(h).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodHold, nil)
				})
			}
		}
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}

		assert.Equal(t, 1, shared.ProbeObserver.MaxHoldConcurrency(actorID), "an actor must process one invocation at a time even across hosts")
	})

	// Concurrent increments from both hosts must all land, proving the shared state stays consistent under cross-host contention
	t.Run("concurrent increments from two hosts are consistent", func(t *testing.T) {
		const actorID = "crosshost-increment"
		const perHost = 10

		var wg sync.WaitGroup
		errs := make([]error, perHost*2)
		for i := range perHost {
			for h := range 2 {
				wg.Go(func() {
					_, errs[i*2+h] = s.cluster.Service(h).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
				})
			}
		}
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}

		var got shared.ProbeState
		err := s.cluster.Service(0).GetState(ctx, shared.ProbeActorType, actorID, &got)
		require.NoError(t, err)
		assert.Equal(t, int64(perHost*2), got.N, "every increment from both hosts should be reflected in the final state")
	})
}
