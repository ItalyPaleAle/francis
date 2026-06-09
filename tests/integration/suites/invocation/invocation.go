//go:build integration

// Package invocation exercises invocation behaviors that depend on host and placement bookkeeping: the per-host limit on the number of active actors of a kind, and the turn-based concurrency that serializes calls to a single actor
package invocation

import (
	"sync"
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

// variants is the representative set the invocation scenarios run against
// Capacity enforcement lives in provider placement, so one variant per distinct placement implementation is enough: SQLite and Postgres each have their own SQL, and the standalone providers share one in-memory placement path that StandaloneMemory stands in for
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the capacity and turn-based scenarios across the representative variants on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&capacity{kind: k, variant: v})
			suite.Register(&turnBased{kind: k, variant: v})
		}
	}
}

// capacity verifies that a host refuses to activate more actors of a kind than its concurrency limit allows, and that halting an actor frees a slot
type capacity struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *capacity) Name() string {
	return "invocation-capacity/" + string(s.kind) + "/" + string(s.variant)
}

func (s *capacity) Setup(t *testing.T) []framework.Option {
	// A limit of two, with a long idle timeout so activated actors stay active for the duration of the test
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors: []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{
			IdleTimeout:      time.Minute,
			ConcurrencyLimit: 2,
		})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *capacity) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// Filling the host to its limit succeeds, since the only host has room for two actors of this kind
	_, err := svc.Invoke(ctx, shared.ProbeActorType, "cap-a", shared.ProbeMethodPing, nil)
	require.NoError(t, err)
	_, err = svc.Invoke(ctx, shared.ProbeActorType, "cap-b", shared.ProbeMethodPing, nil)
	require.NoError(t, err)

	// A third actor cannot be placed: the only host is full for this kind and there is nowhere else to put it
	_, err = svc.Invoke(ctx, shared.ProbeActorType, "cap-c", shared.ProbeMethodPing, nil)
	require.ErrorIs(t, err, actor.ErrNoHost)

	// Halting an active actor frees its slot, after which the previously rejected actor can be placed
	require.NoError(t, svc.Halt(shared.ProbeActorType, "cap-a"))

	// Deactivation propagates to the provider asynchronously, so retry until the freed slot is observable
	require.Eventually(t, func() bool {
		_, invErr := svc.Invoke(ctx, shared.ProbeActorType, "cap-c", shared.ProbeMethodPing, nil)
		return invErr == nil
	}, 15*time.Second, 200*time.Millisecond, "a slot freed by halting should allow a new actor to be placed")
}

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
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodHold, nil)
			}()
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
			}()
		}
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}

		var got shared.ProbeState
		require.NoError(t, svc.GetState(ctx, shared.ProbeActorType, actorID, &got))
		assert.Equal(t, int64(callers), got.N, "every increment should be reflected in the final state")
	})
}
