//go:build integration

// Package lifecycle exercises actor activation lifecycle: idle deactivation, the Deactivate hook, halting, and that state survives deactivation and reactivation
package lifecycle

import (
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

// idleTimeout is short so the actor becomes idle and deactivates quickly
const idleTimeout = time.Second

// variants is the representative set: deactivation is host-level, but state survival across it exercises each provider
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the lifecycle scenario across the representative variants on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&lifecycle{kind: k, variant: v})
		}
	}
}

// lifecycle runs one host with a short idle timeout and exercises deactivation and reactivation
type lifecycle struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *lifecycle) Name() string {
	return "lifecycle/" + string(s.kind) + "/" + string(s.variant)
}

func (s *lifecycle) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: idleTimeout})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *lifecycle) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// An idle actor is deactivated, its Deactivate hook runs, and its state survives so the next invocation resumes from where it left off
	t.Run("idle deactivation preserves state", func(t *testing.T) {
		const actorID = "idle-1"

		env, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		require.NoError(t, err)
		var out shared.ProbeState
		require.NoError(t, env.Decode(&out))
		require.Equal(t, int64(1), out.N)

		// The deactivation count is process-global and the actor ID repeats across scenarios, so measure against a baseline taken while the actor is freshly active
		before := shared.ProbeObserver.DeactivateCount(actorID)

		// Leaving the actor untouched past its idle timeout deactivates it, which runs the Deactivate hook
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.DeactivateCount(actorID) > before
		}, 15*time.Second, 200*time.Millisecond, "an idle actor should be deactivated")

		// Invoking again reactivates the actor, and the persisted state carries over so the counter continues
		env, err = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		require.NoError(t, err)
		require.NoError(t, env.Decode(&out))
		assert.Equal(t, int64(2), out.N, "state should survive deactivation")
	})

	// Halting an active actor deactivates it immediately, and it can be reactivated with its state intact
	t.Run("halt deactivates and reactivates", func(t *testing.T) {
		const actorID = "halt-1"

		env, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		require.NoError(t, err)
		var out shared.ProbeState
		require.NoError(t, env.Decode(&out))
		require.Equal(t, int64(1), out.N)

		before := shared.ProbeObserver.DeactivateCount(actorID)
		require.NoError(t, svc.Halt(shared.ProbeActorType, actorID))

		// Halting runs the Deactivate hook
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.DeactivateCount(actorID) > before
		}, 10*time.Second, 100*time.Millisecond, "halting should deactivate the actor")

		// The actor reactivates on the next invocation, resuming from its persisted state
		env, err = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		require.NoError(t, err)
		require.NoError(t, env.Decode(&out))
		assert.Equal(t, int64(2), out.N, "state should survive halting")
	})

	// Halting an actor that is not active on the host reports the public not-hosted error
	t.Run("halt of non-hosted actor errors", func(t *testing.T) {
		err := svc.Halt(shared.ProbeActorType, "never-active")
		require.ErrorIs(t, err, actor.ErrActorNotHosted)
	})
}
