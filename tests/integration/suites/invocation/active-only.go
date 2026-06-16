//go:build integration

package invocation

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
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// activeOnly verifies the WithInvokeActiveOnly option: an active-only invocation never activates the actor and reports ErrActorNotActive when it is not already active, while a normal invocation activates it so a later active-only call lands
type activeOnly struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *activeOnly) Name() string {
	return "invocation-activeonly/" + string(s.kind) + "/" + string(s.variant)
}

func (s *activeOnly) Setup(t *testing.T) []framework.Option {
	// A long idle timeout keeps the actor active once it has been activated, so the active-only call later in the scenario finds it
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

func (s *activeOnly) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()
	const actorID = "activeonly-1"

	// An active-only invocation of an actor that was never activated reports ErrActorNotActive
	_, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil, actor.WithInvokeActiveOnly())
	require.ErrorIs(t, err, actor.ErrActorNotActive, "an active-only invocation must not activate an inactive actor")

	// It also must not have run the actor: the increment never happened, so there is still no persisted state
	var got shared.ProbeState
	err = svc.GetState(ctx, shared.ProbeActorType, actorID, &got)
	require.ErrorIs(t, err, actor.ErrStateNotFound, "an active-only miss must have no side effects")

	// A normal invocation activates the actor and runs the increment
	env, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&got))
	require.Equal(t, int64(1), got.N)

	// Now that the actor is active, the active-only invocation lands and increments again
	env, err = svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil, actor.WithInvokeActiveOnly())
	require.NoError(t, err, "an active-only invocation should reach an already-active actor")
	require.NoError(t, env.Decode(&got))
	assert.Equal(t, int64(2), got.N)
}
