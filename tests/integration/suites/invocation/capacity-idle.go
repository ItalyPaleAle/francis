//go:build integration

package invocation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// capacityIdle verifies that a capacity slot freed by idle deactivation, rather than an explicit Halt, also lets a previously rejected actor be placed
type capacityIdle struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *capacityIdle) Name() string {
	return "invocation-capacity-idle/" + string(s.kind) + "/" + string(s.variant)
}

// idleTimeout is short so the active actor deactivates on its own within the scenario, but long enough that the full-host assertion is observed before it does
const idleTimeout = 2 * time.Second

func (s *capacityIdle) Setup(t *testing.T) []framework.Option {
	// A limit of one, so a single active actor fills the host for this kind
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(idleTimeout), actorcore.WithConcurrencyLimit(1))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *capacityIdle) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()
	const activeID = "capidle-a"
	const waitingID = "capidle-b"

	// Place one actor, filling the host's only slot for this kind
	_, err := svc.Invoke(ctx, shared.ProbeActorType, activeID, shared.ProbeMethodPing, nil)
	require.NoError(t, err)

	// A second actor cannot be placed while the first holds the only slot
	before := shared.ProbeObserver.DeactivateCount(activeID)
	_, err = svc.Invoke(ctx, shared.ProbeActorType, waitingID, shared.ProbeMethodPing, nil)
	require.ErrorIs(t, err, actor.ErrNoHost, "the host should be full while the first actor is active")

	// Leaving the first actor untouched past its idle timeout deactivates it, which frees the slot without any explicit Halt
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.DeactivateCount(activeID) > before
	}, 15*time.Second, 200*time.Millisecond, "the active actor should be idle-deactivated")

	// With the slot freed by idle deactivation, the previously rejected actor can now be placed
	// Deactivation propagates to the provider asynchronously, so retry until the freed slot is observable
	require.Eventually(t, func() bool {
		_, rErr := svc.Invoke(ctx, shared.ProbeActorType, waitingID, shared.ProbeMethodPing, nil)
		return rErr == nil
	}, 15*time.Second, 200*time.Millisecond, "a slot freed by idle deactivation should allow a new actor to be placed")
}
