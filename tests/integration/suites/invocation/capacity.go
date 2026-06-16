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
	err = svc.Halt(shared.ProbeActorType, "cap-a")
	require.NoError(t, err)

	// Deactivation propagates to the provider asynchronously, so retry until the freed slot is observable
	require.Eventually(t, func() bool {
		_, rErr := svc.Invoke(ctx, shared.ProbeActorType, "cap-c", shared.ProbeMethodPing, nil)
		return rErr == nil
	}, 15*time.Second, 200*time.Millisecond, "a slot freed by halting should allow a new actor to be placed")
}
