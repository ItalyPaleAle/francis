package comptesting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
)

// maxHostsSetter is implemented by test providers that allow overriding MaxHosts at runtime
// It lets the shared suite exercise the host-limit enforcement without constructing new providers
type maxHostsSetter interface {
	SetMaxHosts(n int)
}

// clusterRegisterReq builds a minimal RegisterHostReq for the cluster-admission tests
func clusterRegisterReq(address string) components.RegisterHostReq {
	return components.RegisterHostReq{
		Address:    address,
		ActorTypes: []components.ActorHostType{{ActorType: "TestActor", IdleTimeout: time.Minute}},
	}
}

// TestClusterAdmission exercises the host limit and exclusive-access lease
// It is skipped for providers that do not support cluster admission (the standalone providers)
func (s Suite) TestClusterAdmission(t *testing.T) {
	exclusive, ok := s.p.(components.ExclusiveController)
	if !ok {
		t.Skip("provider does not support exclusive-access leases")
	}
	maxHosts, ok := s.p.(maxHostsSetter)
	if !ok {
		t.Skip("provider does not support setting MaxHosts in tests")
	}

	// Restore the default (unlimited) so later suite tests are unaffected
	defer maxHosts.SetMaxHosts(0)

	t.Run("enforces the host limit", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		maxHosts.SetMaxHosts(2)

		_, err := s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)
		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)

		// The third host exceeds the limit
		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.3:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)
	})

	t.Run("frees a slot when a host expires", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		maxHosts.SetMaxHosts(1)

		_, err := s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)

		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)

		// Advance past the health check deadline so the first host is considered gone
		require.NoError(t, s.p.AdvanceClock(2*time.Minute))

		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("rejects a mismatched limit and reclaims once empty", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		// The first host claims a limit of 1
		maxHosts.SetMaxHosts(1)
		_, err := s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)

		// A host configured with a different limit is rejected while the cluster is not empty
		maxHosts.SetMaxHosts(2)
		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrMaxHostsMismatch)

		// Once the cluster is empty, the new limit can be claimed
		require.NoError(t, s.p.AdvanceClock(2*time.Minute))
		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("exclusive lease blocks registration and evicts hosts", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		maxHosts.SetMaxHosts(0)

		res, err := s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)

		_, err = exclusive.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)

		// New registrations are blocked
		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterLocked)

		// The existing host's health check now fails, so it will self-terminate
		err = s.p.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		// Releasing re-opens the cluster
		err = exclusive.ReleaseExclusiveLease(t.Context(), "admin-1")
		require.NoError(t, err)

		err = s.p.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)

		_, err = s.p.RegisterHost(t.Context(), clusterRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("exclusive lease held by another owner", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		_, err := exclusive.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)
		defer func() { _ = exclusive.ReleaseExclusiveLease(t.Context(), "admin-1") }()

		_, err = exclusive.AcquireExclusiveLease(t.Context(), "admin-2", 5*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)

		// The same owner can re-acquire (idempotent)
		_, err = exclusive.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)
	})

	t.Run("exclusive lease expires without renewal", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		_, err := exclusive.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)

		// After the TTL passes, a different owner can take it
		require.NoError(t, s.p.AdvanceClock(3*time.Minute))
		_, err = exclusive.AcquireExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.NoError(t, err)
		defer func() { _ = exclusive.ReleaseExclusiveLease(t.Context(), "admin-2") }()
	})

	t.Run("exclusive lease renew", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		_, err := exclusive.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)
		defer func() { _ = exclusive.ReleaseExclusiveLease(t.Context(), "admin-1") }()

		exp, err := exclusive.RenewExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)
		assert.False(t, exp.IsZero())

		// A different owner cannot renew
		_, err = exclusive.RenewExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)
	})
}
