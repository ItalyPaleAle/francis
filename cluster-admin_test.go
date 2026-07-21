package francis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/testutil"
)

func newTestAdmin(t *testing.T) *ClusterAdmin {
	t.Helper()

	admin, err := NewClusterAdmin(t.Context(),
		sqlite.SQLiteProviderOptions{ConnectionString: testutil.SQLiteConnString(t)},
		ClusterAdminOptions{
			// Short lease so the test does not depend on the long defaults
			ExclusiveLeaseDuration: 2 * time.Second,
			ExclusiveRenewInterval: time.Second,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { 
		_ = admin.Close()
	})
	return admin
}

func adminRegisterReq(address string) components.RegisterHostReq {
	return components.RegisterHostReq{
		Address:    address,
		ActorTypes: []components.ActorHostType{{ActorType: "test", IdleTimeout: time.Minute}},
	}
}

func TestClusterAdminExclusiveNotSupported(t *testing.T) {
	_, err := NewClusterAdmin(t.Context(), standalone.StandaloneMemoryOptions{}, ClusterAdminOptions{})
	require.ErrorIs(t, err, components.ErrExclusiveNotSupported)
}

func TestClusterAdminForceFalseWithHosts(t *testing.T) {
	admin := newTestAdmin(t)

	res, err := admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// Without force, a connected host makes AcquireExclusive fail fast
	_, err = admin.AcquireExclusive(t.Context(), ExclusiveOpts{Force: false})
	require.ErrorIs(t, err, components.ErrHostsConnected)

	// The lease must have been released on the failure path, so removing the host and retrying succeeds
	err = admin.provider.UnregisterHost(t.Context(), res.HostID)
	require.NoError(t, err)

	_, err = admin.AcquireExclusive(t.Context(), ExclusiveOpts{Force: false})
	require.NoError(t, err)
}

func TestClusterAdminAcquireFencesAndReleases(t *testing.T) {
	admin := newTestAdmin(t)

	leaseCtx, err := admin.AcquireExclusive(t.Context(), ExclusiveOpts{Force: false})
	require.NoError(t, err)
	require.NoError(t, leaseCtx.Err(), "lease context should be live")

	// While the lease is held, registration is fenced
	_, err = admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.ErrorIs(t, err, components.ErrClusterLocked)

	// Releasing re-opens the cluster and cancels the lease context
	err = admin.ReleaseExclusive(t.Context())
	require.NoError(t, err)

	select {
	case <-leaseCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("lease context was not canceled after release")
	}

	_, err = admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)
}

func TestClusterAdminForceDrains(t *testing.T) {
	admin := newTestAdmin(t)

	res, err := admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// Simulate the fenced host self-terminating shortly after
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = admin.provider.UnregisterHost(context.Background(), res.HostID)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	leaseCtx, err := admin.AcquireExclusive(ctx, ExclusiveOpts{Force: true})
	require.NoError(t, err)
	assert.NoError(t, leaseCtx.Err())

	hosts, err := admin.provider.ListHosts(t.Context())
	require.NoError(t, err)
	assert.Empty(t, hosts)
}
