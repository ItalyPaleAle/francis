package clusteradmin

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

func newTestAdmin(t *testing.T) *Admin {
	t.Helper()

	admin, err := New(t.Context(),
		sqlite.SQLiteProviderOptions{ConnectionString: testutil.SQLiteConnString(t)},
		Options{
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
		Address: address,
		ActorTypes: []components.ActorHostType{
			{ActorType: "test", IdleTimeout: time.Minute},
		},
	}
}

func TestStandaloneProviderSupportsExclusive(t *testing.T) {
	admin, err := New(t.Context(), standalone.StandaloneMemoryOptions{}, Options{
		ExclusiveLeaseDuration: 2 * time.Second,
		ExclusiveRenewInterval: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = admin.Close()
	})

	// With no hosts connected, acquiring and releasing exclusive access works end to end
	_, err = admin.AcquireExclusive(t.Context(), AcquireOptions{
		Force: false,
	})
	require.NoError(t, err)

	err = admin.ReleaseExclusive(t.Context())
	require.NoError(t, err)
}

func TestAcquireForceFalseWithHosts(t *testing.T) {
	admin := newTestAdmin(t)

	res, err := admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// Without force, a connected host makes AcquireExclusive fail fast
	_, err = admin.AcquireExclusive(t.Context(), AcquireOptions{Force: false})
	require.ErrorIs(t, err, components.ErrHostsConnected)

	// The lease must have been released on the failure path, so removing the host and retrying succeeds
	err = admin.provider.UnregisterHost(t.Context(), res.HostID)
	require.NoError(t, err)

	_, err = admin.AcquireExclusive(t.Context(), AcquireOptions{Force: false})
	require.NoError(t, err)
}

func TestAcquireBlocksRegistrationAndReleases(t *testing.T) {
	admin := newTestAdmin(t)

	lost, err := admin.AcquireExclusive(t.Context(), AcquireOptions{Force: false})
	require.NoError(t, err)
	select {
	case <-lost:
		t.Fatal("lease should still be held")
	default:
	}

	// While the lease is held, registration is blocked
	_, err = admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.ErrorIs(t, err, components.ErrClusterLocked)

	// Releasing re-opens the cluster
	// The lost channel is not closed on a voluntary release
	err = admin.ReleaseExclusive(t.Context())
	require.NoError(t, err)
	select {
	case <-lost:
		t.Fatal("lost channel must not close on a voluntary release")
	default:
	}

	_, err = admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)
}

func TestAcquireSignalsLostLease(t *testing.T) {
	admin := newTestAdmin(t)

	lost, err := admin.AcquireExclusive(t.Context(), AcquireOptions{Force: false})
	require.NoError(t, err)

	// Simulate the lease being taken away out-of-band
	// The next renewal fails and closes the channel
	err = admin.provider.ReleaseExclusiveLease(t.Context(), admin.owner)
	require.NoError(t, err)

	select {
	case <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("lost channel was not closed after the lease was lost")
	}
}

func TestAcquireForceDrains(t *testing.T) {
	admin := newTestAdmin(t)

	res, err := admin.provider.RegisterHost(t.Context(), adminRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// Simulate the evicted host self-terminating shortly after
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = admin.provider.UnregisterHost(context.Background(), res.HostID)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	lost, err := admin.AcquireExclusive(ctx, AcquireOptions{Force: true})
	require.NoError(t, err)
	select {
	case <-lost:
		t.Fatal("lease should still be held after draining")
	default:
	}

	hosts, err := admin.provider.ListHosts(t.Context())
	require.NoError(t, err)
	assert.Empty(t, hosts)
}
