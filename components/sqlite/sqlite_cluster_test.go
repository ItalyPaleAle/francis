package sqlite

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/testutil"
)

// initTestProviderMaxHosts creates a provider against connString with a specific MaxHosts and a shared fake clock
func initTestProviderMaxHosts(t *testing.T, connString string, clock *clocktesting.FakeClock, maxHosts int) *SQLiteProvider {
	t.Helper()

	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := SQLiteProviderOptions{
		ConnectionString: connString,
		CleanupInterval:  -1,
		clock:            clock,
	}
	providerConfig := comptesting.GetProviderConfig()
	providerConfig.MaxHosts = maxHosts

	s, err := NewSQLiteProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	err = s.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	ctx := testutil.NewContextDoneNotifier(t.Context())
	go func() {
		_ = s.Run(ctx)
	}()
	ctx.WaitForDone()

	return s
}

func newClusterTestClock() *clocktesting.FakeClock {
	return clocktesting.NewFakeClock(time.Now().In(time.FixedZone("test-nonutc", -7*60*60)))
}

func registerReq(address string) components.RegisterHostReq {
	return components.RegisterHostReq{
		Address:    address,
		ActorTypes: []components.ActorHostType{{ActorType: "test", IdleTimeout: time.Minute}},
	}
}

func TestSQLiteMaxHosts(t *testing.T) {
	t.Run("enforces the limit", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 2)

		_, err := s.RegisterHost(t.Context(), registerReq("10.0.0.1:1000"))
		require.NoError(t, err)
		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
		require.NoError(t, err)

		// The third host exceeds the limit
		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.3:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)
	})

	t.Run("frees a slot when a host expires", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 1)

		_, err := s.RegisterHost(t.Context(), registerReq("10.0.0.1:1000"))
		require.NoError(t, err)

		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)

		// Advance past the health check deadline so the first host is considered gone
		clock.Step(2 * comptesting.GetProviderConfig().HostHealthCheckDeadline)

		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("unlimited when zero", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 0)

		for i, addr := range []string{"10.0.0.1:1000", "10.0.0.2:1000", "10.0.0.3:1000"} {
			_, err := s.RegisterHost(t.Context(), registerReq(addr))
			require.NoError(t, err, "host %d", i)
		}
	})
}

func TestSQLiteMaxHostsMismatch(t *testing.T) {
	clock := newClusterTestClock()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	// The first host claims a limit of 1
	s1 := initTestProviderMaxHosts(t, dbPath, clock, 1)
	_, err := s1.RegisterHost(t.Context(), registerReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// A second host configured with a different limit is rejected while the cluster is not empty
	s2 := initTestProviderMaxHosts(t, dbPath, clock, 2)
	_, err = s2.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
	require.ErrorIs(t, err, components.ErrMaxHostsMismatch)

	// Once the cluster is empty the new limit can be claimed
	clock.Step(2 * comptesting.GetProviderConfig().HostHealthCheckDeadline)
	_, err = s2.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
	require.NoError(t, err)
}

func TestSQLiteExclusiveLease(t *testing.T) {
	t.Run("fences registration and evicts hosts", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 0)

		res, err := s.RegisterHost(t.Context(), registerReq("10.0.0.1:1000"))
		require.NoError(t, err)

		// Take an exclusive lease
		_, err = s.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)

		// New registrations are fenced
		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterLocked)

		// The existing host's health check now fails, so it will self-terminate
		err = s.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		// Releasing re-opens the cluster
		err = s.ReleaseExclusiveLease(t.Context(), "admin-1")
		require.NoError(t, err)

		err = s.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)

		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("held by another owner", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 0)

		_, err := s.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)

		_, err = s.AcquireExclusiveLease(t.Context(), "admin-2", 5*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)

		// The same owner can re-acquire (idempotent)
		_, err = s.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)
	})

	t.Run("expires without renewal", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 0)

		_, err := s.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)

		// After the TTL passes, a different owner can take it
		clock.Step(3 * time.Minute)
		_, err = s.AcquireExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.NoError(t, err)

		// And registration works again
		_, err = s.RegisterHost(t.Context(), registerReq("10.0.0.1:1000"))
		require.ErrorIs(t, err, components.ErrClusterLocked, "admin-2 now holds a live lease")
	})

	t.Run("renew keeps the lease and fails once lost", func(t *testing.T) {
		clock := newClusterTestClock()
		s := initTestProviderMaxHosts(t, filepath.Join(t.TempDir(), "test.db"), clock, 0)

		_, err := s.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)

		exp, err := s.RenewExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)
		assert.False(t, exp.IsZero())

		// A different owner cannot renew
		_, err = s.RenewExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)
	})
}
