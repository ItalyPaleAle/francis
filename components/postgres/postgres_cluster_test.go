package postgres

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/testutil"
)

// clusterTestConn creates a fresh test schema and returns a shared pool and clock for cluster-admission tests
func clusterTestConn(t *testing.T) (*pgxpool.Pool, *clocktesting.FakeClock) {
	t.Helper()

	connString := os.Getenv(connstringEnvVar)
	if connString == "" {
		t.Skip(`To run these tests, set the env var ` + connstringEnvVar + ` with the connection string for Postgres database. Example: "` + connstringEnvVar + `=postgres://actors:actors@localhost:5432/actors"`)
	}

	clock := clocktesting.NewFakeClock(time.Now().In(time.FixedZone("test-nonutc", -7*60*60)))
	schema := generateTestSchemaName(t)
	conn := connectTestDatabase(t, connString, schema, true)
	return conn, clock
}

// clusterTestProvider builds a provider against the shared pool with the given MaxHosts
// Passing the same pool and clock to two providers models two differently-configured hosts joining the same cluster
func clusterTestProvider(t *testing.T, conn *pgxpool.Pool, clock *clocktesting.FakeClock, maxHosts int) *PostgresProvider {
	t.Helper()

	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := PostgresProviderOptions{
		DB:              conn,
		CleanupInterval: -1,
		clock:           clock,
	}
	cfg := comptesting.GetProviderConfig()
	cfg.MaxHosts = maxHosts

	p, err := NewPostgresProvider(log, providerOpts, cfg)
	require.NoError(t, err, "Error creating provider")

	err = p.setCurrentFrozenTime()
	require.NoError(t, err, "Error setting frozen time")

	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	ctx := testutil.NewContextDoneNotifier(t.Context())
	go func() {
		_ = p.Run(ctx)
	}()
	ctx.WaitForDone()

	return p
}

func pgRegisterReq(address string) components.RegisterHostReq {
	return components.RegisterHostReq{
		Address:    address,
		ActorTypes: []components.ActorHostType{{ActorType: "test", IdleTimeout: time.Minute}},
	}
}

func TestPostgresMaxHosts(t *testing.T) {
	t.Run("enforces the limit", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 2)

		_, err := p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)
		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.3:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)
	})

	t.Run("frees a slot when a host expires", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 1)

		_, err := p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterFull)

		err = p.AdvanceClock(2 * comptesting.GetProviderConfig().HostHealthCheckDeadline)
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("unlimited when zero", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 0)

		for i, addr := range []string{"10.0.0.1:1000", "10.0.0.2:1000", "10.0.0.3:1000"} {
			_, err := p.RegisterHost(t.Context(), pgRegisterReq(addr))
			require.NoError(t, err, "host %d", i)
		}
	})
}

func TestPostgresMaxHostsMismatch(t *testing.T) {
	conn, clock := clusterTestConn(t)

	// The first host claims a limit of 1
	p1 := clusterTestProvider(t, conn, clock, 1)
	_, err := p1.RegisterHost(t.Context(), pgRegisterReq("10.0.0.1:1000"))
	require.NoError(t, err)

	// A second host configured with a different limit is rejected while the cluster is not empty
	p2 := clusterTestProvider(t, conn, clock, 2)
	_, err = p2.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
	require.ErrorIs(t, err, components.ErrMaxHostsMismatch)

	// Once the cluster is empty the new limit can be claimed
	err = p2.AdvanceClock(2 * comptesting.GetProviderConfig().HostHealthCheckDeadline)
	require.NoError(t, err)
	_, err = p2.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
	require.NoError(t, err)
}

func TestPostgresExclusiveLease(t *testing.T) {
	t.Run("fences registration and evicts hosts", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 0)

		res, err := p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.1:1000"))
		require.NoError(t, err)

		_, err = p.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
		require.ErrorIs(t, err, components.ErrClusterLocked)

		err = p.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		err = p.ReleaseExclusiveLease(t.Context(), "admin-1")
		require.NoError(t, err)

		err = p.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.2:1000"))
		require.NoError(t, err)
	})

	t.Run("held by another owner", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 0)

		_, err := p.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)

		_, err = p.AcquireExclusiveLease(t.Context(), "admin-2", 5*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)

		_, err = p.AcquireExclusiveLease(t.Context(), "admin-1", 5*time.Minute)
		require.NoError(t, err)
	})

	t.Run("expires without renewal", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 0)

		_, err := p.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)

		err = p.AdvanceClock(3 * time.Minute)
		require.NoError(t, err)

		_, err = p.AcquireExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.NoError(t, err)

		_, err = p.RegisterHost(t.Context(), pgRegisterReq("10.0.0.1:1000"))
		require.ErrorIs(t, err, components.ErrClusterLocked, "admin-2 now holds a live lease")
	})

	t.Run("renew keeps the lease and fails once lost", func(t *testing.T) {
		conn, clock := clusterTestConn(t)
		p := clusterTestProvider(t, conn, clock, 0)

		_, err := p.AcquireExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)

		exp, err := p.RenewExclusiveLease(t.Context(), "admin-1", 2*time.Minute)
		require.NoError(t, err)
		assert.False(t, exp.IsZero())

		_, err = p.RenewExclusiveLease(t.Context(), "admin-2", 2*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)
	})
}
