package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "embed"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/actors/components"
	comptesting "github.com/italypaleale/actors/components/testing"
	"github.com/italypaleale/actors/internal/ptr"
	"github.com/italypaleale/actors/internal/sql/transactions"
	"github.com/italypaleale/actors/internal/testutil"
)

const testConnectionString = "postgres://actors:actors@localhost:5432/actors"

//go:embed test-queries/test-setup.sql
var queryTestSetup string

// Uncomment this test to have a test database, including seed data, populated
// It is not removed automatically at the end of the test
func TestPostgresCreateTestDB(t *testing.T) {
	// Comment out to have the schem created
	t.SkipNow()

	p, testSchema, _ := initTestProvider(t)
	t.Log(`Session option query: SET SESSION search_path = "` + testSchema + `", pg_catalog, public`)

	// Close the database at the end, but do not cleanup the data
	t.Cleanup(func() {
		p.db.Close()
	})

	// Seed with the test data
	require.NoError(t, p.Seed(t.Context(), comptesting.GetSpec()))
}

func TestPostgresProvider(t *testing.T) {
	p, _, cleanupFn := initTestProvider(t)
	t.Cleanup(func() {
		cleanupFn()
		p.db.Close()
	})

	// Run the test suites
	suite := comptesting.NewSuite(p)
	t.Run("suite", suite.RunTests)
	t.Run("concurrency suite", suite.RunConcurrencyTests)
}

func initTestProvider(t *testing.T) (p *PostgresProvider, testSchema string, cleanupFn func()) {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	// Generate a random name for the schema
	testSchema = generateTestSchemaName(t)
	t.Log("Test schema:", testSchema)

	// Connect to the database beforehand so we can create a new schema for the tests
	conn, cleanupFnT := connectTestDatabase(t, testSchema)
	cleanupFn = func() { cleanupFnT(t) }

	providerOpts := PostgresProviderOptions{
		DB: conn,

		// Disable automated cleanups in this test
		// We will run the cleanups automatically
		CleanupInterval: -1,

		clock: clock,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	var err error
	p, err = NewPostgresProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Set the current frozen time in the database
	err = p.setCurrentFrozenTime()
	require.NoError(t, err, "Error setting current frozen time in the database")

	// Init the provider
	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background for side effects
	ctx := testutil.NewContextDoneNotifier(t.Context())
	go func() {
		err = p.Run(ctx)
		if err != nil {
			log.Error("Error running provider", slog.Any("error", err))
		}
	}()

	// Wait for Run to call <-ctx.Done()
	ctx.WaitForDone()

	return p, testSchema, cleanupFn
}

func (p *PostgresProvider) CleanupExpired() error {
	return p.gc.CleanupExpired()
}

func (p *PostgresProvider) Now() time.Time {
	return p.clock.Now()
}

func (p *PostgresProvider) AdvanceClock(d time.Duration) error {
	p.clock.Sleep(d)

	return p.setCurrentFrozenTime()
}

func (p *PostgresProvider) setCurrentFrozenTime() error {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.db.Exec(queryCtx,
		`SELECT freeze_time($1, false)`,
		p.clock.Now(),
	)
	if err != nil {
		return fmt.Errorf("error invoking freeze_time: %w", err)
	}
	return nil
}

func (p *PostgresProvider) Seed(ctx context.Context, spec comptesting.Spec) error {
	_, tErr := transactions.ExecuteInPgxTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (z struct{}, err error) {
		// We need to get the current time here because we cannot use "now()" when using CopyFrom
		// However, since time in the database is "frozen" and synced with the mock clock, it should be the same
		now := p.clock.Now()

		// Truncate all data
		for _, tbl := range []string{"active_actors", "host_actor_types", "hosts", "actor_state", "alarms"} {
			_, err = tx.Exec(ctx, "DELETE FROM "+tbl)
			if err != nil {
				return z, fmt.Errorf("truncate '%s': %w", tbl, err)
			}
		}

		// Hosts
		if len(spec.Hosts) > 0 {
			rows := make([][]any, len(spec.Hosts))
			for i, h := range spec.Hosts {
				rows[i] = []any{
					h.HostID,
					h.Address,
					now.Add(-1 * h.LastHealthAgo),
				}
			}
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"hosts"},
				[]string{"host_id", "host_address", "host_last_health_check"},
				pgx.CopyFromRows(rows),
			)
			if err != nil {
				return z, fmt.Errorf("copy hosts: %w", err)
			}
		}

		// Host actor types
		if len(spec.HostActorTypes) > 0 {
			rows := make([][]any, len(spec.HostActorTypes))
			for i, hat := range spec.HostActorTypes {
				rows[i] = []any{
					hat.HostID,
					hat.ActorType,
					hat.ActorIdleTimeout,
					hat.ActorConcurrencyLimit,
				}
			}
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"host_actor_types"},
				[]string{"host_id", "actor_type", "actor_idle_timeout", "actor_concurrency_limit"},
				pgx.CopyFromRows(rows),
			)
			if err != nil {
				return z, fmt.Errorf("copy host actor types: %w", err)
			}
		}

		// Active actors
		if len(spec.ActiveActors) > 0 {
			rows := make([][]any, len(spec.ActiveActors))
			for i, aa := range spec.ActiveActors {
				rows[i] = []any{
					aa.ActorType,
					aa.ActorID,
					aa.HostID,
					aa.ActorIdleTimeout,
					now.Add(-1 * aa.ActivationAgo),
				}
			}
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"active_actors"},
				[]string{"actor_type", "actor_id", "host_id", "actor_idle_timeout", "actor_activation"},
				pgx.CopyFromRows(rows),
			)
			if err != nil {
				return z, fmt.Errorf("copy active actors: %w", err)
			}
		}

		// Alarms
		if len(spec.Alarms) > 0 {
			rows := make([][]any, len(spec.Alarms))
			for i, a := range spec.Alarms {
				var (
					ttl      *time.Time
					interval *string
				)
				if a.Interval != "" {
					interval = ptr.Of(a.Interval)
				}
				if a.TTL > 0 {
					ttl = ptr.Of(now.Add(a.TTL))
				}

				var (
					leaseID  *string
					leaseExp *time.Time
				)
				if a.LeaseTTL != nil {
					leaseExp = ptr.Of(now.Add(*a.LeaseTTL))
					leaseID = ptr.Of(uuid.New().String())
				}

				rows[i] = []any{
					a.AlarmID, a.ActorType, a.ActorID, a.Name, now.Add(a.DueIn),
					interval, ttl, a.Data,
					leaseID, leaseExp,
				}
			}
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"alarms"},
				[]string{
					"alarm_id", "actor_type", "actor_id", "alarm_name", "alarm_due_time",
					"alarm_interval", "alarm_ttl_time", "alarm_data",
					"alarm_lease_id", "alarm_lease_expiration_time",
				},
				pgx.CopyFromRows(rows),
			)
			if err != nil {
				return z, fmt.Errorf("copy alarms: %w", err)
			}
		}

		return z, nil
	})

	return tErr
}

func (p *PostgresProvider) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	rows, err := p.db.Query(ctx, "SELECT actor_type, actor_id, actor_state_data FROM actor_state")
	if err != nil {
		return nil, fmt.Errorf("select actor_state: %w", err)
	}
	defer rows.Close()

	res := make(comptesting.ActorStateSpecCollection, 0)
	for rows.Next() {
		var r comptesting.ActorStateSpec
		err = rows.Scan(&r.ActorType, &r.ActorID, &r.Data)
		if err != nil {
			return nil, fmt.Errorf("reading actor_state row: %w", err)
		}
		res = append(res, r)
	}

	return res, nil
}

func (p *PostgresProvider) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	return transactions.ExecuteInPgxTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (res comptesting.Spec, err error) {
		// Load all hosts
		rows, err := tx.Query(ctx, "SELECT host_id, host_address, now() - host_last_health_check FROM hosts")
		if err != nil {
			return res, fmt.Errorf("select hosts: %w", err)
		}

		res.Hosts = make([]comptesting.HostSpec, 0)
		for rows.Next() {
			var r comptesting.HostSpec
			err = rows.Scan(&r.HostID, &r.Address, &r.LastHealthAgo)
			if err != nil {
				return res, fmt.Errorf("reading host row: %w", err)
			}
			res.Hosts = append(res.Hosts, r)
		}
		rows.Close()

		// Load all actor types
		rows, err = tx.Query(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM host_actor_types")
		if err != nil {
			return res, fmt.Errorf("select host_actor_types: %w", err)
		}

		res.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
		for rows.Next() {
			var r comptesting.HostActorTypeSpec
			err = rows.Scan(&r.HostID, &r.ActorType, &r.ActorIdleTimeout, &r.ActorConcurrencyLimit)
			if err != nil {
				return res, fmt.Errorf("reading host_actor_types row: %w", err)
			}
			res.HostActorTypes = append(res.HostActorTypes, r)
		}
		rows.Close()

		// Load all active actors
		rows, err = tx.Query(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, now() - actor_activation FROM active_actors")
		if err != nil {
			return res, fmt.Errorf("select active_actors: %w", err)
		}

		res.ActiveActors = make([]comptesting.ActiveActorSpec, 0)
		for rows.Next() {
			var r comptesting.ActiveActorSpec
			err = rows.Scan(&r.ActorType, &r.ActorID, &r.HostID, &r.ActorIdleTimeout, &r.ActivationAgo)
			if err != nil {
				return res, fmt.Errorf("reading active_actors row: %w", err)
			}
			res.ActiveActors = append(res.ActiveActors, r)
		}
		rows.Close()

		// Load all alarms
		rows, err = tx.Query(ctx, "SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time - now(), alarm_interval, alarm_ttl_time, alarm_data, alarm_lease_id, alarm_lease_expiration_time FROM alarms")
		if err != nil {
			return res, fmt.Errorf("select alarms: %w", err)
		}

		res.Alarms = make([]comptesting.AlarmSpec, 0)
		for rows.Next() {
			var (
				r        comptesting.AlarmSpec
				interval *string
				ttl      *time.Duration
			)
			err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.Name, &r.DueIn, &interval, &ttl, &r.Data, &r.LeaseID, &r.LeaseExp)
			if err != nil {
				return res, fmt.Errorf("reading alarms row: %w", err)
			}
			if interval != nil {
				r.Interval = *interval
			}
			if ttl != nil {
				r.TTL = *ttl
			}
			res.Alarms = append(res.Alarms, r)
		}
		rows.Close()

		return res, nil
	})
}

func TestHostGarbageCollection(t *testing.T) {
	p, _, cleanupFn := initTestProvider(t)
	t.Cleanup(func() {
		cleanupFn()
		p.db.Close()
	})

	t.Run("garbage collector removes expired hosts", func(t *testing.T) {
		// Register multiple hosts at different times
		host1Req := components.RegisterHostReq{
			Address: "192.168.1.1:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor1", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		host1Res, err := p.RegisterHost(t.Context(), host1Req)
		require.NoError(t, err)

		// Advance clock by 30 seconds
		p.AdvanceClock(30 * time.Second)

		host2Req := components.RegisterHostReq{
			Address: "192.168.1.2:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor2", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 3},
			},
		}
		host2Res, err := p.RegisterHost(t.Context(), host2Req)
		require.NoError(t, err)

		// Advance clock by another 20 seconds (so host3 is 20s after host2)
		p.AdvanceClock(20 * time.Second)

		host3Req := components.RegisterHostReq{
			Address: "192.168.1.3:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor3", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		host3Res, err := p.RegisterHost(t.Context(), host3Req)
		require.NoError(t, err)

		// Verify all hosts are initially present
		spec, err := p.GetAllHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 3, "All hosts should be present initially")
		require.Len(t, spec.HostActorTypes, 3, "All host actor types should be present initially")

		// Advance clock to make only the first host expired (beyond 1 minute health check deadline)
		// Current state: Host1 at 50s, Host2 at 20s, Host3 at 0s
		// Advance by 15 seconds: Host1 at 65s (expired), Host2 at 35s (healthy), Host3 at 15s (healthy)
		p.AdvanceClock(15 * time.Second)

		// Run garbage collection
		err = p.CleanupExpired()
		require.NoError(t, err)

		// Verify only host1 and its actor types are removed
		spec, err = p.GetAllHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 2, "Only expired host should be removed")
		require.Len(t, spec.HostActorTypes, 2, "Expired host's actor types should be removed")

		// Verify the correct hosts remain (host2 and host3)
		hostIDs := make([]string, len(spec.Hosts))
		for i, host := range spec.Hosts {
			hostIDs[i] = host.HostID
		}
		assert.Contains(t, hostIDs, host2Res.HostID, "Host2 should still be present")
		assert.Contains(t, hostIDs, host3Res.HostID, "Host3 should still be present")
		assert.NotContains(t, hostIDs, host1Res.HostID, "Host1 should be removed")

		// Verify the correct actor types remain
		actorTypes := make([]string, len(spec.HostActorTypes))
		for i, hat := range spec.HostActorTypes {
			actorTypes[i] = hat.ActorType
		}
		assert.Contains(t, actorTypes, "TestActor2", "TestActor2 should still be present")
		assert.Contains(t, actorTypes, "TestActor3", "TestActor3 should still be present")
		assert.NotContains(t, actorTypes, "TestActor1", "TestActor1 should be removed")

		// Advance clock to make host2 also expired
		// Current state: Host2 at 35s, Host3 at 15s
		// Advance by 30 seconds: Host2 at 65s (expired), Host3 at 45s (healthy)
		p.AdvanceClock(30 * time.Second)

		// Run garbage collection again
		err = p.CleanupExpired()
		require.NoError(t, err)

		// Verify only host3 remains
		spec, err = p.GetAllHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 1, "Only one host should remain")
		require.Len(t, spec.HostActorTypes, 1, "Only one host actor type should remain")

		// Verify host3 is the remaining one
		assert.Equal(t, host3Res.HostID, spec.Hosts[0].HostID, "Host3 should be the remaining host")
		assert.Equal(t, "TestActor3", spec.HostActorTypes[0].ActorType, "TestActor3 should be the remaining actor type")

		// Advance clock to make all hosts expired
		// Current state: Host3 at 45s
		// Advance by 20 seconds: Host3 at 65s (expired)
		p.AdvanceClock(20 * time.Second)

		// Run garbage collection one more time
		err = p.CleanupExpired()
		require.NoError(t, err)

		// Verify all hosts are removed
		spec, err = p.GetAllHosts(t.Context())
		require.NoError(t, err)
		assert.Len(t, spec.Hosts, 0, "All hosts should be removed")
		assert.Len(t, spec.HostActorTypes, 0, "All host actor types should be removed")
	})
}

func generateTestSchemaName(t *testing.T) string {
	t.Helper()

	testSchemaB := make([]byte, 5)
	_, err := io.ReadFull(rand.Reader, testSchemaB)
	require.NoError(t, err)
	return "test_" + hex.EncodeToString(testSchemaB)
}

func connectTestDatabase(t *testing.T, testSchema string) (conn *pgxpool.Pool, cleanupFn func(t *testing.T)) {
	t.Helper()

	// Parse the connection string
	cfg, err := pgxpool.ParseConfig(testConnectionString)
	require.NoError(t, err)

	// Set a callback so we can make sure that the schema exists after connecting, and setting the correct search path
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := c.Exec(queryCtx, `CREATE SCHEMA IF NOT EXISTS "`+testSchema+`"`)
		if err != nil {
			return fmt.Errorf("failed to ensure test schema '%s' exists: %w", testSchema, err)
		}

		queryCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = c.Exec(queryCtx, `SET SESSION search_path = "`+testSchema+`", pg_catalog, public`)
		if err != nil {
			return fmt.Errorf("failed to set search path for session: %w", err)
		}

		return nil
	}

	// Log notices from the database
	if cfg.ConnConfig == nil {
		cfg.ConnConfig = &pgx.ConnConfig{}
	}
	/*cfg.ConnConfig.OnNotice = func(pc *pgconn.PgConn, n *pgconn.Notice) {
		fmt.Println("PostgreSQL NOTICE:", n.Message)
	}*/

	// Connect to the database
	conn, err = pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err, "Failed to connect to database")

	// Execute the test setup queries
	queryCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = conn.Exec(queryCtx, queryTestSetup)
	require.NoError(t, err, "Failed to perform test setup")

	// Cleanup function that deletes the schema at the end of the tests
	cleanupFn = func(t *testing.T) {
		// Use a background context because t.Context() has been canceled already
		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := conn.Exec(queryCtx, `DROP SCHEMA "`+testSchema+`" CASCADE`)
		require.NoError(t, err, "Failed to drop test schema")
	}

	return conn, cleanupFn
}
