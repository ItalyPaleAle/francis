package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/testutil"
)

// Runs the full test suite with a fast, in-memory database
func TestSQLiteProviderInMemory(t *testing.T) {
	// Connect to an in-memory database
	s := initTestProvider(t, "file:dbtest?mode=memory")

	// Run the test suites
	suite := comptesting.NewSuite(s)
	t.Run("suite", suite.RunTests)
	t.Run("concurrency suite", suite.RunConcurrencyTests)
}

// Runs the full test suite with an on-disk database
// For SQLite, this makes a difference because of locking
func TestSQLiteProviderDisk(t *testing.T) {
	// Get a temporary file
	dbPath := filepath.Join(t.TempDir(), "sqlitetest.db")

	// Increase the timeout since the concurrent tests can take some time in a locked state
	s := initTestProvider(t, "file:"+dbPath+"?_pragma=busy_timeout(15000)")
	s.timeout = 20 * time.Second

	// Run the test suite
	suite := comptesting.NewSuite(s)
	t.Run("suite", suite.RunTests)
	t.Run("concurrency suite", suite.RunConcurrencyTests)
}

func initTestProvider(t *testing.T, connString string) (p *SQLiteProvider) {
	return initTestProviderWithPrefix(t, connString, "")
}

func initTestProviderWithPrefix(t *testing.T, connString string, tablePrefix string) (p *SQLiteProvider) {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := SQLiteProviderOptions{
		ConnectionString: connString,
		TablePrefix:      tablePrefix,

		// Disable automated cleanups in this test
		// We will run the cleanups automatically
		CleanupInterval: -1,

		clock: clock,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	s, err := NewSQLiteProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = s.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background for side effects
	ctx := testutil.NewContextDoneNotifier(t.Context())
	go func() {
		err = s.Run(ctx)
		if err != nil {
			log.Error("Error running provider", slog.Any("error", err))
		}
	}()

	// Wait for Run to call <-ctx.Done()
	ctx.WaitForDone()

	return s
}

func (s *SQLiteProvider) CleanupExpired(_ context.Context) error {
	return s.gc.CleanupExpired()
}

func (s *SQLiteProvider) Now() time.Time {
	return s.clock.Now()
}

func (s *SQLiteProvider) AdvanceClock(d time.Duration) error {
	s.clock.Sleep(d)
	return nil
}

func (s *SQLiteProvider) Seed(ctx context.Context, spec comptesting.Spec) error {
	_, tErr := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (z struct{}, err error) {
		now := s.clock.Now()

		// Truncate all data
		for _, tbl := range []string{"active_actors", "host_actor_types", "hosts", "actor_state", "alarms"} {
			// Disable the "G202: SQL string concatenation" gosec warning, since there's no risk of SQL injection here
			// #nosec G202
			_, err = tx.ExecContext(ctx, "DELETE FROM "+s.tablePrefix+tbl)
			if err != nil {
				return z, fmt.Errorf("truncate '%s': %w", tbl, err)
			}
		}

		// Hosts
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		insHost, err := tx.PrepareContext(ctx,
			`INSERT INTO `+s.tablePrefix+`hosts (host_id, host_address, host_last_health_check)
			VALUES (?, ?, ?)`,
		)
		if err != nil {
			return z, fmt.Errorf("prep hosts: %w", err)
		}
		defer insHost.Close()
		for _, h := range spec.Hosts {
			hb := now.UnixMilli() - h.LastHealthAgo.Milliseconds()
			_, err = insHost.ExecContext(ctx, h.HostID, h.Address, hb)
			if err != nil {
				return z, fmt.Errorf("insert host '%s': %w", h.HostID, err)
			}
		}
		_ = insHost.Close()

		// Host actor types
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		insHat, err := tx.PrepareContext(ctx,
			`INSERT INTO `+s.tablePrefix+`host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit)
			VALUES (?, ?, ?, ?)`,
		)
		if err != nil {
			return z, fmt.Errorf("prep host_actor_types: %w", err)
		}
		defer insHat.Close()
		for _, hat := range spec.HostActorTypes {
			_, err = insHat.ExecContext(ctx,
				hat.HostID,
				hat.ActorType,
				hat.ActorIdleTimeout.Milliseconds(),
				hat.ActorConcurrencyLimit,
			)
			if err != nil {
				return z, fmt.Errorf("insert host_actor_type '%s/%s': %w", hat.HostID, hat.ActorType, err)
			}
		}
		_ = insHat.Close()

		// Active actors
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		insAA, err := tx.PrepareContext(ctx,
			`INSERT INTO `+s.tablePrefix+`active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
			VALUES (?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return z, fmt.Errorf("prep active_actors: %w", err)
		}
		defer insAA.Close()
		for _, aa := range spec.ActiveActors {
			act := now.UnixMilli() - aa.ActivationAgo.Milliseconds()
			_, err = insAA.ExecContext(ctx,
				aa.ActorType, aa.ActorID, aa.HostID,
				aa.ActorIdleTimeout.Milliseconds(), act,
			)
			if err != nil {
				return z, fmt.Errorf("insert active_actor '%s/%s': %w", aa.ActorType, aa.ActorID, err)
			}
		}
		_ = insAA.Close()

		// Alarms
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		insAlarm, err := tx.PrepareContext(ctx,
			`INSERT INTO `+s.tablePrefix+`alarms (
				alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
				alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_expiration_time
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return z, fmt.Errorf("prep alarms: %w", err)
		}
		defer insAlarm.Close()
		for _, a := range spec.Alarms {
			due := now.UnixMilli() + a.DueIn.Milliseconds()

			var (
				ttl      *int64
				interval *string
			)
			if a.Interval != "" {
				interval = &a.Interval
			}
			if a.TTL > 0 {
				ttl = new(now.UnixMilli() + a.TTL.Milliseconds())
			}

			var (
				leaseID  *string
				leaseExp *int64
			)
			if a.LeaseTTL != nil {
				leaseExp = new(now.UnixMilli() + a.LeaseTTL.Milliseconds())
				leaseID = new(uuid.New().String())
			}

			_, err = insAlarm.ExecContext(ctx,
				a.AlarmID, a.ActorType, a.ActorID, a.Name, due,
				interval, ttl, a.Data, leaseID, leaseExp,
			)
			if err != nil {
				return z, fmt.Errorf("insert alarm '%s': %w", a.AlarmID, err)
			}
		}
		_ = insAlarm.Close()

		return z, nil
	})

	return tErr
}

func (s *SQLiteProvider) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.QueryContext(ctx, "SELECT actor_type, actor_id, actor_state_data FROM "+s.tablePrefix+"actor_state")
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

func (s *SQLiteProvider) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	return sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (res comptesting.Spec, err error) {
		// Load all hosts
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		rows, err := tx.QueryContext(ctx, "SELECT host_id, host_address, host_last_health_check FROM "+s.tablePrefix+"hosts")
		if err != nil {
			return res, fmt.Errorf("select hosts: %w", err)
		}

		res.Hosts = make([]comptesting.HostSpec, 0)
		for rows.Next() {
			var (
				r               comptesting.HostSpec
				lastHealthCheck int64
			)
			err = rows.Scan(&r.HostID, &r.Address, &lastHealthCheck)
			if err != nil {
				return res, fmt.Errorf("reading host row: %w", err)
			}
			r.LastHealthAgo = s.clock.Since(time.UnixMilli(lastHealthCheck))
			res.Hosts = append(res.Hosts, r)
		}
		rows.Close() //nolint:sqlclosecheck

		// Load all actor types
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		rows, err = tx.QueryContext(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM "+s.tablePrefix+"host_actor_types")
		if err != nil {
			return res, fmt.Errorf("select host_actor_types: %w", err)
		}

		res.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
		for rows.Next() {
			var (
				r           comptesting.HostActorTypeSpec
				idleTimeout int64
			)
			err = rows.Scan(&r.HostID, &r.ActorType, &idleTimeout, &r.ActorConcurrencyLimit)
			if err != nil {
				return res, fmt.Errorf("reading host_actor_types row: %w", err)
			}
			r.ActorIdleTimeout = time.Duration(idleTimeout) * time.Millisecond
			res.HostActorTypes = append(res.HostActorTypes, r)
		}
		rows.Close() //nolint:sqlclosecheck

		// Load all active actors
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		rows, err = tx.QueryContext(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM "+s.tablePrefix+"active_actors")
		if err != nil {
			return res, fmt.Errorf("select active_actors: %w", err)
		}

		res.ActiveActors = make([]comptesting.ActiveActorSpec, 0)
		for rows.Next() {
			var (
				r                       comptesting.ActiveActorSpec
				idleTimeout, activation int64
			)
			err = rows.Scan(&r.ActorType, &r.ActorID, &r.HostID, &idleTimeout, &activation)
			if err != nil {
				return res, fmt.Errorf("reading active_actors row: %w", err)
			}
			r.ActorIdleTimeout = time.Duration(idleTimeout) * time.Millisecond
			r.ActivationAgo = s.clock.Since(time.UnixMilli(activation))
			res.ActiveActors = append(res.ActiveActors, r)
		}
		rows.Close() //nolint:sqlclosecheck

		// Load all alarms
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		rows, err = tx.QueryContext(ctx, "SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data, alarm_lease_id, alarm_lease_expiration_time FROM "+s.tablePrefix+"alarms")
		if err != nil {
			return res, fmt.Errorf("select alarms: %w", err)
		}

		res.Alarms = make([]comptesting.AlarmSpec, 0)
		for rows.Next() {
			var (
				r             comptesting.AlarmSpec
				due           int64
				interval      *string
				ttl, leaseExp *int64
			)
			err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.Name, &due, &interval, &ttl, &r.Data, &r.LeaseID, &leaseExp)
			if err != nil {
				return res, fmt.Errorf("reading active_actors row: %w", err)
			}
			r.DueIn = time.UnixMilli(due).Sub(s.clock.Now())
			if interval != nil {
				r.Interval = *interval
			}
			if ttl != nil {
				r.TTL = time.Duration(*ttl) * time.Millisecond
			}
			if leaseExp != nil {
				r.LeaseExp = new(time.UnixMilli(*leaseExp))
			}
			res.Alarms = append(res.Alarms, r)
		}
		rows.Close() //nolint:sqlclosecheck

		return res, nil
	})
}

func TestHostGarbageCollection(t *testing.T) {
	// Connect to an in-memory database, but note it has a different name form the previous
	s := initTestProvider(t, "file:gctest?mode=memory")

	t.Run("garbage collector removes expired hosts", func(t *testing.T) {
		// Register multiple hosts at different times
		host1Req := components.RegisterHostReq{
			Address: "192.168.1.1:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor1", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		host1Res, err := s.RegisterHost(t.Context(), host1Req)
		require.NoError(t, err)

		// Advance clock by 30 seconds
		_ = s.AdvanceClock(30 * time.Second) //nolint:errcheck

		host2Req := components.RegisterHostReq{
			Address: "192.168.1.2:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor2", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 3},
			},
		}
		host2Res, err := s.RegisterHost(t.Context(), host2Req)
		require.NoError(t, err)

		// Advance clock by another 20 seconds (so host3 is 20s after host2)
		_ = s.AdvanceClock(20 * time.Second) //nolint:errcheck

		host3Req := components.RegisterHostReq{
			Address: "192.168.1.3:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor3", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		host3Res, err := s.RegisterHost(t.Context(), host3Req)
		require.NoError(t, err)

		// Verify all hosts are initially present
		spec, err := s.GetAllHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 3, "All hosts should be present initially")
		require.Len(t, spec.HostActorTypes, 3, "All host actor types should be present initially")

		// Advance clock to make only the first host expired (beyond 1 minute health check deadline)
		// Current state: Host1 at 50s, Host2 at 20s, Host3 at 0s
		// Advance by 15 seconds: Host1 at 65s (expired), Host2 at 35s (healthy), Host3 at 15s (healthy)
		_ = s.AdvanceClock(15 * time.Second) //nolint:errcheck

		// Run garbage collection
		err = s.CleanupExpired(t.Context())
		require.NoError(t, err)

		// Verify only host1 and its actor types are removed
		spec, err = s.GetAllHosts(t.Context())
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
		_ = s.AdvanceClock(30 * time.Second) //nolint:errcheck

		// Run garbage collection again
		err = s.CleanupExpired(t.Context())
		require.NoError(t, err)

		// Verify only host3 remains
		spec, err = s.GetAllHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 1, "Only one host should remain")
		require.Len(t, spec.HostActorTypes, 1, "Only one host actor type should remain")

		// Verify host3 is the remaining one
		assert.Equal(t, host3Res.HostID, spec.Hosts[0].HostID, "Host3 should be the remaining host")
		assert.Equal(t, "TestActor3", spec.HostActorTypes[0].ActorType, "TestActor3 should be the remaining actor type")

		// Advance clock to make all hosts expired
		// Current state: Host3 at 45s
		// Advance by 20 seconds: Host3 at 65s (expired)
		_ = s.AdvanceClock(20 * time.Second) //nolint:errcheck

		// Run garbage collection one more time
		err = s.CleanupExpired(t.Context())
		require.NoError(t, err)

		// Verify all hosts are removed
		spec, err = s.GetAllHosts(t.Context())
		require.NoError(t, err)
		assert.Empty(t, spec.Hosts, "All hosts should be removed")
		assert.Empty(t, spec.HostActorTypes, "All host actor types should be removed")
	})
}

func TestTablePrefix(t *testing.T) {
	// schemaObjects returns the names of all tables and views in the database
	schemaObjects := func(t *testing.T, s *SQLiteProvider) []string {
		t.Helper()
		rows, err := s.db.QueryContext(t.Context(), "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		return names
	}

	t.Run("default prefix is francis", func(t *testing.T) {
		s := initTestProvider(t, "file:prefixdefault?mode=memory")
		assert.Equal(t, "francis_", s.tablePrefix)

		names := schemaObjects(t, s)
		assert.Contains(t, names, "francis_hosts")
		assert.Contains(t, names, "francis_alarms")
		assert.Contains(t, names, "francis_dead_jobs")
		assert.Contains(t, names, "francis_host_active_actor_count")
		assert.Contains(t, names, "francis_metadata")
		// No object should exist under its bare, unprefixed name
		assert.NotContains(t, names, "hosts")
		assert.NotContains(t, names, "alarms")
	})

	t.Run("custom prefix", func(t *testing.T) {
		s := initTestProviderWithPrefix(t, "file:prefixcustom?mode=memory", "myapp")
		assert.Equal(t, "myapp_", s.tablePrefix)

		names := schemaObjects(t, s)
		for _, name := range names {
			assert.Truef(t, strings.HasPrefix(name, "myapp_"), "schema object %q is not prefixed", name)
		}
		assert.Contains(t, names, "myapp_hosts")
		assert.Contains(t, names, "myapp_alarms")
		assert.Contains(t, names, "myapp_metadata")
	})

	t.Run("custom prefix is functional end-to-end", func(t *testing.T) {
		s := initTestProviderWithPrefix(t, "file:prefixe2e?mode=memory", "myapp")

		// Register a host and set an alarm, then read them back through the regular API
		hostRes, err := s.RegisterHost(t.Context(), components.RegisterHostReq{
			Address: "10.0.0.1:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, hostRes.HostID)

		hosts, err := s.ListHosts(t.Context())
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		assert.Equal(t, "10.0.0.1:8080", hosts[0].Address)
	})
}

func TestGetInPlaceholders(t *testing.T) {
	t.Run("empty vals", func(t *testing.T) {
		vals := []string{}
		args := make([]any, 0)
		placeholders := getInPlaceholders(vals, args, 0)
		assert.Empty(t, placeholders)
		assert.Empty(t, args)
	})

	t.Run("args empty", func(t *testing.T) {
		vals := []string{"a", "b"}
		args := make([]any, 2)
		placeholders := getInPlaceholders(vals, args, 0)
		assert.Equal(t, "?,?", placeholders)
		assert.Equal(t, []any{"a", "b"}, args)
	})

	t.Run("append to args", func(t *testing.T) {
		vals := []string{"a", "b"}
		args := make([]any, 4)
		args[0] = "x"
		args[1] = "y"
		placeholders := getInPlaceholders(vals, args, 2)
		assert.Equal(t, "?,?", placeholders)
		assert.Equal(t, []any{"x", "y", "a", "b"}, args)
	})
}

func TestActiveHostsList_HostForActorType(t *testing.T) {
	t.Run("pick hosts at random", func(t *testing.T) {
		h1 := &activeHost{HostID: comptesting.SpecHostH1, ActorType: "typeA", Capacity: 10000}
		h2 := &activeHost{HostID: comptesting.SpecHostH2, ActorType: "typeA", Capacity: 10000}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				comptesting.SpecHostH1: h1,
				comptesting.SpecHostH2: h2,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1, h2},
			},
		}

		observed := map[string]int{}
		for range 100 {
			host := ahl.HostForActorType("typeA")
			require.NotNil(t, host)
			observed[host.HostID]++
		}

		// Should be roughly 50/50, but we enforce at least 30 to leave some buffer for randomness
		assert.Len(t, observed, 2)
		assert.GreaterOrEqual(t, observed[comptesting.SpecHostH1], 30)
		assert.GreaterOrEqual(t, observed[comptesting.SpecHostH2], 30)
	})

	t.Run("single host capacity exhaustion", func(t *testing.T) {
		h1 := &activeHost{HostID: comptesting.SpecHostH1, ActorType: "typeA", Capacity: 3}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				comptesting.SpecHostH1: h1,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1},
			},
		}

		// First three calls should return the host ID
		// Fourth should be empty
		for range 3 {
			host := ahl.HostForActorType("typeA")
			require.NotNil(t, host)
			assert.Equal(t, comptesting.SpecHostH1, host.HostID)
		}
		assert.Nil(t, ahl.HostForActorType("typeA"))
	})

	t.Run("multiple hosts capacity exhaustion", func(t *testing.T) {
		h1 := &activeHost{HostID: comptesting.SpecHostH1, ActorType: "typeA", Capacity: 5}
		h2 := &activeHost{HostID: comptesting.SpecHostH2, ActorType: "typeA", Capacity: 10000}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				comptesting.SpecHostH1: h1,
				comptesting.SpecHostH2: h2,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1, h2},
			},
		}

		observed := map[string]int{}
		for range 100 {
			host := ahl.HostForActorType("typeA")
			require.NotNil(t, host)
			observed[host.HostID]++
		}

		// Should have depleted all capacity in H1 (5), and the rest should be H2
		assert.Len(t, observed, 2)
		assert.Equal(t, 5, observed[comptesting.SpecHostH1])
		assert.Equal(t, 95, observed[comptesting.SpecHostH2])
	})

	t.Run("unsupported actor type", func(t *testing.T) {
		ahl := &activeHostsList{
			hosts:      map[string]*activeHost{},
			capacities: map[string][]*activeHost{},
		}
		assert.Nil(t, ahl.HostForActorType("missing"))
	})

	t.Run("zero capacity host", func(t *testing.T) {
		h1 := &activeHost{HostID: comptesting.SpecHostH1, ActorType: "typeA", Capacity: 0}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				comptesting.SpecHostH1: h1,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1},
			},
		}
		assert.Nil(t, ahl.HostForActorType("typeA"))
	})
}
