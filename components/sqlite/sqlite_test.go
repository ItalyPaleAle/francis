package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/actors/components"
	comptesting "github.com/italypaleale/actors/components/testing"
	"github.com/italypaleale/actors/internal/ptr"
	"github.com/italypaleale/actors/internal/sql/transactions"
	"github.com/italypaleale/actors/internal/testutil"
)

// Connect to an in-memory database
// Replace with the commented-out string to write to a file on disk, for debugging
// Note: file must be deleted manually at the end of each test
const testConnectionString = ":memory:" // "file:testdb.db"

func TestSqliteProvider(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := SQLiteProviderOptions{
		ConnectionString: testConnectionString,

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

	// Run the test suite
	suite := comptesting.NewSuite(s)
	suite.Run(t)
}

func (s *SQLiteProvider) CleanupExpired() error {
	return s.gc.CleanupExpired()
}

func (s *SQLiteProvider) Now() time.Time {
	return s.clock.Now()
}

func (s *SQLiteProvider) AdvanceClock(d time.Duration) {
	s.clock.Sleep(d)
}

func (s *SQLiteProvider) Seed(ctx context.Context, spec comptesting.Spec) error {
	_, tErr := transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (z struct{}, err error) {
		now := s.clock.Now()

		// Truncate all data
		for _, tbl := range []string{"active_actors", "host_actor_types", "hosts", "actor_state", "alarms"} {
			_, err = tx.ExecContext(ctx, "DELETE FROM "+tbl)
			if err != nil {
				return z, fmt.Errorf("truncate '%s': %w", tbl, err)
			}
		}

		// Hosts
		insHost, err := tx.PrepareContext(ctx,
			`INSERT INTO hosts (host_id, host_address, host_last_health_check)
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
		insHat, err := tx.PrepareContext(ctx,
			`INSERT INTO host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit)
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
		insAA, err := tx.PrepareContext(ctx,
			`INSERT INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
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
		insAlarm, err := tx.PrepareContext(ctx,
			`INSERT INTO alarms (
				alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
				alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_expiration_time, alarm_lease_pid
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
				interval = ptr.Of(a.Interval)
			}
			if a.TTL > 0 {
				ttl = ptr.Of(now.UnixMilli() + a.TTL.Milliseconds())
			}

			var (
				leaseID, leasePID *string
				leaseExp          *int64
			)
			if a.LeaseTTL != nil {
				leaseExp = ptr.Of(now.UnixMilli() + a.LeaseTTL.Milliseconds())
				leaseID = ptr.Of(uuid.New().String())
				leasePID = &s.pid
			}

			_, err = insAlarm.ExecContext(ctx,
				a.AlarmID, a.ActorType, a.ActorID, a.Name, due,
				interval, ttl, a.Data, leaseID, leaseExp, leasePID,
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
	rows, err := s.db.QueryContext(ctx, "SELECT actor_type, actor_id, actor_state_data FROM actor_state")
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
	return transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (res comptesting.Spec, err error) {
		// Load all hosts
		rows, err := tx.QueryContext(ctx, "SELECT host_id, host_address, host_last_health_check FROM hosts")
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
		rows.Close()

		// Load all actor types
		rows, err = tx.QueryContext(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM host_actor_types")
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
		rows.Close()

		// Load all active actors
		rows, err = tx.QueryContext(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM active_actors")
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
		rows.Close()

		// Load all alarms
		rows, err = tx.QueryContext(ctx, "SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data, alarm_lease_id, alarm_lease_expiration_time, alarm_lease_pid FROM alarms")
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
			err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.Name, &due, &interval, &ttl, &r.Data, &r.LeaseID, &leaseExp, &r.LeasePID)
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
				r.LeaseExp = ptr.Of(time.UnixMilli(*leaseExp))
			}
			res.Alarms = append(res.Alarms, r)
		}
		rows.Close()

		return res, nil
	})
}

func TestHostGarbageCollection(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := SQLiteProviderOptions{
		ConnectionString: "file:gctest?mode=memory",
		// Disable automated cleanups in this test
		// We will run the cleanups manually
		CleanupInterval: -1,
		clock:           clock,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	s, err := NewSQLiteProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = s.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background for side effects (this initializes the GC)
	ctx := testutil.NewContextDoneNotifier(t.Context())
	go func() {
		err = s.Run(ctx)
		if err != nil && err != context.Canceled {
			log.Error("Error running provider", slog.Any("error", err))
		}
	}()

	// Wait for Run to call <-ctx.Done()
	ctx.WaitForDone()

	t.Run("garbage collector removes expired hosts", func(t *testing.T) {
		// Register multiple hosts at different times
		host1Req := components.RegisterHostReq{
			Address: "192.168.1.1:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor1", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		host1Res, err := s.RegisterHost(ctx, host1Req)
		require.NoError(t, err)

		// Advance clock by 30 seconds
		s.AdvanceClock(30 * time.Second)

		host2Req := components.RegisterHostReq{
			Address: "192.168.1.2:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor2", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 3},
			},
		}
		host2Res, err := s.RegisterHost(ctx, host2Req)
		require.NoError(t, err)

		// Advance clock by another 20 seconds (so host3 is 20s after host2)
		s.AdvanceClock(20 * time.Second)

		host3Req := components.RegisterHostReq{
			Address: "192.168.1.3:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor3", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		host3Res, err := s.RegisterHost(ctx, host3Req)
		require.NoError(t, err)

		// Verify all hosts are initially present
		spec, err := s.GetAllHosts(ctx)
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 3, "All hosts should be present initially")
		require.Len(t, spec.HostActorTypes, 3, "All host actor types should be present initially")

		// Advance clock to make only the first host expired (beyond 1 minute health check deadline)
		// Current state: Host1 at 50s, Host2 at 20s, Host3 at 0s
		// Advance by 15 seconds: Host1 at 65s (expired), Host2 at 35s (healthy), Host3 at 15s (healthy)
		s.AdvanceClock(15 * time.Second)

		// Run garbage collection
		err = s.CleanupExpired()
		require.NoError(t, err)

		// Verify only host1 and its actor types are removed
		spec, err = s.GetAllHosts(ctx)
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
		s.AdvanceClock(30 * time.Second)

		// Run garbage collection again
		err = s.CleanupExpired()
		require.NoError(t, err)

		// Verify only host3 remains
		spec, err = s.GetAllHosts(ctx)
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 1, "Only one host should remain")
		require.Len(t, spec.HostActorTypes, 1, "Only one host actor type should remain")

		// Verify host3 is the remaining one
		assert.Equal(t, host3Res.HostID, spec.Hosts[0].HostID, "Host3 should be the remaining host")
		assert.Equal(t, "TestActor3", spec.HostActorTypes[0].ActorType, "TestActor3 should be the remaining actor type")

		// Advance clock to make all hosts expired
		// Current state: Host3 at 45s
		// Advance by 20 seconds: Host3 at 65s (expired)
		s.AdvanceClock(20 * time.Second)

		// Run garbage collection one more time
		err = s.CleanupExpired()
		require.NoError(t, err)

		// Verify all hosts are removed
		spec, err = s.GetAllHosts(ctx)
		require.NoError(t, err)
		assert.Len(t, spec.Hosts, 0, "All hosts should be removed")
		assert.Len(t, spec.HostActorTypes, 0, "All host actor types should be removed")
	})
}

func TestGetInPlaceholders(t *testing.T) {
	t.Run("empty vals", func(t *testing.T) {
		vals := []string{}
		args := make([]any, 0)
		placeholders := getInPlaceholders(vals, args, 0)
		assert.Equal(t, "", placeholders)
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
		h1 := &activeHost{HostID: "H1", ActorType: "typeA", Capacity: 10000}
		h2 := &activeHost{HostID: "H2", ActorType: "typeA", Capacity: 10000}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				"H1": h1,
				"H2": h2,
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
		assert.GreaterOrEqual(t, observed["H1"], 30)
		assert.GreaterOrEqual(t, observed["H2"], 30)
	})

	t.Run("single host capacity exhaustion", func(t *testing.T) {
		h1 := &activeHost{HostID: "H1", ActorType: "typeA", Capacity: 3}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				"H1": h1,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1},
			},
		}

		// First three calls should return the host ID; fourth should be empty
		for range 3 {
			host := ahl.HostForActorType("typeA")
			require.NotNil(t, host)
			assert.Equal(t, "H1", host.HostID)
		}
		assert.Nil(t, ahl.HostForActorType("typeA"))
	})

	t.Run("multiple hosts capacity exhaustion", func(t *testing.T) {
		h1 := &activeHost{HostID: "H1", ActorType: "typeA", Capacity: 5}
		h2 := &activeHost{HostID: "H2", ActorType: "typeA", Capacity: 10000}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				"H1": h1,
				"H2": h2,
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
		assert.Equal(t, 5, observed["H1"])
		assert.Equal(t, 95, observed["H2"])
	})

	t.Run("unsupported actor type", func(t *testing.T) {
		ahl := &activeHostsList{
			hosts:      map[string]*activeHost{},
			capacities: map[string][]*activeHost{},
		}
		assert.Nil(t, ahl.HostForActorType("missing"))
	})

	t.Run("zero capacity host", func(t *testing.T) {
		h1 := &activeHost{HostID: "H1", ActorType: "typeA", Capacity: 0}
		ahl := &activeHostsList{
			hosts: map[string]*activeHost{
				"H1": h1,
			},
			capacities: map[string][]*activeHost{
				"typeA": {h1},
			},
		}
		assert.Nil(t, ahl.HostForActorType("typeA"))
	})
}
