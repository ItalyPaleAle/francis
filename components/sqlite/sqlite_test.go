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

	comptesting "github.com/italypaleale/actors/components/testing"
	"github.com/italypaleale/actors/internal/ptr"
	"github.com/italypaleale/actors/internal/sql/transactions"
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
	go func() {
		err = s.Run(t.Context())
		if err != nil {
			log.Error("Error running provider", slog.Any("error", err))
		}
	}()

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
