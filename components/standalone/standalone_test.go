package standalone

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
	_ "modernc.org/sqlite"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone/internal"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/ref"
)

func TestStandaloneMemory(t *testing.T) {
	p := initTestProvider(t)

	// Run the test suites
	suite := comptesting.NewSuite(p)
	t.Run("suite", suite.RunTests)
}

func TestStandaloneSQLiteBacked(t *testing.T) {
	p := initSQLiteTestProvider(t)

	// Run the test suites
	suite := comptesting.NewSuite(p)
	t.Run("suite", suite.RunTests)
}

// Name of the environmental variable containing the connection string to the test database.
// Example: TEST_STANDALONE_POSTGRES_CONNSTRING=postgres://actors:actors@localhost:5432/actors
const postgresConnstringEnvVar = "TEST_STANDALONE_POSTGRES_CONNSTRING"

func TestStandalonePostgresBacked(t *testing.T) {
	p, cleanupFn := initPostgresTestProvider(t)
	t.Cleanup(cleanupFn)

	// Run the test suites
	suite := comptesting.NewSuite(p)
	t.Run("suite", suite.RunTests)
}

func TestStandaloneTablePrefix(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) {
		// sqliteTables returns the names of all tables in the database, excluding SQLite's internal objects
		sqliteTables := func(t *testing.T, p *StandaloneSQLiteBacked) []string {
			t.Helper()
			rows, err := p.db.QueryContext(t.Context(), "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
			require.NoError(t, err)
			defer rows.Close()

			var names []string
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}

			err = rows.Err()
			require.NoError(t, err)

			return names
		}

		t.Run("default prefix is francis", func(t *testing.T) {
			p := initSQLiteTestProvider(t)
			require.Equal(t, "francis_", p.tablePrefix)

			names := sqliteTables(t, p)
			require.Contains(t, names, "francis_hosts")
			require.Contains(t, names, "francis_alarms")
			require.Contains(t, names, "francis_dead_jobs")
			require.Contains(t, names, "francis_metadata")
			// No object should exist under its bare, unprefixed name
			require.NotContains(t, names, "hosts")
			require.NotContains(t, names, "alarms")
		})

		t.Run("custom prefix", func(t *testing.T) {
			p := initSQLiteTestProviderWithPrefix(t, "myapp")
			require.Equal(t, "myapp_", p.tablePrefix)

			names := sqliteTables(t, p)
			for _, name := range names {
				require.Truef(t, strings.HasPrefix(name, "myapp_"), "table %q is not prefixed", name)
			}
			require.Contains(t, names, "myapp_hosts")
			require.Contains(t, names, "myapp_dead_jobs")
			require.Contains(t, names, "myapp_metadata")
		})

		t.Run("custom prefix is functional end-to-end", func(t *testing.T) {
			p := initSQLiteTestProviderWithPrefix(t, "myapp")

			// Register a host and read it back through the regular API (which uses the prefixed inline queries)
			hostRes, err := p.RegisterHost(t.Context(), components.RegisterHostReq{
				Address: "10.0.0.1:8080",
				ActorTypes: []components.ActorHostType{
					{ActorType: "TestActor", IdleTimeout: 5 * time.Minute},
				},
			})
			require.NoError(t, err)
			require.NotEmpty(t, hostRes.HostID)

			hosts, err := p.ListHosts(t.Context())
			require.NoError(t, err)
			require.Len(t, hosts, 1)
			require.Equal(t, "10.0.0.1:8080", hosts[0].Address)
		})
	})

	t.Run("Postgres", func(t *testing.T) {
		// tableInSchema reports whether a table exists in the connection's current schema (the isolated per-test schema)
		tableInSchema := func(t *testing.T, p *StandalonePostgresBacked, name string) bool {
			t.Helper()
			var exists bool
			err := p.db.
				QueryRow(t.Context(),
					"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)",
					name,
				).
				Scan(&exists)
			require.NoError(t, err)
			return exists
		}

		t.Run("default prefix is francis", func(t *testing.T) {
			p, cleanupFn := initPostgresTestProvider(t)
			t.Cleanup(cleanupFn)
			require.Equal(t, "francis_", p.tablePrefix)

			require.True(t, tableInSchema(t, p, "francis_hosts"))
			require.True(t, tableInSchema(t, p, "francis_dead_jobs"))
			require.True(t, tableInSchema(t, p, "francis_metadata"))
			// No object should exist under its bare, unprefixed name
			require.False(t, tableInSchema(t, p, "hosts"))
			require.False(t, tableInSchema(t, p, "alarms"))
		})

		t.Run("custom prefix is functional end-to-end", func(t *testing.T) {
			p, cleanupFn := initPostgresTestProviderWithPrefix(t, "myapp")
			t.Cleanup(cleanupFn)
			require.Equal(t, "myapp_", p.tablePrefix)

			require.True(t, tableInSchema(t, p, "myapp_hosts"))
			require.False(t, tableInSchema(t, p, "hosts"))

			// Register a host and read it back through the regular API (which uses the prefixed inline queries)
			hostRes, err := p.RegisterHost(t.Context(), components.RegisterHostReq{
				Address: "10.0.0.1:8080",
				ActorTypes: []components.ActorHostType{
					{ActorType: "TestActor", IdleTimeout: 5 * time.Minute},
				},
			})
			require.NoError(t, err)
			require.NotEmpty(t, hostRes.HostID)

			hosts, err := p.ListHosts(t.Context())
			require.NoError(t, err)
			require.Len(t, hosts, 1)
			require.Equal(t, "10.0.0.1:8080", hosts[0].Address)
		})
	})
}

func initSQLiteTestProvider(t *testing.T) *StandaloneSQLiteBacked {
	return initSQLiteTestProviderWithPrefix(t, "")
}

func initSQLiteTestProviderWithPrefix(t *testing.T, tablePrefix string) *StandaloneSQLiteBacked {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	// Open in-memory SQLite database
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "Error opening SQLite database")
	db.SetMaxOpenConns(1) // Required for in-memory SQLite
	t.Cleanup(func() { db.Close() })

	// Enable foreign keys — required by the provider (with MaxOpenConns(1) this applies to the single connection)
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON")
	require.NoError(t, err, "Error enabling foreign keys")

	providerOpts := StandaloneSQLiteOptions{
		DB:          db,
		Clock:       clock,
		TablePrefix: tablePrefix,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	p, err := NewStandaloneSQLiteBacked(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = p.Run(ctx)
	}()

	// Give a brief moment for Run to start
	time.Sleep(10 * time.Millisecond)

	return p
}

func initPostgresTestProvider(t *testing.T) (*StandalonePostgresBacked, func()) {
	return initPostgresTestProviderWithPrefix(t, "")
}

func initPostgresTestProviderWithPrefix(t *testing.T, tablePrefix string) (*StandalonePostgresBacked, func()) {
	connString := os.Getenv(postgresConnstringEnvVar)
	if connString == "" {
		t.Skip(`To run these tests, set the env var ` + postgresConnstringEnvVar + ` with the connection string for Postgres database. Example: "` + postgresConnstringEnvVar + `=postgres://actors:actors@localhost:5432/actors"`)
	}

	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	// Generate a random name for the schema
	testSchema := generateTestSchemaName(t)
	t.Log("Test schema:", testSchema)

	// Connect to the database with the test schema
	conn, cleanupSchema := connectPostgresTestDatabase(t, connString, testSchema)

	providerOpts := StandalonePostgresOptions{
		DB:          conn,
		Clock:       clock,
		TablePrefix: tablePrefix,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	p, err := NewStandalonePostgresBacked(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background
	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		_ = p.Run(ctx)
	}()

	// Give a brief moment for Run to start
	time.Sleep(10 * time.Millisecond)

	cleanupFn := func() {
		cancel()
		cleanupSchema(t)
		conn.Close()
	}

	return p, cleanupFn
}

func generateTestSchemaName(t *testing.T) string {
	t.Helper()

	testSchemaB := make([]byte, 5)
	_, err := io.ReadFull(rand.Reader, testSchemaB)
	require.NoError(t, err)
	return "test_standalone_" + hex.EncodeToString(testSchemaB)
}

func connectPostgresTestDatabase(t *testing.T, connString string, testSchema string) (conn *pgxpool.Pool, cleanupFn func(t *testing.T)) {
	t.Helper()

	// Parse the connection string
	cfg, err := pgxpool.ParseConfig(connString)
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

	// Connect to the database
	conn, err = pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err, "Failed to connect to database")

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

func initTestProvider(t *testing.T) *StandaloneMemory {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := StandaloneMemoryOptions{
		Clock: clock,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	p, err := NewStandaloneMemory(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = p.Run(ctx)
	}()

	// Give a brief moment for Run to start
	time.Sleep(10 * time.Millisecond)

	return p
}

func (p *StandaloneMemory) clearData() {
	p.Hosts = make(map[string]*internal.Host)
	p.HostsByAddress = make(map[string]string)
	p.HostActorTypes = make(map[string][]*internal.HostActorType)
	p.ActiveActors = make(map[internal.ActorKey]*internal.ActiveActor)
	p.Alarms = make(map[internal.AlarmKey]*internal.Alarm)
	p.AlarmsByID = make(map[string]*internal.Alarm)
	p.ActorState = make(map[internal.ActorKey]*internal.StateEntry)
}

// Seed seeds the data into the provider.
func (p *StandaloneMemory) Seed(ctx context.Context, spec comptesting.Spec) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	// Seed also mutates the state map (via clearData), so it must hold StateMu too, to
	// serialize against the background cleanup loop
	p.StateMu.Lock()
	defer p.StateMu.Unlock()

	now := p.Clock.Now()

	// Clear all data
	p.clearData()

	// Seed hosts
	for _, h := range spec.Hosts {
		host := &internal.Host{
			ID:              h.HostID,
			Address:         h.Address,
			LastHealthCheck: now.Add(-h.LastHealthAgo),
		}
		p.Hosts[h.HostID] = host
		p.HostsByAddress[h.Address] = h.HostID
	}

	// Seed host actor types
	for _, hat := range spec.HostActorTypes {
		if p.HostActorTypes[hat.HostID] == nil {
			p.HostActorTypes[hat.HostID] = make([]*internal.HostActorType, 0)
		}
		p.HostActorTypes[hat.HostID] = append(p.HostActorTypes[hat.HostID], &internal.HostActorType{
			HostID:           hat.HostID,
			ActorType:        hat.ActorType,
			IdleTimeout:      hat.ActorIdleTimeout,
			ConcurrencyLimit: int32(hat.ActorConcurrencyLimit), // #nosec G115
		})
	}

	// Seed active actors
	for _, aa := range spec.ActiveActors {
		key := internal.NewActorKey(aa.ActorType, aa.ActorID)
		p.ActiveActors[key] = &internal.ActiveActor{
			ActorType:   aa.ActorType,
			ActorID:     aa.ActorID,
			HostID:      aa.HostID,
			IdleTimeout: aa.ActorIdleTimeout,
			Activation:  now.Add(-aa.ActivationAgo),
		}
	}

	// Seed alarms
	for _, a := range spec.Alarms {
		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		alm := &internal.Alarm{
			ID:        a.AlarmID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueTime:   now.Add(a.DueIn),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL > 0 {
			alm.TTL = new(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = new(uuid.New().String())
		}

		p.Alarms[key] = alm
		p.AlarmsByID[a.AlarmID] = alm
	}

	return nil
}

// Now returns the current time.
func (p *StandaloneMemory) Now() time.Time {
	return p.Clock.Now()
}

// AdvanceClock advances the clock.
func (p *StandaloneMemory) AdvanceClock(d time.Duration) error {
	p.Clock.Sleep(d)
	return nil
}

// GetAllActorState returns all stored actor state.
func (p *StandaloneMemory) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	result := make(comptesting.ActorStateSpecCollection, 0, len(p.ActorState))
	for key, state := range p.ActorState {
		result = append(result, comptesting.ActorStateSpec{
			ActorType: key.ActorType,
			ActorID:   key.ActorID,
			Data:      state.Data,
		})
	}

	return result, nil
}

// GetAllHosts returns all stored hosts, host actor types, active actors, and alarms.
func (p *StandaloneMemory) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()
	spec := comptesting.Spec{}

	// Hosts
	spec.Hosts = make([]comptesting.HostSpec, 0, len(p.Hosts))
	for _, h := range p.Hosts {
		spec.Hosts = append(spec.Hosts, comptesting.HostSpec{
			HostID:        h.ID,
			Address:       h.Address,
			LastHealthAgo: now.Sub(h.LastHealthCheck),
		})
	}

	// Host actor types
	spec.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
	for hostID, types := range p.HostActorTypes {
		for _, hat := range types {
			spec.HostActorTypes = append(spec.HostActorTypes, comptesting.HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             hat.ActorType,
				ActorIdleTimeout:      hat.IdleTimeout,
				ActorConcurrencyLimit: int(hat.ConcurrencyLimit),
			})
		}
	}

	// Active actors
	spec.ActiveActors = make([]comptesting.ActiveActorSpec, 0, len(p.ActiveActors))
	for _, aa := range p.ActiveActors {
		spec.ActiveActors = append(spec.ActiveActors, comptesting.ActiveActorSpec{
			ActorType:        aa.ActorType,
			ActorID:          aa.ActorID,
			HostID:           aa.HostID,
			ActorIdleTimeout: aa.IdleTimeout,
			ActivationAgo:    now.Sub(aa.Activation),
		})
	}

	// Alarms
	spec.Alarms = make([]comptesting.AlarmSpec, 0, len(p.Alarms))
	for _, a := range p.Alarms {
		as := comptesting.AlarmSpec{
			AlarmID:   a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueIn:     a.DueTime.Sub(now),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL != nil {
			as.TTL = a.TTL.Sub(now)
		}

		if a.LeaseID != nil {
			as.LeaseID = a.LeaseID
		}

		if a.LeaseExpiration != nil {
			as.LeaseExp = a.LeaseExpiration
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}

// Test helper methods for StandaloneSQLiteBacked

func (p *StandaloneSQLiteBacked) clearData() {
	p.Hosts = make(map[string]*internal.Host)
	p.HostsByAddress = make(map[string]string)
	p.HostActorTypes = make(map[string][]*internal.HostActorType)
	p.ActiveActors = make(map[internal.ActorKey]*internal.ActiveActor)
	p.Alarms = make(map[internal.AlarmKey]*internal.Alarm)
	p.AlarmsByID = make(map[string]*internal.Alarm)
	p.ActorState = make(map[internal.ActorKey]*internal.StateEntry)
}

func (p *StandaloneSQLiteBacked) clearDatabase(ctx context.Context) error {
	tables := []string{"alarms", "active_actors", "host_actor_types", "hosts", "actor_state"}
	for _, table := range tables {
		//nolint:gosec
		_, err := p.db.ExecContext(ctx, "DELETE FROM "+p.tablePrefix+table)
		if err != nil {
			return err
		}
	}
	return nil
}

// Seed seeds the data into the provider.
func (p *StandaloneSQLiteBacked) Seed(ctx context.Context, spec comptesting.Spec) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	// Seed also mutates the state map (via clearData), so it must hold StateMu too, to
	// serialize against the background cleanup loop
	p.StateMu.Lock()
	defer p.StateMu.Unlock()

	now := p.Clock.Now()

	// Clear all data from memory
	p.clearData()

	// Clear all data from database
	err := p.clearDatabase(ctx)
	if err != nil {
		return err
	}

	// Seed hosts
	for _, h := range spec.Hosts {
		host := &internal.Host{
			ID:              h.HostID,
			Address:         h.Address,
			LastHealthCheck: now.Add(-h.LastHealthAgo),
		}
		p.Hosts[h.HostID] = host
		p.HostsByAddress[h.Address] = h.HostID
	}

	// Seed host actor types
	for _, hat := range spec.HostActorTypes {
		if p.HostActorTypes[hat.HostID] == nil {
			p.HostActorTypes[hat.HostID] = make([]*internal.HostActorType, 0)
		}
		p.HostActorTypes[hat.HostID] = append(p.HostActorTypes[hat.HostID], &internal.HostActorType{
			HostID:           hat.HostID,
			ActorType:        hat.ActorType,
			IdleTimeout:      hat.ActorIdleTimeout,
			ConcurrencyLimit: int32(hat.ActorConcurrencyLimit), // #nosec G115
		})
	}

	// Seed active actors
	for _, aa := range spec.ActiveActors {
		key := internal.NewActorKey(aa.ActorType, aa.ActorID)
		p.ActiveActors[key] = &internal.ActiveActor{
			ActorType:   aa.ActorType,
			ActorID:     aa.ActorID,
			HostID:      aa.HostID,
			IdleTimeout: aa.ActorIdleTimeout,
			Activation:  now.Add(-aa.ActivationAgo),
		}
	}

	// Seed alarms
	for _, a := range spec.Alarms {
		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		alm := &internal.Alarm{
			ID:        a.AlarmID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueTime:   now.Add(a.DueIn),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL > 0 {
			alm.TTL = new(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = new(uuid.New().String())
		}

		p.Alarms[key] = alm
		p.AlarmsByID[a.AlarmID] = alm
	}

	return nil
}

// Now returns the current time.
func (p *StandaloneSQLiteBacked) Now() time.Time {
	return p.Clock.Now()
}

// AdvanceClock advances the clock.
func (p *StandaloneSQLiteBacked) AdvanceClock(d time.Duration) error {
	p.Clock.Sleep(d)
	return nil
}

// GetAllActorState returns all stored actor state.
func (p *StandaloneSQLiteBacked) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	result := make(comptesting.ActorStateSpecCollection, 0, len(p.ActorState))
	for key, state := range p.ActorState {
		result = append(result, comptesting.ActorStateSpec{
			ActorType: key.ActorType,
			ActorID:   key.ActorID,
			Data:      state.Data,
		})
	}

	return result, nil
}

// GetAllHosts returns all stored hosts, host actor types, active actors, and alarms.
func (p *StandaloneSQLiteBacked) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()
	spec := comptesting.Spec{}

	// Hosts
	spec.Hosts = make([]comptesting.HostSpec, 0, len(p.Hosts))
	for _, h := range p.Hosts {
		spec.Hosts = append(spec.Hosts, comptesting.HostSpec{
			HostID:        h.ID,
			Address:       h.Address,
			LastHealthAgo: now.Sub(h.LastHealthCheck),
		})
	}

	// Host actor types
	spec.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
	for hostID, types := range p.HostActorTypes {
		for _, hat := range types {
			spec.HostActorTypes = append(spec.HostActorTypes, comptesting.HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             hat.ActorType,
				ActorIdleTimeout:      hat.IdleTimeout,
				ActorConcurrencyLimit: int(hat.ConcurrencyLimit),
			})
		}
	}

	// Active actors
	spec.ActiveActors = make([]comptesting.ActiveActorSpec, 0, len(p.ActiveActors))
	for _, aa := range p.ActiveActors {
		spec.ActiveActors = append(spec.ActiveActors, comptesting.ActiveActorSpec{
			ActorType:        aa.ActorType,
			ActorID:          aa.ActorID,
			HostID:           aa.HostID,
			ActorIdleTimeout: aa.IdleTimeout,
			ActivationAgo:    now.Sub(aa.Activation),
		})
	}

	// Alarms
	spec.Alarms = make([]comptesting.AlarmSpec, 0, len(p.Alarms))
	for _, a := range p.Alarms {
		as := comptesting.AlarmSpec{
			AlarmID:   a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueIn:     a.DueTime.Sub(now),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL != nil {
			as.TTL = a.TTL.Sub(now)
		}

		if a.LeaseID != nil {
			as.LeaseID = a.LeaseID
		}

		if a.LeaseExpiration != nil {
			as.LeaseExp = a.LeaseExpiration
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}

// Test helper methods for StandalonePostgresBacked

func (p *StandalonePostgresBacked) clearData() {
	p.Hosts = make(map[string]*internal.Host)
	p.HostsByAddress = make(map[string]string)
	p.HostActorTypes = make(map[string][]*internal.HostActorType)
	p.ActiveActors = make(map[internal.ActorKey]*internal.ActiveActor)
	p.Alarms = make(map[internal.AlarmKey]*internal.Alarm)
	p.AlarmsByID = make(map[string]*internal.Alarm)
	p.ActorState = make(map[internal.ActorKey]*internal.StateEntry)
}

func (p *StandalonePostgresBacked) clearDatabase(ctx context.Context) error {
	tables := []string{"alarms", "active_actors", "host_actor_types", "hosts", "actor_state"}
	for _, table := range tables {
		//nolint:gosec
		_, err := p.db.Exec(ctx, "DELETE FROM "+p.tablePrefix+table)
		if err != nil {
			return err
		}
	}
	return nil
}

// Seed seeds the data into the provider.
func (p *StandalonePostgresBacked) Seed(ctx context.Context, spec comptesting.Spec) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	// Seed also mutates the state map (via clearData), so it must hold StateMu too, to serialize against the background cleanup loop
	p.StateMu.Lock()
	defer p.StateMu.Unlock()

	now := p.Clock.Now()

	// Clear all data from memory
	p.clearData()

	// Clear all data from database
	err := p.clearDatabase(ctx)
	if err != nil {
		return err
	}

	// Seed hosts
	for _, h := range spec.Hosts {
		host := &internal.Host{
			ID:              h.HostID,
			Address:         h.Address,
			LastHealthCheck: now.Add(-h.LastHealthAgo),
		}
		p.Hosts[h.HostID] = host
		p.HostsByAddress[h.Address] = h.HostID
	}

	// Seed host actor types
	for _, hat := range spec.HostActorTypes {
		if p.HostActorTypes[hat.HostID] == nil {
			p.HostActorTypes[hat.HostID] = make([]*internal.HostActorType, 0)
		}
		p.HostActorTypes[hat.HostID] = append(p.HostActorTypes[hat.HostID], &internal.HostActorType{
			HostID:           hat.HostID,
			ActorType:        hat.ActorType,
			IdleTimeout:      hat.ActorIdleTimeout,
			ConcurrencyLimit: int32(hat.ActorConcurrencyLimit), // #nosec G115
		})
	}

	// Seed active actors
	for _, aa := range spec.ActiveActors {
		key := internal.NewActorKey(aa.ActorType, aa.ActorID)
		p.ActiveActors[key] = &internal.ActiveActor{
			ActorType:   aa.ActorType,
			ActorID:     aa.ActorID,
			HostID:      aa.HostID,
			IdleTimeout: aa.ActorIdleTimeout,
			Activation:  now.Add(-aa.ActivationAgo),
		}
	}

	// Seed alarms
	for _, a := range spec.Alarms {
		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		alm := &internal.Alarm{
			ID:        a.AlarmID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueTime:   now.Add(a.DueIn),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL > 0 {
			alm.TTL = new(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = new(uuid.New().String())
		}

		p.Alarms[key] = alm
		p.AlarmsByID[a.AlarmID] = alm
	}

	return nil
}

// Now returns the current time.
func (p *StandalonePostgresBacked) Now() time.Time {
	return p.Clock.Now()
}

// AdvanceClock advances the clock.
func (p *StandalonePostgresBacked) AdvanceClock(d time.Duration) error {
	p.Clock.Sleep(d)
	return nil
}

// GetAllActorState returns all stored actor state.
func (p *StandalonePostgresBacked) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	result := make(comptesting.ActorStateSpecCollection, 0, len(p.ActorState))
	for key, state := range p.ActorState {
		result = append(result, comptesting.ActorStateSpec{
			ActorType: key.ActorType,
			ActorID:   key.ActorID,
			Data:      state.Data,
		})
	}

	return result, nil
}

// GetAllHosts returns all stored hosts, host actor types, active actors, and alarms.
func (p *StandalonePostgresBacked) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()
	spec := comptesting.Spec{}

	// Hosts
	spec.Hosts = make([]comptesting.HostSpec, 0, len(p.Hosts))
	for _, h := range p.Hosts {
		spec.Hosts = append(spec.Hosts, comptesting.HostSpec{
			HostID:        h.ID,
			Address:       h.Address,
			LastHealthAgo: now.Sub(h.LastHealthCheck),
		})
	}

	// Host actor types
	spec.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
	for hostID, types := range p.HostActorTypes {
		for _, hat := range types {
			spec.HostActorTypes = append(spec.HostActorTypes, comptesting.HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             hat.ActorType,
				ActorIdleTimeout:      hat.IdleTimeout,
				ActorConcurrencyLimit: int(hat.ConcurrencyLimit),
			})
		}
	}

	// Active actors
	spec.ActiveActors = make([]comptesting.ActiveActorSpec, 0, len(p.ActiveActors))
	for _, aa := range p.ActiveActors {
		spec.ActiveActors = append(spec.ActiveActors, comptesting.ActiveActorSpec{
			ActorType:        aa.ActorType,
			ActorID:          aa.ActorID,
			HostID:           aa.HostID,
			ActorIdleTimeout: aa.IdleTimeout,
			ActivationAgo:    now.Sub(aa.Activation),
		})
	}

	// Alarms
	spec.Alarms = make([]comptesting.AlarmSpec, 0, len(p.Alarms))
	for _, a := range p.Alarms {
		as := comptesting.AlarmSpec{
			AlarmID:   a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueIn:     a.DueTime.Sub(now),
			Interval:  a.Interval,
			Data:      a.Data,
		}

		if a.TTL != nil {
			as.TTL = a.TTL.Sub(now)
		}

		if a.LeaseID != nil {
			as.LeaseID = a.LeaseID
		}

		if a.LeaseExpiration != nil {
			as.LeaseExp = a.LeaseExpiration
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}

// MockPersistHook is a mock implementation of internal.PersistHook that
// captures each call to PersistChanges for later verification.
type MockPersistHook struct {
	Calls   []*internal.Changes
	mu      sync.Mutex
	ErrFunc func() error // Optional function to return errors
}

func (m *MockPersistHook) PersistChanges(ctx context.Context, changes *internal.Changes) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy the changes since they're pooled and reused
	m.Calls = append(m.Calls, changes.Clone())

	if m.ErrFunc != nil {
		return m.ErrFunc()
	}
	return nil
}

// Reset clears all recorded calls
func (m *MockPersistHook) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = nil
}

// GetCalls returns a copy of the recorded calls
func (m *MockPersistHook) GetCalls() []*internal.Changes {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Calls
}

// initProviderWithMockHook creates a test provider with a MockPersistHook
func initProviderWithMockHook(t *testing.T) (*internal.Provider, *MockPersistHook) {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	mock := &MockPersistHook{}

	providerConfig := comptesting.GetProviderConfig()
	providerOpts := internal.ProviderOptions{
		Clock:           clock,
		CleanupInterval: -1, // Disable automatic cleanup
		PersistHook:     mock,
	}

	p, err := internal.NewProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	return p, mock
}

func TestPersistHook_RegisterHost(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host with actor types
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute, ConcurrencyLimit: 10},
			{ActorType: "otheractor", IdleTimeout: 2 * time.Minute, ConcurrencyLimit: 5},
		},
	}

	res, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)
	require.NotEmpty(t, res.HostID)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Hosts.Set
	require.Len(t, changes.Hosts.Set, 1, "Expected 1 host to be set")
	require.Equal(t, res.HostID, changes.Hosts.Set[0].Key)
	require.Equal(t, "localhost:8080", changes.Hosts.Set[0].Value.Address)

	// Verify HostActorTypes.Set
	require.Len(t, changes.HostActorTypes.Set, 2, "Expected 2 actor types to be set")
	actorTypes := make(map[string]*internal.HostActorType)
	for _, hat := range changes.HostActorTypes.Set {
		actorTypes[hat.ActorType] = hat
	}
	require.Contains(t, actorTypes, "myactor")
	require.Contains(t, actorTypes, "otheractor")
	require.Equal(t, time.Minute, actorTypes["myactor"].IdleTimeout)
	require.Equal(t, int32(10), actorTypes["myactor"].ConcurrencyLimit)
}

func TestPersistHook_UpdateActorHost(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// First register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	res, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	mock.Reset()

	// Update the host with new actor types
	updateReq := components.UpdateActorHostReq{
		UpdateLastHealthCheck: true,
		ActorTypes: []components.ActorHostType{
			{ActorType: "newactor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 20},
		},
	}

	err = p.UpdateActorHost(t.Context(), res.HostID, updateReq)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Hosts.Set (health check update)
	require.Len(t, changes.Hosts.Set, 1, "Expected 1 host to be set for health check update")

	// Verify HostActorTypes.Delete (old types removed)
	require.Len(t, changes.HostActorTypes.Delete, 1, "Expected 1 actor type to be deleted")
	require.Equal(t, "myactor", changes.HostActorTypes.Delete[0].ActorType)

	// Verify HostActorTypes.Set (new types added)
	require.Len(t, changes.HostActorTypes.Set, 1, "Expected 1 actor type to be set")
	require.Equal(t, "newactor", changes.HostActorTypes.Set[0].ActorType)
}

func TestPersistHook_UnregisterHost(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	res, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	// Place an actor on the host
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	_, err = p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)

	mock.Reset()

	// Unregister the host
	err = p.UnregisterHost(t.Context(), res.HostID)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Hosts.Delete
	require.Len(t, changes.Hosts.Delete, 1, "Expected 1 host to be deleted")
	require.Equal(t, res.HostID, changes.Hosts.Delete[0])

	// Verify HostActorTypes.Delete
	require.Len(t, changes.HostActorTypes.Delete, 1, "Expected 1 actor type to be deleted")
	require.Equal(t, res.HostID, changes.HostActorTypes.Delete[0].HostID)

	// Verify ActiveActors.Delete
	require.Len(t, changes.ActiveActors.Delete, 1, "Expected 1 active actor to be deleted")
	require.Equal(t, "myactor", changes.ActiveActors.Delete[0].ActorType)
	require.Equal(t, "actor1", changes.ActiveActors.Delete[0].ActorID)
}

func TestPersistHook_SetState(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	data := []byte(`{"key":"value"}`)

	err := p.SetState(t.Context(), actorRef, data, components.SetStateOpts{TTL: time.Hour})
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify ActorState.Set
	require.Len(t, changes.ActorState.Set, 1, "Expected 1 state to be set")
	require.Equal(t, "myactor", changes.ActorState.Set[0].Key.ActorType)
	require.Equal(t, "actor1", changes.ActorState.Set[0].Key.ActorID)
	require.Equal(t, data, changes.ActorState.Set[0].Value.Data)
	require.NotNil(t, changes.ActorState.Set[0].Value.Expiration)
}

func TestPersistHook_DeleteState(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}

	// First set state
	err := p.SetState(t.Context(), actorRef, []byte("data"), components.SetStateOpts{})
	require.NoError(t, err)

	mock.Reset()

	// Now delete it
	err = p.DeleteState(t.Context(), actorRef)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify ActorState.Delete
	require.Len(t, changes.ActorState.Delete, 1, "Expected 1 state to be deleted")
	require.Equal(t, "myactor", changes.ActorState.Delete[0].ActorType)
	require.Equal(t, "actor1", changes.ActorState.Delete[0].ActorID)
}

func TestPersistHook_SetAlarm(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(time.Hour)
	req := components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  dueTime,
			Interval: "@every 1h",
			Data:     []byte("alarm-data"),
		},
	}

	err := p.SetAlarm(t.Context(), alarmRef, req)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Set
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm to be set")
	alarm := changes.Alarms.Set[0].Value
	require.Equal(t, "myactor", alarm.ActorType)
	require.Equal(t, "actor1", alarm.ActorID)
	require.Equal(t, "reminder", alarm.Name)
	require.Equal(t, "@every 1h", alarm.Interval)
	require.Equal(t, []byte("alarm-data"), alarm.Data)
}

func TestPersistHook_DeleteAlarm(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(time.Hour)

	// First set an alarm
	err := p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	mock.Reset()

	// Now delete it
	err = p.DeleteAlarm(t.Context(), alarmRef)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Delete
	require.Len(t, changes.Alarms.Delete, 1, "Expected 1 alarm to be deleted")
	require.NotEmpty(t, changes.Alarms.Delete[0])
}

func TestPersistHook_FetchAndLeaseUpcomingAlarms(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	// Set an alarm that's due soon
	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second) // Due in 5 seconds (within fetch ahead interval)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	mock.Reset()

	// Fetch and lease upcoming alarms
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Set (lease acquisition)
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm to be set with lease")
	require.NotNil(t, changes.Alarms.Set[0].Value.LeaseID)
	require.NotNil(t, changes.Alarms.Set[0].Value.LeaseExpiration)

	// Verify ActiveActors.Set (placement)
	require.Len(t, changes.ActiveActors.Set, 1, "Expected 1 active actor to be set")
	require.Equal(t, "myactor", changes.ActiveActors.Set[0].Value.ActorType)
	require.Equal(t, "actor1", changes.ActiveActors.Set[0].Value.ActorID)
	require.Equal(t, hostRes.HostID, changes.ActiveActors.Set[0].Value.HostID)
}

func TestPersistHook_GetLeasedAlarm_NoChanges(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host and set an alarm
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime, Data: []byte("test-data")}})
	require.NoError(t, err)

	// Fetch and lease
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	mock.Reset()

	// Get the leased alarm - this should be read-only
	res, err := p.GetLeasedAlarm(t.Context(), leases[0])
	require.NoError(t, err)
	require.Equal(t, []byte("test-data"), res.Data)

	// Verify NO PersistChanges was called (read-only operation)
	calls := mock.GetCalls()
	require.Empty(t, calls, "Expected no PersistChanges call for read-only operation")
}

func TestPersistHook_RenewAlarmLeases(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host and set an alarm
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Fetch and lease
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	mock.Reset()

	// Renew the leases
	res, err := p.RenewAlarmLeases(t.Context(), components.RenewAlarmLeasesReq{
		Hosts:  []string{hostRes.HostID},
		Leases: leases,
	})
	require.NoError(t, err)
	require.Len(t, res.Leases, 1)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Set (lease expiration update)
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm to be updated")
	require.NotNil(t, changes.Alarms.Set[0].Value.LeaseExpiration)
}

func TestPersistHook_ReleaseAlarmLease(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host and set an alarm
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Fetch and lease
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	mock.Reset()

	// Release the lease
	err = p.ReleaseAlarmLease(t.Context(), leases[0])
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Set (lease cleared)
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm to be updated")
	require.Nil(t, changes.Alarms.Set[0].Value.LeaseID)
	require.Nil(t, changes.Alarms.Set[0].Value.LeaseExpiration)
}

func TestPersistHook_UpdateLeasedAlarm(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host and set an alarm
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Fetch and lease
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	mock.Reset()

	// Update the leased alarm
	newDueTime := p.Clock.Now().Add(2 * time.Hour)
	err = p.UpdateLeasedAlarm(t.Context(), leases[0], components.UpdateLeasedAlarmReq{
		DueTime:      newDueTime,
		RefreshLease: true,
	})
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Set (due time + lease update)
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm to be updated")
	require.Equal(t, newDueTime, changes.Alarms.Set[0].Value.DueTime)
	require.NotNil(t, changes.Alarms.Set[0].Value.LeaseID)
	require.NotNil(t, changes.Alarms.Set[0].Value.LeaseExpiration)
}

func TestPersistHook_DeleteLeasedAlarm(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host and set an alarm
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)

	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Fetch and lease
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)

	mock.Reset()

	// Delete the leased alarm
	err = p.DeleteLeasedAlarm(t.Context(), leases[0])
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify Alarms.Delete
	require.Len(t, changes.Alarms.Delete, 1, "Expected 1 alarm to be deleted")
}

func TestPersistHook_LookupActor_NewPlacement(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	mock.Reset()

	// Lookup an actor (will create new placement)
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	lookupRes, err := p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)
	require.Equal(t, hostRes.HostID, lookupRes.HostID)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify ActiveActors.Set
	require.Len(t, changes.ActiveActors.Set, 1, "Expected 1 active actor to be set")
	require.Equal(t, "myactor", changes.ActiveActors.Set[0].Key.ActorType)
	require.Equal(t, "actor1", changes.ActiveActors.Set[0].Key.ActorID)
	require.Equal(t, hostRes.HostID, changes.ActiveActors.Set[0].Value.HostID)
}

func TestPersistHook_LookupActor_ExistingActor(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	// First lookup creates the placement
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	_, err = p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)

	mock.Reset()

	// Second lookup should return existing placement without persistence
	lookupRes, err := p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)
	require.Equal(t, hostRes.HostID, lookupRes.HostID)

	// Verify NO PersistChanges was called (existing actor)
	calls := mock.GetCalls()
	require.Empty(t, calls, "Expected no PersistChanges call for existing actor")
}

func TestPersistHook_RemoveActor(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	// Place an actor
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	_, err = p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)

	// Set an alarm with a lease
	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)
	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Fetch and lease the alarm
	_, err = p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)

	mock.Reset()

	// Remove the actor
	err = p.RemoveActor(t.Context(), actorRef)
	require.NoError(t, err)

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	changes := calls[0]

	// Verify ActiveActors.Delete
	require.Len(t, changes.ActiveActors.Delete, 1, "Expected 1 active actor to be deleted")
	require.Equal(t, "myactor", changes.ActiveActors.Delete[0].ActorType)
	require.Equal(t, "actor1", changes.ActiveActors.Delete[0].ActorID)

	// Verify Alarms.Set (lease released)
	require.Len(t, changes.Alarms.Set, 1, "Expected 1 alarm lease to be released")
	require.Nil(t, changes.Alarms.Set[0].Value.LeaseID)
	require.Nil(t, changes.Alarms.Set[0].Value.LeaseExpiration)
}

func TestPersistHook_CleanupUnhealthyHosts(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host
	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), req)
	require.NoError(t, err)

	// Place an actor
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	_, err = p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.NoError(t, err)

	// Advance clock past health check deadline
	//nolint:forcetypeassert
	p.Clock.(*clocktesting.FakeClock).Sleep(p.Cfg.HostHealthCheckDeadline + time.Second)

	mock.Reset()

	// Call cleanup
	p.Mu.Lock()
	changes := internal.NewChanges()
	p.CleanupUnhealthyHosts(changes)
	if !changes.IsEmpty() {
		err := p.PersistHook.PersistChanges(t.Context(), changes)
		require.NoError(t, err)
	}
	changes.Release()
	p.Mu.Unlock()

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	persistedChanges := calls[0]

	// Verify Hosts.Delete
	require.Len(t, persistedChanges.Hosts.Delete, 1)
	require.Equal(t, hostRes.HostID, persistedChanges.Hosts.Delete[0])

	// Verify HostActorTypes.Delete
	require.Len(t, persistedChanges.HostActorTypes.Delete, 1)

	// Verify ActiveActors.Delete
	require.Len(t, persistedChanges.ActiveActors.Delete, 1)
}

func TestPersistHook_CleanupExpiredState(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Set state with TTL
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	err := p.SetState(t.Context(), actorRef, []byte("data"), components.SetStateOpts{TTL: time.Minute})
	require.NoError(t, err)

	// Advance clock past TTL
	//nolint:forcetypeassert
	p.Clock.(*clocktesting.FakeClock).Sleep(2 * time.Minute)

	mock.Reset()

	// Call cleanup
	p.StateMu.Lock()
	changes := internal.NewChanges()
	p.CleanupExpiredState(changes)
	if !changes.IsEmpty() {
		err := p.PersistHook.PersistChanges(t.Context(), changes)
		require.NoError(t, err)
	}
	changes.Release()
	p.StateMu.Unlock()

	// Verify PersistChanges was called
	calls := mock.GetCalls()
	require.Len(t, calls, 1, "Expected exactly 1 PersistChanges call")

	persistedChanges := calls[0]

	// Verify ActorState.Delete
	require.Len(t, persistedChanges.ActorState.Delete, 1)
	require.Equal(t, "myactor", persistedChanges.ActorState.Delete[0].ActorType)
	require.Equal(t, "actor1", persistedChanges.ActorState.Delete[0].ActorID)
}

func TestPersistHook_Rollback_SetState(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Configure mock to return error
	mock.ErrFunc = func() error {
		return errors.New("simulated persistence error")
	}

	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	err := p.SetState(t.Context(), actorRef, []byte("data"), components.SetStateOpts{})

	// Should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated persistence error")

	// Verify state was not stored in memory
	_, err = p.GetState(t.Context(), actorRef)
	require.ErrorIs(t, err, components.ErrNoState)
}

func TestPersistHook_Rollback_RegisterHost(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Configure mock to return error
	mock.ErrFunc = func() error {
		return errors.New("simulated persistence error")
	}

	req := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}

	_, err := p.RegisterHost(t.Context(), req)

	// Should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated persistence error")

	// Verify host was not stored in memory
	p.Mu.RLock()
	require.Empty(t, p.Hosts, "Host should have been rolled back")
	require.Empty(t, p.HostsByAddress, "HostsByAddress should have been rolled back")
	require.Empty(t, p.HostActorTypes, "HostActorTypes should have been rolled back")
	p.Mu.RUnlock()
}

func TestPersistHook_Rollback_SetAlarm(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Configure mock to return error
	mock.ErrFunc = func() error {
		return errors.New("simulated persistence error")
	}

	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(time.Hour)

	err := p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})

	// Should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated persistence error")

	// Verify alarm was not stored in memory
	_, err = p.GetAlarm(t.Context(), alarmRef)
	require.ErrorIs(t, err, components.ErrNoAlarm)
}

func TestPersistHook_Rollback_FetchAndLeaseUpcomingAlarms(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host (this should succeed)
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	hostRes, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	// Set an alarm (this should also succeed)
	alarmRef := ref.AlarmRef{ActorType: "myactor", ActorID: "actor1", Name: "reminder"}
	dueTime := p.Clock.Now().Add(5 * time.Second)
	err = p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{AlarmProperties: ref.AlarmProperties{DueTime: dueTime}})
	require.NoError(t, err)

	// Now configure mock to return error
	mock.ErrFunc = func() error {
		return errors.New("simulated persistence error")
	}

	// Try to fetch and lease - should fail
	leases, err := p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})

	// Should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated persistence error")
	require.Nil(t, leases)

	// Verify actor was not placed
	p.Mu.RLock()
	require.Empty(t, p.ActiveActors, "ActiveActors should have been rolled back")
	p.Mu.RUnlock()

	// Verify alarm lease was not set
	alarmRes, err := p.GetAlarm(t.Context(), alarmRef)
	require.NoError(t, err)
	require.Equal(t, dueTime, alarmRes.DueTime)

	// Reset mock error
	mock.ErrFunc = nil

	// Now fetch should work
	leases, err = p.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{hostRes.HostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 1)
}

func TestPersistHook_Rollback_LookupActor(t *testing.T) {
	p, mock := initProviderWithMockHook(t)

	// Register a host (this should succeed)
	hostReq := components.RegisterHostReq{
		Address: "localhost:8080",
		ActorTypes: []components.ActorHostType{
			{ActorType: "myactor", IdleTimeout: time.Minute},
		},
	}
	_, err := p.RegisterHost(t.Context(), hostReq)
	require.NoError(t, err)

	// Now configure mock to return error
	mock.ErrFunc = func() error {
		return errors.New("simulated persistence error")
	}

	// Try to lookup actor - should fail
	actorRef := ref.ActorRef{ActorType: "myactor", ActorID: "actor1"}
	_, err = p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})

	// Should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated persistence error")

	// Verify actor was not placed
	p.Mu.RLock()
	require.Empty(t, p.ActiveActors, "ActiveActors should have been rolled back")
	p.Mu.RUnlock()
}
