package standalone

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
	_ "modernc.org/sqlite"

	"github.com/italypaleale/francis/components/standalone/internal"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/ptr"
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

func initSQLiteTestProvider(t *testing.T) *StandaloneSQLiteBacked {
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

	providerOpts := StandaloneSQLiteOptions{
		DB:    db,
		Clock: clock,
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
		DB:    conn,
		Clock: clock,
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

// CleanupExpired performs garbage collection of expired records.
func (p *StandaloneMemory) CleanupExpired() error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	changes := internal.NewChanges()

	// Clean up unhealthy hosts
	p.CleanupUnhealthyHosts(changes)

	// Clean up expired state
	p.CleanupExpiredState(changes)

	return nil
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
			alm.TTL = ptr.Of(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = ptr.Of(uuid.New().String())
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
			as.LeaseExp = ptr.Of(*a.LeaseExpiration)
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}

// Test helper methods for StandaloneSQLiteBacked

// CleanupExpired performs garbage collection of expired records.
func (p *StandaloneSQLiteBacked) CleanupExpired() error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	changes := internal.NewChanges()

	// Clean up unhealthy hosts
	p.CleanupUnhealthyHosts(changes)

	// Clean up expired state
	p.CleanupExpiredState(changes)

	return nil
}

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
		_, err := p.db.ExecContext(ctx, "DELETE FROM "+table)
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
			alm.TTL = ptr.Of(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = ptr.Of(uuid.New().String())
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
			as.LeaseExp = ptr.Of(*a.LeaseExpiration)
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}

// Test helper methods for StandalonePostgresBacked

// CleanupExpired performs garbage collection of expired records.
func (p *StandalonePostgresBacked) CleanupExpired() error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	changes := internal.NewChanges()

	// Clean up unhealthy hosts
	p.CleanupUnhealthyHosts(changes)

	// Clean up expired state
	p.CleanupExpiredState(changes)

	return nil
}

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
		_, err := p.db.Exec(ctx, "DELETE FROM "+table)
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
			alm.TTL = ptr.Of(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.LeaseExpiration = &leaseExp
			alm.LeaseID = ptr.Of(uuid.New().String())
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
			as.LeaseExp = ptr.Of(*a.LeaseExpiration)
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}
