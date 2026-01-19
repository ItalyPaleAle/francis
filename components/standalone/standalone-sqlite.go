package standalone

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"github.com/italypaleale/go-sql-utils/migrations"
	sqlitemigrations "github.com/italypaleale/go-sql-utils/migrations/sqlite"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
)

//go:embed migrations/sqlite/*.sql
var sqliteMigrations embed.FS

// StandaloneSQLiteBacked is an in-memory provider backed by SQLite for persistence.
// All data is kept in memory for fast access, but changes are persisted to SQLite
// so that state survives process restarts.
type StandaloneSQLiteBacked struct {
	*provider
	db      *sql.DB
	timeout time.Duration
	log     *slog.Logger
}

// StandaloneSQLiteOptions contains options for creating a StandaloneSQLiteBacked provider.
type StandaloneSQLiteOptions struct {
	components.ProviderOptions

	// DB is the SQL database connection.
	// Required.
	DB *sql.DB

	// Timeout for database queries.
	// Default is 5 seconds.
	Timeout time.Duration

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration
}

const defaultSQLiteTimeout = 5 * time.Second

// NewStandaloneSQLiteBacked creates a new in-memory ActorProvider backed by SQLite.
func NewStandaloneSQLiteBacked(log *slog.Logger, opts StandaloneSQLiteOptions, providerConfig components.ProviderConfig) (*StandaloneSQLiteBacked, error) {
	if opts.DB == nil {
		return nil, errors.New("DB is required")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultSQLiteTimeout
	}

	s := &StandaloneSQLiteBacked{
		db:      opts.DB,
		timeout: timeout,
		log:     log,
	}

	// Create the core provider with this as the persistence hook
	p, err := newProvider(log, providerOptions{
		ProviderOptions: opts.ProviderOptions,
		Clock:           opts.Clock,
		CleanupInterval: opts.CleanupInterval,
		PersistHook:     s, // StandaloneSQLiteBacked implements PersistHook
	}, providerConfig)
	if err != nil {
		return nil, err
	}
	s.provider = p

	return s, nil
}

func (s *StandaloneSQLiteBacked) Init(ctx context.Context) error {
	// Run migrations
	if err := s.runMigrations(ctx); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	// Load all data from DB into memory
	if err := s.loadFromDB(ctx); err != nil {
		return fmt.Errorf("failed to load data from database: %w", err)
	}

	return nil
}

func (s *StandaloneSQLiteBacked) runMigrations(ctx context.Context) error {
	m := sqlitemigrations.Migrations{
		Pool:              s.db,
		MetadataTableName: "metadata",
		MetadataKey:       "migrations-version",
	}

	// Get all migration scripts
	entries, err := sqliteMigrations.ReadDir("migrations/sqlite")
	if err != nil {
		return fmt.Errorf("error while loading migration scripts: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	slices.Sort(names)

	migrationFns := make([]migrations.MigrationFn, len(names))
	for i, name := range names {
		data, err := sqliteMigrations.ReadFile(filepath.Join("migrations/sqlite", name))
		if err != nil {
			return fmt.Errorf("error reading migration script '%s': %w", name, err)
		}

		migrationFns[i] = func(ctx context.Context) error {
			s.log.InfoContext(ctx, "Performing SQLite database migration", slog.String("migration", name))
			_, err := m.GetConn().ExecContext(ctx, string(data))
			if err != nil {
				return fmt.Errorf("failed to perform migration '%s': %w", name, err)
			}
			return nil
		}
	}

	// Execute the migrations
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	return m.Perform(queryCtx, migrationFns, s.log)
}

func (s *StandaloneSQLiteBacked) loadFromDB(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Load hosts
	if err := s.loadHosts(queryCtx); err != nil {
		return fmt.Errorf("failed to load hosts: %w", err)
	}

	// Load host actor types
	if err := s.loadHostActorTypes(queryCtx); err != nil {
		return fmt.Errorf("failed to load host actor types: %w", err)
	}

	// Load active actors
	if err := s.loadActiveActors(queryCtx); err != nil {
		return fmt.Errorf("failed to load active actors: %w", err)
	}

	// Load alarms
	if err := s.loadAlarms(queryCtx); err != nil {
		return fmt.Errorf("failed to load alarms: %w", err)
	}

	// Load actor state
	if err := s.loadActorState(queryCtx); err != nil {
		return fmt.Errorf("failed to load actor state: %w", err)
	}

	return nil
}

func (s *StandaloneSQLiteBacked) loadHosts(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT host_id, host_address, host_last_health_check FROM hosts")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var h host
		var healthCheckMs int64
		if err := rows.Scan(&h.id, &h.address, &healthCheckMs); err != nil {
			return err
		}
		h.lastHealthCheck = time.UnixMilli(healthCheckMs)
		s.hosts[h.id] = &h
		s.hostsByAddress[h.address] = h.id
	}

	return rows.Err()
}

func (s *StandaloneSQLiteBacked) loadHostActorTypes(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM host_actor_types")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var hat hostActorType
		var idleTimeoutMs int64
		if err := rows.Scan(&hat.hostID, &hat.actorType, &idleTimeoutMs, &hat.concurrencyLimit); err != nil {
			return err
		}
		hat.idleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		if s.hostActorTypes[hat.hostID] == nil {
			s.hostActorTypes[hat.hostID] = make([]*hostActorType, 0)
		}
		s.hostActorTypes[hat.hostID] = append(s.hostActorTypes[hat.hostID], &hat)
	}

	return rows.Err()
}

func (s *StandaloneSQLiteBacked) loadActiveActors(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM active_actors")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var aa activeActor
		var idleTimeoutMs, activationMs int64
		if err := rows.Scan(&aa.actorType, &aa.actorID, &aa.hostID, &idleTimeoutMs, &activationMs); err != nil {
			return err
		}
		aa.idleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		aa.activation = time.UnixMilli(activationMs)
		key := newActorKey(aa.actorType, aa.actorID)
		s.activeActors[key] = &aa
	}

	return rows.Err()
}

func (s *StandaloneSQLiteBacked) loadAlarms(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
		       alarm_interval, alarm_ttl_time, alarm_data,
		       alarm_lease_id, alarm_lease_expiration_time
		FROM alarms
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var a alarm
		var dueTimeMs int64
		var interval sql.NullString
		var ttlMs sql.NullInt64
		var data []byte
		var leaseID sql.NullString
		var leaseExpMs sql.NullInt64

		if err := rows.Scan(&a.id, &a.actorType, &a.actorID, &a.name, &dueTimeMs,
			&interval, &ttlMs, &data, &leaseID, &leaseExpMs); err != nil {
			return err
		}

		a.dueTime = time.UnixMilli(dueTimeMs)
		if interval.Valid {
			a.interval = interval.String
		}
		if ttlMs.Valid {
			t := time.UnixMilli(ttlMs.Int64)
			a.ttl = &t
		}
		if len(data) > 0 {
			a.data = data
		}
		if leaseID.Valid {
			a.leaseID = &leaseID.String
		}
		if leaseExpMs.Valid {
			t := time.UnixMilli(leaseExpMs.Int64)
			a.leaseExpiration = &t
		}

		key := newAlarmKey(a.actorType, a.actorID, a.name)
		s.alarms[key] = &a
		s.alarmsByID[a.id] = &a
	}

	return rows.Err()
}

func (s *StandaloneSQLiteBacked) loadActorState(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time FROM actor_state")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var actorType, actorID string
		var data []byte
		var expMs sql.NullInt64

		if err := rows.Scan(&actorType, &actorID, &data, &expMs); err != nil {
			return err
		}

		entry := &stateEntry{
			data: data,
		}
		if expMs.Valid {
			t := time.UnixMilli(expMs.Int64)
			entry.expiration = &t
		}

		key := newActorKey(actorType, actorID)
		s.actorState[key] = entry
	}

	return rows.Err()
}

// PersistChanges implements PersistHook.
func (s *StandaloneSQLiteBacked) PersistChanges(ctx context.Context, changes *changes) error {
	if changes.isEmpty() {
		return nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.db.BeginTx(queryCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Process host changes
	if err := s.persistHostChanges(queryCtx, tx, changes); err != nil {
		return err
	}

	// Process host actor type changes
	if err := s.persistHostActorTypeChanges(queryCtx, tx, changes); err != nil {
		return err
	}

	// Process active actor changes
	if err := s.persistActiveActorChanges(queryCtx, tx, changes); err != nil {
		return err
	}

	// Process alarm changes
	if err := s.persistAlarmChanges(queryCtx, tx, changes); err != nil {
		return err
	}

	// Process actor state changes
	if err := s.persistActorStateChanges(queryCtx, tx, changes); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistHostChanges(ctx context.Context, tx *sql.Tx, changes *changes) error {
	// Deletes
	for _, hostID := range changes.Hosts.Delete {
		_, err := tx.ExecContext(ctx, "DELETE FROM hosts WHERE host_id = ?", hostID)
		if err != nil {
			return fmt.Errorf("failed to delete host %s: %w", hostID, err)
		}
	}

	// Creates
	for _, h := range changes.Hosts.Create {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO hosts (host_id, host_address, host_last_health_check) VALUES (?, ?, ?)",
			h.id, h.address, h.lastHealthCheck.UnixMilli())
		if err != nil {
			return fmt.Errorf("failed to insert host %s: %w", h.id, err)
		}
	}

	// Updates
	for _, h := range changes.Hosts.Update {
		_, err := tx.ExecContext(ctx,
			"UPDATE hosts SET host_address = ?, host_last_health_check = ? WHERE host_id = ?",
			h.address, h.lastHealthCheck.UnixMilli(), h.id)
		if err != nil {
			return fmt.Errorf("failed to update host %s: %w", h.id, err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistHostActorTypeChanges(ctx context.Context, tx *sql.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.HostActorTypes.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM host_actor_types WHERE host_id = ? AND actor_type = ?",
			key.hostID, key.actorType)
		if err != nil {
			return fmt.Errorf("failed to delete host actor type: %w", err)
		}
	}

	// Creates
	for _, hat := range changes.HostActorTypes.Create {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit) VALUES (?, ?, ?, ?)",
			hat.hostID, hat.actorType, hat.idleTimeout.Milliseconds(), hat.concurrencyLimit)
		if err != nil {
			return fmt.Errorf("failed to insert host actor type: %w", err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistActiveActorChanges(ctx context.Context, tx *sql.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.ActiveActors.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM active_actors WHERE actor_type = ? AND actor_id = ?",
			key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to delete active actor: %w", err)
		}
	}

	// Creates
	for _, aa := range changes.ActiveActors.Create {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation) VALUES (?, ?, ?, ?, ?)",
			aa.actorType, aa.actorID, aa.hostID, aa.idleTimeout.Milliseconds(), aa.activation.UnixMilli())
		if err != nil {
			return fmt.Errorf("failed to insert active actor: %w", err)
		}
	}

	// Updates
	for _, aa := range changes.ActiveActors.Update {
		_, err := tx.ExecContext(ctx,
			"UPDATE active_actors SET host_id = ?, actor_idle_timeout = ?, actor_activation = ? WHERE actor_type = ? AND actor_id = ?",
			aa.hostID, aa.idleTimeout.Milliseconds(), aa.activation.UnixMilli(), aa.actorType, aa.actorID)
		if err != nil {
			return fmt.Errorf("failed to update active actor: %w", err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistAlarmChanges(ctx context.Context, tx *sql.Tx, changes *changes) error {
	// Deletes
	for _, alarmID := range changes.Alarms.Delete {
		_, err := tx.ExecContext(ctx, "DELETE FROM alarms WHERE alarm_id = ?", alarmID)
		if err != nil {
			return fmt.Errorf("failed to delete alarm %s: %w", alarmID, err)
		}
	}

	// Creates
	for _, a := range changes.Alarms.Create {
		var intervalVal, leaseIDVal any
		var ttlVal, leaseExpVal any

		if a.interval != "" {
			intervalVal = a.interval
		}
		if a.ttl != nil {
			ttlVal = a.ttl.UnixMilli()
		}
		if a.leaseID != nil {
			leaseIDVal = *a.leaseID
		}
		if a.leaseExpiration != nil {
			leaseExpVal = a.leaseExpiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			`INSERT INTO alarms (alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			                     alarm_interval, alarm_ttl_time, alarm_data,
			                     alarm_lease_id, alarm_lease_expiration_time)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.id, a.actorType, a.actorID, a.name, a.dueTime.UnixMilli(),
			intervalVal, ttlVal, a.data, leaseIDVal, leaseExpVal)
		if err != nil {
			return fmt.Errorf("failed to insert alarm %s: %w", a.id, err)
		}
	}

	// Updates
	for _, a := range changes.Alarms.Update {
		var intervalVal, leaseIDVal any
		var ttlVal, leaseExpVal any

		if a.interval != "" {
			intervalVal = a.interval
		}
		if a.ttl != nil {
			ttlVal = a.ttl.UnixMilli()
		}
		if a.leaseID != nil {
			leaseIDVal = *a.leaseID
		}
		if a.leaseExpiration != nil {
			leaseExpVal = a.leaseExpiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			`UPDATE alarms SET actor_type = ?, actor_id = ?, alarm_name = ?, alarm_due_time = ?,
			                   alarm_interval = ?, alarm_ttl_time = ?, alarm_data = ?,
			                   alarm_lease_id = ?, alarm_lease_expiration_time = ?
			 WHERE alarm_id = ?`,
			a.actorType, a.actorID, a.name, a.dueTime.UnixMilli(),
			intervalVal, ttlVal, a.data, leaseIDVal, leaseExpVal, a.id)
		if err != nil {
			return fmt.Errorf("failed to update alarm %s: %w", a.id, err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistActorStateChanges(ctx context.Context, tx *sql.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.ActorState.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM actor_state WHERE actor_type = ? AND actor_id = ?",
			key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to delete actor state: %w", err)
		}
	}

	// Creates
	for key, entry := range changes.ActorState.Create {
		var expVal any
		if entry.expiration != nil {
			expVal = entry.expiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			"INSERT INTO actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time) VALUES (?, ?, ?, ?)",
			key.actorType, key.actorID, entry.data, expVal)
		if err != nil {
			return fmt.Errorf("failed to insert actor state: %w", err)
		}
	}

	// Updates
	for key, entry := range changes.ActorState.Update {
		var expVal any
		if entry.expiration != nil {
			expVal = entry.expiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			"UPDATE actor_state SET actor_state_data = ?, actor_state_expiration_time = ? WHERE actor_type = ? AND actor_id = ?",
			entry.data, expVal, key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to update actor state: %w", err)
		}
	}

	return nil
}
