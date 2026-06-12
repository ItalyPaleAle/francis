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
	"github.com/italypaleale/francis/components/standalone/internal"
)

//go:embed migrations/sqlite/*.sql
var sqliteMigrations embed.FS

// StandaloneSQLiteBacked is an in-memory provider backed by SQLite for persistence.
// All data is kept in memory for fast access, but changes are persisted to SQLite
// so that state survives process restarts.
type StandaloneSQLiteBacked struct {
	*internal.Provider

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

	// Interval at which to purge expired state from memory
	// Default is 5 minutes
	// Set to a negative value to disable
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
	p, err := internal.NewProvider(log, internal.ProviderOptions{
		ProviderOptions: opts.ProviderOptions,
		Clock:           opts.Clock,
		CleanupInterval: opts.CleanupInterval,
		PersistHook:     s,
	}, providerConfig)
	if err != nil {
		return nil, err
	}
	s.Provider = p

	return s, nil
}

func (s *StandaloneSQLiteBacked) Init(ctx context.Context) error {
	// Run migrations
	err := s.runMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	// Load all data from DB into memory
	err = s.loadFromDB(ctx)
	if err != nil {
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
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.StateMu.Lock()
	defer s.StateMu.Unlock()

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Load hosts
	err := s.loadHosts(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load hosts: %w", err)
	}

	// Load host actor types
	err = s.loadHostActorTypes(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load host actor types: %w", err)
	}

	// Load active actors
	err = s.loadActiveActors(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load active actors: %w", err)
	}

	// Load alarms
	err = s.loadAlarms(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load alarms: %w", err)
	}

	// Load actor state
	err = s.loadActorState(queryCtx)
	if err != nil {
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
		var (
			h             internal.Host
			healthCheckMs int64
		)
		err := rows.Scan(&h.ID, &h.Address, &healthCheckMs)
		if err != nil {
			return err
		}
		h.LastHealthCheck = time.UnixMilli(healthCheckMs)
		s.Hosts[h.ID] = &h
		s.HostsByAddress[h.Address] = h.ID
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
		var (
			hat           internal.HostActorType
			idleTimeoutMs int64
		)
		err := rows.Scan(&hat.HostID, &hat.ActorType, &idleTimeoutMs, &hat.ConcurrencyLimit)
		if err != nil {
			return err
		}
		hat.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		if s.HostActorTypes[hat.HostID] == nil {
			s.HostActorTypes[hat.HostID] = make([]*internal.HostActorType, 0)
		}
		s.HostActorTypes[hat.HostID] = append(s.HostActorTypes[hat.HostID], &hat)
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
		var (
			aa                          internal.ActiveActor
			idleTimeoutMs, activationMs int64
		)
		err := rows.Scan(&aa.ActorType, &aa.ActorID, &aa.HostID, &idleTimeoutMs, &activationMs)
		if err != nil {
			return err
		}
		aa.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		aa.Activation = time.UnixMilli(activationMs)
		key := internal.NewActorKey(aa.ActorType, aa.ActorID)
		s.ActiveActors[key] = &aa
	}

	return rows.Err()
}

func (s *StandaloneSQLiteBacked) loadAlarms(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			alarm_interval, alarm_ttl_time, alarm_data,
			alarm_lease_id, alarm_lease_expiration_time
		FROM alarms
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			a          internal.Alarm
			dueTimeMs  int64
			interval   sql.NullString
			ttlMs      sql.NullInt64
			data       []byte
			leaseID    sql.NullString
			leaseExpMs sql.NullInt64
		)

		err := rows.Scan(
			&a.ID, &a.ActorType, &a.ActorID, &a.Name, &dueTimeMs,
			&interval, &ttlMs, &data, &leaseID, &leaseExpMs,
		)
		if err != nil {
			return err
		}

		a.DueTime = time.UnixMilli(dueTimeMs)
		if interval.Valid {
			a.Interval = interval.String
		}
		if ttlMs.Valid {
			a.TTL = new(time.UnixMilli(ttlMs.Int64))
		}
		if len(data) > 0 {
			a.Data = data
		}
		if leaseID.Valid {
			a.LeaseID = &leaseID.String
		}
		if leaseExpMs.Valid {
			a.LeaseExpiration = new(time.UnixMilli(leaseExpMs.Int64))
		}

		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		s.Alarms[key] = &a
		s.AlarmsByID[a.ID] = &a
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
		var (
			actorType, actorID string
			data               []byte
			expMs              sql.NullInt64
		)

		err := rows.Scan(&actorType, &actorID, &data, &expMs)
		if err != nil {
			return err
		}

		entry := &internal.StateEntry{
			Data: data,
		}
		if expMs.Valid {
			t := time.UnixMilli(expMs.Int64)
			entry.Expiration = &t
		}

		key := internal.NewActorKey(actorType, actorID)
		s.ActorState[key] = entry
	}

	return rows.Err()
}

// PersistChanges implements PersistHook.
func (s *StandaloneSQLiteBacked) PersistChanges(ctx context.Context, changes *internal.Changes) error {
	if changes.IsEmpty() {
		return nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.db.BeginTx(queryCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	var committed bool
	defer func() {
		if committed {
			return
		}
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			s.log.WarnContext(ctx, "Error while rolling back transaction", slog.Any("error", rollbackErr))
		}
	}()

	// Process host changes
	err = s.persistHostChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process host actor type changes
	err = s.persistHostActorTypeChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process active actor changes
	err = s.persistActiveActorChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process alarm changes
	err = s.persistAlarmChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process actor state changes
	err = s.persistActorStateChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	return nil
}

func (s *StandaloneSQLiteBacked) persistHostChanges(ctx context.Context, tx *sql.Tx, changes *internal.Changes) error {
	// Deletes
	for _, hostID := range changes.Hosts.Delete {
		_, err := tx.ExecContext(ctx, "DELETE FROM hosts WHERE host_id = ?", hostID)
		if err != nil {
			return fmt.Errorf("failed to delete host %s: %w", hostID, err)
		}
	}

	// Upserts
	for _, hc := range changes.Hosts.Set {
		h := hc.Value
		_, err := tx.ExecContext(ctx,
			`REPLACE INTO hosts (host_id, host_address, host_last_health_check) VALUES (?, ?, ?)`,
			h.ID, h.Address, h.LastHealthCheck.UnixMilli(),
		)
		if err != nil {
			return fmt.Errorf("failed to upsert host %s: %w", h.ID, err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistHostActorTypeChanges(ctx context.Context, tx *sql.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.HostActorTypes.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM host_actor_types WHERE host_id = ? AND actor_type = ?",
			key.HostID, key.ActorType,
		)
		if err != nil {
			return fmt.Errorf("failed to delete host actor type: %w", err)
		}
	}

	// Upserts
	for _, hat := range changes.HostActorTypes.Set {
		_, err := tx.ExecContext(ctx,
			`REPLACE INTO host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit) VALUES (?, ?, ?, ?)`,
			hat.HostID, hat.ActorType, hat.IdleTimeout.Milliseconds(), hat.ConcurrencyLimit,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert host actor type: %w", err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistActiveActorChanges(ctx context.Context, tx *sql.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActiveActors.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM active_actors WHERE actor_type = ? AND actor_id = ?",
			key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete active actor: %w", err)
		}
	}

	// Upserts
	for _, aac := range changes.ActiveActors.Set {
		aa := aac.Value
		_, err := tx.ExecContext(ctx,
			`REPLACE INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation) VALUES (?, ?, ?, ?, ?)`,
			aa.ActorType, aa.ActorID, aa.HostID, aa.IdleTimeout.Milliseconds(), aa.Activation.UnixMilli(),
		)
		if err != nil {
			return fmt.Errorf("failed to upsert active actor: %w", err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistAlarmChanges(ctx context.Context, tx *sql.Tx, changes *internal.Changes) error {
	// Deletes
	for _, alarmID := range changes.Alarms.Delete {
		_, err := tx.ExecContext(ctx, "DELETE FROM alarms WHERE alarm_id = ?", alarmID)
		if err != nil {
			return fmt.Errorf("failed to delete alarm %s: %w", alarmID, err)
		}
	}

	// Upserts
	for _, ac := range changes.Alarms.Set {
		a := ac.Value
		var (
			intervalVal, leaseIDVal any
			ttlVal, leaseExpVal     any
		)

		if a.Interval != "" {
			intervalVal = a.Interval
		}
		if a.TTL != nil {
			ttlVal = a.TTL.UnixMilli()
		}
		if a.LeaseID != nil {
			leaseIDVal = *a.LeaseID
		}
		if a.LeaseExpiration != nil {
			leaseExpVal = a.LeaseExpiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			`REPLACE INTO alarms (
				alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			    alarm_interval, alarm_ttl_time, alarm_data,
			    alarm_lease_id, alarm_lease_expiration_time
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.ID, a.ActorType, a.ActorID, a.Name, a.DueTime.UnixMilli(),
			intervalVal, ttlVal, a.Data, leaseIDVal, leaseExpVal,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert alarm %s: %w", a.ID, err)
		}
	}

	return nil
}

func (s *StandaloneSQLiteBacked) persistActorStateChanges(ctx context.Context, tx *sql.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActorState.Delete {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM actor_state WHERE actor_type = ? AND actor_id = ?",
			key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete actor state: %w", err)
		}
	}

	// Upserts
	for _, asc := range changes.ActorState.Set {
		key := asc.Key
		entry := asc.Value
		var expVal any
		if entry.Expiration != nil {
			expVal = entry.Expiration.UnixMilli()
		}

		_, err := tx.ExecContext(ctx,
			`REPLACE INTO actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time) VALUES (?, ?, ?, ?)`,
			key.ActorType, key.ActorID, entry.Data, expVal,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert actor state: %w", err)
		}
	}

	return nil
}
