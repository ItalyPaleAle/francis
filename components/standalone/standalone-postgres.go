package standalone

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"github.com/italypaleale/go-sql-utils/migrations"
	postgresmigrations "github.com/italypaleale/go-sql-utils/migrations/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone/internal"
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

// StandalonePostgresBacked is an in-memory provider backed by PostgreSQL for persistence.
// All data is kept in memory for fast access, but changes are persisted to PostgreSQL
// so that state survives process restarts.
type StandalonePostgresBacked struct {
	*internal.Provider

	db      *pgxpool.Pool
	timeout time.Duration
	log     *slog.Logger
}

// StandalonePostgresOptions contains options for creating a StandalonePostgresBacked provider.
type StandalonePostgresOptions struct {
	components.ProviderOptions

	// DB is the PostgreSQL database connection pool.
	// Required.
	DB *pgxpool.Pool

	// Timeout for database queries.
	// Default is 5 seconds.
	Timeout time.Duration

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration
}

const defaultPostgresTimeout = 5 * time.Second

// NewStandalonePostgresBacked creates a new in-memory ActorProvider backed by PostgreSQL.
func NewStandalonePostgresBacked(log *slog.Logger, opts StandalonePostgresOptions, providerConfig components.ProviderConfig) (*StandalonePostgresBacked, error) {
	if opts.DB == nil {
		return nil, errors.New("DB is required")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultPostgresTimeout
	}

	s := &StandalonePostgresBacked{
		db:      opts.DB,
		timeout: timeout,
		log:     log,
	}

	// Create the core provider with this as the persistence hook
	p, err := internal.NewProvider(log, internal.ProviderOptions{
		ProviderOptions: opts.ProviderOptions,
		Clock:           opts.Clock,
		CleanupInterval: opts.CleanupInterval,
		PersistHook:     s, // StandalonePostgresBacked implements PersistHook
	}, providerConfig)
	if err != nil {
		return nil, err
	}
	s.Provider = p

	return s, nil
}

func (s *StandalonePostgresBacked) Init(ctx context.Context) error {
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

func (s *StandalonePostgresBacked) runMigrations(ctx context.Context) error {
	m := postgresmigrations.Migrations{
		DB:                s.db,
		MetadataTableName: "metadata",
		MetadataKey:       "migrations-version",
	}

	// Get all migration scripts
	entries, err := postgresMigrations.ReadDir("migrations/postgres")
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
		data, err := postgresMigrations.ReadFile(filepath.Join("migrations/postgres", name))
		if err != nil {
			return fmt.Errorf("error reading migration script '%s': %w", name, err)
		}

		migrationFns[i] = func(ctx context.Context) error {
			s.log.InfoContext(ctx, "Performing Postgres database migration", slog.String("migration", name))
			_, err := m.DB.Exec(ctx, string(data))
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

func (s *StandalonePostgresBacked) loadFromDB(ctx context.Context) error {
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

func (s *StandalonePostgresBacked) loadHosts(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT host_id, host_address, host_last_health_check FROM hosts")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var h internal.Host
		err := rows.Scan(&h.ID, &h.Address, &h.LastHealthCheck)
		if err != nil {
			return err
		}
		s.Hosts[h.ID] = &h
		s.HostsByAddress[h.Address] = h.ID
	}

	return rows.Err()
}

func (s *StandalonePostgresBacked) loadHostActorTypes(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM host_actor_types")
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

func (s *StandalonePostgresBacked) loadActiveActors(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM active_actors")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			aa            internal.ActiveActor
			idleTimeoutMs int64
		)
		err := rows.Scan(&aa.ActorType, &aa.ActorID, &aa.HostID, &idleTimeoutMs, &aa.Activation)
		if err != nil {
			return err
		}
		aa.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		key := internal.NewActorKey(aa.ActorType, aa.ActorID)
		s.ActiveActors[key] = &aa
	}

	return rows.Err()
}

func (s *StandalonePostgresBacked) loadAlarms(ctx context.Context) error {
	rows, err := s.db.Query(ctx, `
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
		var (
			a        internal.Alarm
			interval *string
			ttl      *time.Time
			data     []byte
			leaseID  *string
			leaseExp *time.Time
		)

		err := rows.Scan(
			&a.ID, &a.ActorType, &a.ActorID, &a.Name, &a.DueTime,
			&interval, &ttl, &data, &leaseID, &leaseExp,
		)
		if err != nil {
			return err
		}

		if interval != nil {
			a.Interval = *interval
		}
		if ttl != nil {
			a.TTL = ttl
		}
		if len(data) > 0 {
			a.Data = data
		}
		if leaseID != nil {
			a.LeaseID = leaseID
		}
		if leaseExp != nil {
			a.LeaseExpiration = leaseExp
		}

		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		s.Alarms[key] = &a
		s.AlarmsByID[a.ID] = &a
	}

	return rows.Err()
}

func (s *StandalonePostgresBacked) loadActorState(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time FROM actor_state")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			actorType, actorID string
			data               []byte
			exp                *time.Time
		)

		err := rows.Scan(&actorType, &actorID, &data, &exp)
		if err != nil {
			return err
		}

		entry := &internal.StateEntry{
			Data:       data,
			Expiration: exp,
		}

		key := internal.NewActorKey(actorType, actorID)
		s.ActorState[key] = entry
	}

	return rows.Err()
}

// PersistChanges implements PersistHook.
func (s *StandalonePostgresBacked) PersistChanges(ctx context.Context, changes *internal.Changes) error {
	if changes.IsEmpty() {
		return nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.db.Begin(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(ctx, s.timeout)
		rollbackErr := tx.Rollback(rollbackCtx)
		rollbackCancel()
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

	err = tx.Commit(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *StandalonePostgresBacked) persistHostChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, hostID := range changes.Hosts.Delete {
		_, err := tx.Exec(ctx, "DELETE FROM hosts WHERE host_id = $1", hostID)
		if err != nil {
			return fmt.Errorf("failed to delete host %s: %w", hostID, err)
		}
	}

	// Creates
	for _, h := range changes.Hosts.Create {
		_, err := tx.Exec(ctx,
			"INSERT INTO hosts (host_id, host_address, host_last_health_check) VALUES ($1, $2, $3)",
			h.ID, h.Address, h.LastHealthCheck.UTC(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert host %s: %w", h.ID, err)
		}
	}

	// Updates
	for _, h := range changes.Hosts.Update {
		_, err := tx.Exec(ctx,
			"UPDATE hosts SET host_address = $1, host_last_health_check = $2 WHERE host_id = $3",
			h.Address, h.LastHealthCheck.UTC(), h.ID,
		)
		if err != nil {
			return fmt.Errorf("failed to update host %s: %w", h.ID, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistHostActorTypeChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.HostActorTypes.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM host_actor_types WHERE host_id = $1 AND actor_type = $2",
			key.HostID, key.ActorType,
		)
		if err != nil {
			return fmt.Errorf("failed to delete host actor type: %w", err)
		}
	}

	// Creates
	for _, hat := range changes.HostActorTypes.Create {
		_, err := tx.Exec(ctx,
			"INSERT INTO host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit) VALUES ($1, $2, $3, $4)",
			hat.HostID, hat.ActorType, hat.IdleTimeout.Milliseconds(), hat.ConcurrencyLimit,
		)
		if err != nil {
			return fmt.Errorf("failed to insert host actor type: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActiveActorChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActiveActors.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM active_actors WHERE actor_type = $1 AND actor_id = $2",
			key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete active actor: %w", err)
		}
	}

	// Creates
	for _, aa := range changes.ActiveActors.Create {
		_, err := tx.Exec(ctx,
			"INSERT INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation) VALUES ($1, $2, $3, $4, $5)",
			aa.ActorType, aa.ActorID, aa.HostID, aa.IdleTimeout.Milliseconds(), aa.Activation.UTC(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert active actor: %w", err)
		}
	}

	// Updates
	for _, aa := range changes.ActiveActors.Update {
		_, err := tx.Exec(ctx,
			"UPDATE active_actors SET host_id = $1, actor_idle_timeout = $2, actor_activation = $3 WHERE actor_type = $4 AND actor_id = $5",
			aa.HostID, aa.IdleTimeout.Milliseconds(), aa.Activation.UTC(), aa.ActorType, aa.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to update active actor: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistAlarmChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, alarmID := range changes.Alarms.Delete {
		_, err := tx.Exec(ctx, "DELETE FROM alarms WHERE alarm_id = $1", alarmID)
		if err != nil {
			return fmt.Errorf("failed to delete alarm %s: %w", alarmID, err)
		}
	}

	// Creates
	for _, a := range changes.Alarms.Create {
		var (
			intervalVal, leaseIDVal any
			ttlVal, leaseExpVal     any
		)

		if a.Interval != "" {
			intervalVal = a.Interval
		}
		if a.TTL != nil {
			ttlVal = a.TTL.UTC()
		}
		if a.LeaseID != nil {
			leaseIDVal = *a.LeaseID
		}
		if a.LeaseExpiration != nil {
			leaseExpVal = a.LeaseExpiration.UTC()
		}

		_, err := tx.Exec(ctx,
			`INSERT INTO alarms (
				alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			    alarm_interval, alarm_ttl_time, alarm_data,
			    alarm_lease_id, alarm_lease_expiration_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			a.ID, a.ActorType, a.ActorID, a.Name, a.DueTime.UTC(),
			intervalVal, ttlVal, a.Data, leaseIDVal, leaseExpVal,
		)
		if err != nil {
			return fmt.Errorf("failed to insert alarm %s: %w", a.ID, err)
		}
	}

	// Updates
	for _, a := range changes.Alarms.Update {
		var (
			intervalVal, leaseIDVal any
			ttlVal, leaseExpVal     any
		)

		if a.Interval != "" {
			intervalVal = a.Interval
		}
		if a.TTL != nil {
			ttlVal = a.TTL.UTC()
		}
		if a.LeaseID != nil {
			leaseIDVal = *a.LeaseID
		}
		if a.LeaseExpiration != nil {
			leaseExpVal = a.LeaseExpiration.UTC()
		}

		_, err := tx.Exec(ctx,
			`UPDATE alarms SET
				actor_type = $1, actor_id = $2, alarm_name = $3, alarm_due_time = $4,
			    alarm_interval = $5, alarm_ttl_time = $6, alarm_data = $7,
			    alarm_lease_id = $8, alarm_lease_expiration_time = $9
			WHERE alarm_id = $10`,
			a.ActorType, a.ActorID, a.Name, a.DueTime.UTC(),
			intervalVal, ttlVal, a.Data, leaseIDVal, leaseExpVal, a.ID,
		)
		if err != nil {
			return fmt.Errorf("failed to update alarm %s: %w", a.ID, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActorStateChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActorState.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM actor_state WHERE actor_type = $1 AND actor_id = $2",
			key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete actor state: %w", err)
		}
	}

	// Creates
	for key, entry := range changes.ActorState.Create {
		var expVal any
		if entry.Expiration != nil {
			expVal = entry.Expiration.UTC()
		}

		_, err := tx.Exec(ctx,
			"INSERT INTO actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time) VALUES ($1, $2, $3, $4)",
			key.ActorType, key.ActorID, entry.Data, expVal,
		)
		if err != nil {
			return fmt.Errorf("failed to insert actor state: %w", err)
		}
	}

	// Updates
	for key, entry := range changes.ActorState.Update {
		var expVal any
		if entry.Expiration != nil {
			expVal = entry.Expiration.UTC()
		}

		_, err := tx.Exec(ctx,
			"UPDATE actor_state SET actor_state_data = $1, actor_state_expiration_time = $2 WHERE actor_type = $3 AND actor_id = $4",
			entry.Data, expVal, key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to update actor state: %w", err)
		}
	}

	return nil
}
