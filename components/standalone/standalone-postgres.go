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
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

// StandalonePostgresBacked is an in-memory provider backed by PostgreSQL for persistence.
// All data is kept in memory for fast access, but changes are persisted to PostgreSQL
// so that state survives process restarts.
type StandalonePostgresBacked struct {
	*provider
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
	p, err := newProvider(log, providerOptions{
		ProviderOptions: opts.ProviderOptions,
		Clock:           opts.Clock,
		CleanupInterval: opts.CleanupInterval,
		PersistHook:     s, // StandalonePostgresBacked implements PersistHook
	}, providerConfig)
	if err != nil {
		return nil, err
	}
	s.provider = p

	return s, nil
}

func (s *StandalonePostgresBacked) Init(ctx context.Context) error {
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

func (s *StandalonePostgresBacked) loadHosts(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT host_id, host_address, host_last_health_check FROM hosts")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var h host
		if err := rows.Scan(&h.id, &h.address, &h.lastHealthCheck); err != nil {
			return err
		}
		s.hosts[h.id] = &h
		s.hostsByAddress[h.address] = h.id
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

func (s *StandalonePostgresBacked) loadActiveActors(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM active_actors")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var aa activeActor
		var idleTimeoutMs int64
		if err := rows.Scan(&aa.actorType, &aa.actorID, &aa.hostID, &idleTimeoutMs, &aa.activation); err != nil {
			return err
		}
		aa.idleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		key := newActorKey(aa.actorType, aa.actorID)
		s.activeActors[key] = &aa
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
		var a alarm
		var interval *string
		var ttl *time.Time
		var data []byte
		var leaseID *string
		var leaseExp *time.Time

		if err := rows.Scan(&a.id, &a.actorType, &a.actorID, &a.name, &a.dueTime,
			&interval, &ttl, &data, &leaseID, &leaseExp); err != nil {
			return err
		}

		if interval != nil {
			a.interval = *interval
		}
		if ttl != nil {
			a.ttl = ttl
		}
		if len(data) > 0 {
			a.data = data
		}
		if leaseID != nil {
			a.leaseID = leaseID
		}
		if leaseExp != nil {
			a.leaseExpiration = leaseExp
		}

		key := newAlarmKey(a.actorType, a.actorID, a.name)
		s.alarms[key] = &a
		s.alarmsByID[a.id] = &a
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
		var actorType, actorID string
		var data []byte
		var exp *time.Time

		if err := rows.Scan(&actorType, &actorID, &data, &exp); err != nil {
			return err
		}

		entry := &stateEntry{
			data:       data,
			expiration: exp,
		}

		key := newActorKey(actorType, actorID)
		s.actorState[key] = entry
	}

	return rows.Err()
}

// PersistChanges implements PersistHook.
func (s *StandalonePostgresBacked) PersistChanges(ctx context.Context, changes *changes) error {
	if changes.isEmpty() {
		return nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.db.Begin(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(queryCtx) //nolint:errcheck

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

	if err := tx.Commit(queryCtx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *StandalonePostgresBacked) persistHostChanges(ctx context.Context, tx pgx.Tx, changes *changes) error {
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
			h.id, h.address, h.lastHealthCheck.UTC())
		if err != nil {
			return fmt.Errorf("failed to insert host %s: %w", h.id, err)
		}
	}

	// Updates
	for _, h := range changes.Hosts.Update {
		_, err := tx.Exec(ctx,
			"UPDATE hosts SET host_address = $1, host_last_health_check = $2 WHERE host_id = $3",
			h.address, h.lastHealthCheck.UTC(), h.id)
		if err != nil {
			return fmt.Errorf("failed to update host %s: %w", h.id, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistHostActorTypeChanges(ctx context.Context, tx pgx.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.HostActorTypes.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM host_actor_types WHERE host_id = $1 AND actor_type = $2",
			key.hostID, key.actorType)
		if err != nil {
			return fmt.Errorf("failed to delete host actor type: %w", err)
		}
	}

	// Creates
	for _, hat := range changes.HostActorTypes.Create {
		_, err := tx.Exec(ctx,
			"INSERT INTO host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit) VALUES ($1, $2, $3, $4)",
			hat.hostID, hat.actorType, hat.idleTimeout.Milliseconds(), hat.concurrencyLimit)
		if err != nil {
			return fmt.Errorf("failed to insert host actor type: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActiveActorChanges(ctx context.Context, tx pgx.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.ActiveActors.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM active_actors WHERE actor_type = $1 AND actor_id = $2",
			key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to delete active actor: %w", err)
		}
	}

	// Creates
	for _, aa := range changes.ActiveActors.Create {
		_, err := tx.Exec(ctx,
			"INSERT INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation) VALUES ($1, $2, $3, $4, $5)",
			aa.actorType, aa.actorID, aa.hostID, aa.idleTimeout.Milliseconds(), aa.activation.UTC())
		if err != nil {
			return fmt.Errorf("failed to insert active actor: %w", err)
		}
	}

	// Updates
	for _, aa := range changes.ActiveActors.Update {
		_, err := tx.Exec(ctx,
			"UPDATE active_actors SET host_id = $1, actor_idle_timeout = $2, actor_activation = $3 WHERE actor_type = $4 AND actor_id = $5",
			aa.hostID, aa.idleTimeout.Milliseconds(), aa.activation.UTC(), aa.actorType, aa.actorID)
		if err != nil {
			return fmt.Errorf("failed to update active actor: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistAlarmChanges(ctx context.Context, tx pgx.Tx, changes *changes) error {
	// Deletes
	for _, alarmID := range changes.Alarms.Delete {
		_, err := tx.Exec(ctx, "DELETE FROM alarms WHERE alarm_id = $1", alarmID)
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
			ttlVal = a.ttl.UTC()
		}
		if a.leaseID != nil {
			leaseIDVal = *a.leaseID
		}
		if a.leaseExpiration != nil {
			leaseExpVal = a.leaseExpiration.UTC()
		}

		_, err := tx.Exec(ctx,
			`INSERT INTO alarms (alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			                     alarm_interval, alarm_ttl_time, alarm_data,
			                     alarm_lease_id, alarm_lease_expiration_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			a.id, a.actorType, a.actorID, a.name, a.dueTime.UTC(),
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
			ttlVal = a.ttl.UTC()
		}
		if a.leaseID != nil {
			leaseIDVal = *a.leaseID
		}
		if a.leaseExpiration != nil {
			leaseExpVal = a.leaseExpiration.UTC()
		}

		_, err := tx.Exec(ctx,
			`UPDATE alarms SET actor_type = $1, actor_id = $2, alarm_name = $3, alarm_due_time = $4,
			                   alarm_interval = $5, alarm_ttl_time = $6, alarm_data = $7,
			                   alarm_lease_id = $8, alarm_lease_expiration_time = $9
			 WHERE alarm_id = $10`,
			a.actorType, a.actorID, a.name, a.dueTime.UTC(),
			intervalVal, ttlVal, a.data, leaseIDVal, leaseExpVal, a.id)
		if err != nil {
			return fmt.Errorf("failed to update alarm %s: %w", a.id, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActorStateChanges(ctx context.Context, tx pgx.Tx, changes *changes) error {
	// Deletes
	for _, key := range changes.ActorState.Delete {
		_, err := tx.Exec(ctx,
			"DELETE FROM actor_state WHERE actor_type = $1 AND actor_id = $2",
			key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to delete actor state: %w", err)
		}
	}

	// Creates
	for key, entry := range changes.ActorState.Create {
		var expVal any
		if entry.expiration != nil {
			expVal = entry.expiration.UTC()
		}

		_, err := tx.Exec(ctx,
			"INSERT INTO actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time) VALUES ($1, $2, $3, $4)",
			key.actorType, key.actorID, entry.data, expVal)
		if err != nil {
			return fmt.Errorf("failed to insert actor state: %w", err)
		}
	}

	// Updates
	for key, entry := range changes.ActorState.Update {
		var expVal any
		if entry.expiration != nil {
			expVal = entry.expiration.UTC()
		}

		_, err := tx.Exec(ctx,
			"UPDATE actor_state SET actor_state_data = $1, actor_state_expiration_time = $2 WHERE actor_type = $3 AND actor_id = $4",
			entry.data, expVal, key.actorType, key.actorID)
		if err != nil {
			return fmt.Errorf("failed to update actor state: %w", err)
		}
	}

	return nil
}
