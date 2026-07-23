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

	db          *pgxpool.Pool
	timeout     time.Duration
	log         *slog.Logger
	tablePrefix string
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

	// Interval at which to purge expired state from memory
	// Default is 5 minutes
	// Set to a negative value to disable
	CleanupInterval time.Duration

	// Prefix added to the name of every table (and other schema object) used by the provider
	// When set, tables are named "<prefix>_<table>", e.g. with prefix "francis" the hosts table is "francis_hosts"
	// Defaults to "francis" when empty
	TablePrefix string
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
		db:          opts.DB,
		timeout:     timeout,
		log:         log,
		tablePrefix: resolveTablePrefix(opts.TablePrefix),
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
		MetadataTableName: s.tablePrefix + "metadata",
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

		// Apply the table prefix to the script's "%s" placeholders
		script := applyTablePrefix(s.tablePrefix, string(data))

		migrationFns[i] = func(ctx context.Context) error {
			s.log.InfoContext(ctx, "Performing Postgres database migration", slog.String("migration", name))
			_, err := m.DB.Exec(ctx, script)
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

	// Load dead jobs
	err = s.loadDeadJobs(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load dead jobs: %w", err)
	}

	// Load actor state
	err = s.loadActorState(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load actor state: %w", err)
	}

	// Load the cluster-admission state
	err = s.loadClusterConfig(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to load cluster config: %w", err)
	}

	return nil
}

func (s *StandalonePostgresBacked) loadClusterConfig(ctx context.Context) error {
	var (
		maxHosts *int
		owner    *string
		expires  *int64
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := s.db.QueryRow(ctx,
		"SELECT max_hosts, exclusive_owner, exclusive_expires_at FROM "+s.tablePrefix+"cluster_config WHERE cluster_config_id = 1",
	).Scan(&maxHosts, &owner, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		// The row is seeded by a migration, so this is only reached if it was removed; treat it as an empty state
		return nil
	} else if err != nil {
		return err
	}

	if maxHosts != nil {
		v := *maxHosts
		s.Cluster.MaxHosts = &v
	}
	if owner != nil {
		s.Cluster.ExclusiveOwner = *owner
	}
	if expires != nil {
		s.Cluster.ExclusiveExpiresAt = *expires
	}
	return nil
}

func (s *StandalonePostgresBacked) loadHosts(ctx context.Context) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, "SELECT host_id, host_address, host_last_health_check FROM "+s.tablePrefix+"hosts")
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
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, "SELECT host_id, actor_type, actor_idle_timeout, actor_concurrency_limit FROM "+s.tablePrefix+"host_actor_types")
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
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, "SELECT actor_type, actor_id, host_id, actor_idle_timeout, actor_activation FROM "+s.tablePrefix+"active_actors")
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
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, `
		SELECT
			alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
			alarm_lease_id, alarm_lease_expiration_time, alarm_kind, job_method
		FROM `+s.tablePrefix+`alarms
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			a         internal.Alarm
			interval  *string
			cron      *string
			ttl       *time.Time
			data      []byte
			leaseID   *string
			leaseExp  *time.Time
			kind      *string
			jobMethod *string
		)

		err := rows.Scan(
			&a.ID, &a.ActorType, &a.ActorID, &a.Name, &a.DueTime,
			&interval, &cron, &ttl, &data, &leaseID, &leaseExp, &kind, &jobMethod,
		)
		if err != nil {
			return err
		}

		if interval != nil {
			a.Interval = *interval
		}
		if cron != nil {
			a.Cron = *cron
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
		if kind != nil {
			a.Kind = *kind
		}
		if jobMethod != nil {
			a.JobMethod = *jobMethod
		}

		key := internal.NewAlarmKey(a.ActorType, a.ActorID, a.Name)
		s.Alarms[key] = &a
		s.AlarmsByID[a.ID] = &a
	}

	return rows.Err()
}

func (s *StandalonePostgresBacked) loadDeadJobs(ctx context.Context) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, `
		SELECT
			job_id, actor_type, actor_id, job_method, job_data,
			attempts, last_error, failed_at, original_due, job_interval, job_cron
		FROM `+s.tablePrefix+`dead_jobs
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			d         internal.DeadJob
			data      []byte
			lastError *string
			interval  *string
			cron      *string
		)

		err := rows.Scan(
			&d.JobID, &d.ActorType, &d.ActorID, &d.Method, &data,
			&d.Attempts, &lastError, &d.FailedAt, &d.OriginalDue, &interval, &cron,
		)
		if err != nil {
			return err
		}

		if len(data) > 0 {
			d.Data = data
		}
		if lastError != nil {
			d.LastError = *lastError
		}
		if interval != nil {
			d.Interval = *interval
		}
		if cron != nil {
			d.Cron = *cron
		}

		s.DeadJobs[d.JobID] = &d
	}

	return rows.Err()
}

func (s *StandalonePostgresBacked) loadActorState(ctx context.Context) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.Query(ctx, "SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time FROM "+s.tablePrefix+"actor_state")
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
	var committed bool
	defer func() {
		if committed {
			return
		}
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

	// Process dead job changes
	err = s.persistDeadJobChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process actor state changes
	err = s.persistActorStateChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	// Process the cluster-admission state change
	err = s.persistClusterChanges(queryCtx, tx, changes)
	if err != nil {
		return err
	}

	err = tx.Commit(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	return nil
}

func (s *StandalonePostgresBacked) persistClusterChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	if changes.Cluster.Set == nil {
		return nil
	}
	st := changes.Cluster.Set

	var (
		maxHosts *int
		owner    *string
		expires  *int64
	)
	if st.MaxHosts != nil {
		v := *st.MaxHosts
		maxHosts = &v
	}
	// The lease columns are written together: both hold a value while a lease is held, and both are null otherwise
	if st.ExclusiveOwner != "" {
		owner = &st.ExclusiveOwner
		expires = &st.ExclusiveExpiresAt
	}

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := tx.Exec(ctx,
		`UPDATE `+s.tablePrefix+`cluster_config
		SET max_hosts = $1, exclusive_owner = $2, exclusive_expires_at = $3
		WHERE cluster_config_id = 1`,
		maxHosts, owner, expires,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert cluster config: %w", err)
	}
	return nil
}

func (s *StandalonePostgresBacked) persistHostChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, hostID := range changes.Hosts.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx, "DELETE FROM "+s.tablePrefix+"hosts WHERE host_id = $1", hostID)
		if err != nil {
			return fmt.Errorf("failed to delete host %s: %w", hostID, err)
		}
	}

	// Upserts
	for _, hc := range changes.Hosts.Set {
		h := hc.Value
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`hosts (host_id, host_address, host_last_health_check)
			VALUES ($1, $2, $3)
			ON CONFLICT(host_id) DO UPDATE SET
				host_address = EXCLUDED.host_address,
				host_last_health_check = EXCLUDED.host_last_health_check`,
			h.ID, h.Address, h.LastHealthCheck.UTC(),
		)
		if err != nil {
			return fmt.Errorf("failed to upsert host %s: %w", h.ID, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistHostActorTypeChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.HostActorTypes.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			"DELETE FROM "+s.tablePrefix+"host_actor_types WHERE host_id = $1 AND actor_type = $2",
			key.HostID, key.ActorType,
		)
		if err != nil {
			return fmt.Errorf("failed to delete host actor type: %w", err)
		}
	}

	// Upserts
	for _, hat := range changes.HostActorTypes.Set {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`host_actor_types (host_id, actor_type, actor_idle_timeout, actor_concurrency_limit)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT(host_id, actor_type) DO UPDATE SET
				actor_idle_timeout = EXCLUDED.actor_idle_timeout,
				actor_concurrency_limit = EXCLUDED.actor_concurrency_limit`,
			hat.HostID, hat.ActorType, hat.IdleTimeout.Milliseconds(), hat.ConcurrencyLimit,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert host actor type: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActiveActorChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActiveActors.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			"DELETE FROM "+s.tablePrefix+"active_actors WHERE actor_type = $1 AND actor_id = $2",
			key.ActorType, key.ActorID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete active actor: %w", err)
		}
	}

	// Upserts
	for _, aac := range changes.ActiveActors.Set {
		aa := aac.Value
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT(actor_type, actor_id) DO UPDATE SET
				host_id = EXCLUDED.host_id,
				actor_idle_timeout = EXCLUDED.actor_idle_timeout,
				actor_activation = EXCLUDED.actor_activation`,
			aa.ActorType, aa.ActorID, aa.HostID, aa.IdleTimeout.Milliseconds(), aa.Activation.UTC(),
		)
		if err != nil {
			return fmt.Errorf("failed to upsert active actor: %w", err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistAlarmChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, alarmID := range changes.Alarms.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx, "DELETE FROM "+s.tablePrefix+"alarms WHERE alarm_id = $1", alarmID)
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
			cronVal, jobMethodVal   any
		)

		if a.Interval != "" {
			intervalVal = a.Interval
		}
		if a.Cron != "" {
			cronVal = a.Cron
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
		kind := a.Kind
		if kind == "" {
			kind = "alarm"
		}
		if a.JobMethod != "" {
			jobMethodVal = a.JobMethod
		}

		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`alarms (
				alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			    alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
			    alarm_lease_id, alarm_lease_expiration_time, alarm_kind, job_method)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT(alarm_id) DO UPDATE SET
				actor_type = EXCLUDED.actor_type,
				actor_id = EXCLUDED.actor_id,
				alarm_name = EXCLUDED.alarm_name,
				alarm_due_time = EXCLUDED.alarm_due_time,
			    alarm_interval = EXCLUDED.alarm_interval,
				alarm_cron = EXCLUDED.alarm_cron,
				alarm_ttl_time = EXCLUDED.alarm_ttl_time,
				alarm_data = EXCLUDED.alarm_data,
			    alarm_lease_id = EXCLUDED.alarm_lease_id,
				alarm_lease_expiration_time = EXCLUDED.alarm_lease_expiration_time,
				alarm_kind = EXCLUDED.alarm_kind,
				job_method = EXCLUDED.job_method`,
			a.ID, a.ActorType, a.ActorID, a.Name, a.DueTime.UTC(),
			intervalVal, cronVal, ttlVal, a.Data, leaseIDVal, leaseExpVal, kind, jobMethodVal,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert alarm %s: %w", a.ID, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistDeadJobChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, jobID := range changes.DeadJobs.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx, "DELETE FROM "+s.tablePrefix+"dead_jobs WHERE job_id = $1", jobID)
		if err != nil {
			return fmt.Errorf("failed to delete dead job %s: %w", jobID, err)
		}
	}

	// Upserts
	for _, dc := range changes.DeadJobs.Set {
		d := dc.Value
		var (
			lastErrorVal         any
			intervalVal, cronVal any
		)
		if d.LastError != "" {
			lastErrorVal = d.LastError
		}
		if d.Interval != "" {
			intervalVal = d.Interval
		}
		if d.Cron != "" {
			cronVal = d.Cron
		}

		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`dead_jobs (
				job_id, actor_type, actor_id, job_method, job_data,
				attempts, last_error, failed_at, original_due, job_interval, job_cron)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT(job_id) DO UPDATE SET
				actor_type = EXCLUDED.actor_type,
				actor_id = EXCLUDED.actor_id,
				job_method = EXCLUDED.job_method,
				job_data = EXCLUDED.job_data,
				attempts = EXCLUDED.attempts,
				last_error = EXCLUDED.last_error,
				failed_at = EXCLUDED.failed_at,
				original_due = EXCLUDED.original_due,
				job_interval = EXCLUDED.job_interval,
				job_cron = EXCLUDED.job_cron`,
			d.JobID, d.ActorType, d.ActorID, d.Method, d.Data,
			d.Attempts, lastErrorVal, d.FailedAt.UTC(), d.OriginalDue.UTC(), intervalVal, cronVal,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert dead job %s: %w", d.JobID, err)
		}
	}

	return nil
}

func (s *StandalonePostgresBacked) persistActorStateChanges(ctx context.Context, tx pgx.Tx, changes *internal.Changes) error {
	// Deletes
	for _, key := range changes.ActorState.Delete {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			"DELETE FROM "+s.tablePrefix+"actor_state WHERE actor_type = $1 AND actor_id = $2",
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
			expVal = entry.Expiration.UTC()
		}

		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx,
			`INSERT INTO `+s.tablePrefix+`actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT(actor_type, actor_id) DO UPDATE SET
				actor_state_data = EXCLUDED.actor_state_data,
				actor_state_expiration_time = EXCLUDED.actor_state_expiration_time`,
			key.ActorType, key.ActorID, entry.Data, expVal,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert actor state: %w", err)
		}
	}

	return nil
}
