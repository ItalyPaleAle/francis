package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"
	"modernc.org/sqlite"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/sql/cleanup"
	"github.com/italypaleale/actors/internal/sql/migrations"
	sqlitemigrations "github.com/italypaleale/actors/internal/sql/migrations/sqlite"
	"github.com/italypaleale/actors/internal/sql/sqladapter"
)

var (
	//go:embed migrations
	migrationScripts embed.FS

	//go:embed queries/fetch-upcoming-alarms-no-constraints.sql
	queryFetchUpcomingAlarmsNoConstraints string

	//go:embed queries/fetch-upcoming-alarms-with-constraints.sql
	queryFetchUpcomingAlarmsWithConstraints string
)

type SQLiteProvider struct {
	pid             string
	cfg             components.ProviderConfig
	db              *sql.DB
	running         atomic.Bool
	log             *slog.Logger
	timeout         time.Duration
	cleanupInterval time.Duration
	gc              cleanup.GarbageCollector
	clock           clock.WithTicker
}

func NewSQLiteProvider(log *slog.Logger, sqliteOpts SQLiteProviderOptions, providerConfig components.ProviderConfig) (*SQLiteProvider, error) {
	var err error

	s := &SQLiteProvider{
		// TODO: Set a PID
		pid:             "TODO",
		cfg:             providerConfig,
		log:             log,
		timeout:         sqliteOpts.Timeout,
		cleanupInterval: sqliteOpts.CleanupInterval,
		clock:           sqliteOpts.clock,
		db:              sqliteOpts.DB,
	}

	// Set default values
	s.cfg.SetDefaults()
	if s.timeout <= 0 {
		s.timeout = DefaultTimeout
	}
	if s.cleanupInterval == 0 {
		// A zero value means the default
		s.cleanupInterval = DefaultCleanupInterval
	} else if s.cleanupInterval < 0 {
		// A negative value means disabled
		s.cleanupInterval = 0
	}
	if s.clock == nil {
		s.clock = clock.RealClock{}
	}

	// The query timeout should be greater than HostHealthCheckDeadline
	if s.timeout >= s.cfg.HostHealthCheckDeadline {
		return nil, fmt.Errorf("the configured host health check deadline ('%v') must be bigger than the query timeout ('%v')", s.timeout, s.cfg.HostHealthCheckDeadline)
	}
	if s.cfg.HostHealthCheckDeadline-s.timeout < 5*time.Second {
		s.log.Warn("The configured host health check deadline is less than 5s more than the query timeout: this could cause issues", "healthCheckDeadline", s.cfg.HostHealthCheckDeadline, "queryTimeout", s.timeout)
	}

	// Open a database connection unless we have one passed in already
	if s.db == nil {
		// Parse the connection string
		if sqliteOpts.ConnectionString == "" {
			sqliteOpts.ConnectionString = DefaultConnectionString
		}
		sqliteOpts.ConnectionString, err = ParseConnectionString(sqliteOpts.ConnectionString, s.log)
		if err != nil {
			return nil, fmt.Errorf("connection string for SQLite is not valid")
		}

		// Open the database
		s.db, err = sql.Open("sqlite", sqliteOpts.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("failed to open SQLite database: %w", err)
		}
	}

	return s, nil
}

type SQLiteProviderOptions struct {
	components.ProviderOptions

	// Connection string or path to the SQLite database
	// This allows the provider to establish a new database connection
	ConnectionString string

	// Connection to an existing database
	DB *sql.DB

	// Timeout for requests to the database
	Timeout time.Duration

	// Interval at which to perform garbage collection
	CleanupInterval time.Duration

	// Clock, used to pass a mock one for testing
	clock clock.WithTicker
}

func (s *SQLiteProvider) Init(ctx context.Context) error {
	// Validate that the connection has the required parameters
	err := s.validateConnection(ctx)
	if err != nil {
		return err
	}

	// Perform schema migrations
	err = s.performMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to perform schema migrations: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	// Start the background garbage collection
	err := s.initGC()
	if err != nil {
		return fmt.Errorf("failed to start garbage collector: %w", err)
	}

	// Wait for the context to be canceled
	<-ctx.Done()

	// Stop the garbage collector
	err = s.gc.Close()
	if err != nil {
		return fmt.Errorf("failed to stop garbage collector: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) HealthCheckInterval() time.Duration {
	// The recommended health check interval is the deadline, less the query timeout, less 1s, then rounded down to the closest 5s
	interval := (s.cfg.HostHealthCheckDeadline - s.timeout - time.Second).Truncate(time.Second)
	interval = interval - time.Duration(int64(interval.Seconds())%5)*time.Second

	// ...however, there's a minimum of 1s
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

func (s *SQLiteProvider) performMigrations(ctx context.Context) error {
	m := sqlitemigrations.Migrations{
		Pool:              s.db,
		MetadataTableName: "metadata",
		MetadataKey:       "migrations-version",
	}

	// Get all migration scripts
	entries, err := migrationScripts.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("error while loading migration scripts: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			// Should not happen...
			continue
		}
		names = append(names, e.Name())
	}
	slices.Sort(names)

	migrationFns := make([]migrations.MigrationFn, len(entries))
	for i, e := range names {
		data, err := migrationScripts.ReadFile(filepath.Join("migrations", e))
		if err != nil {
			return fmt.Errorf("error reading migration script '%s': %w", e, err)
		}

		migrationFns[i] = func(ctx context.Context) error {
			s.log.InfoContext(ctx, "Performing SQLite database migration", slog.String("migration", e))
			_, err := m.GetConn().ExecContext(ctx, string(data))
			if err != nil {
				return fmt.Errorf("failed to perform migration '%s': %w", e, err)
			}
			return nil
		}
	}

	// Execute the migrations
	err = m.Perform(ctx, migrationFns, s.log)
	if err != nil {
		return fmt.Errorf("migrations failed with error: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) validateConnection(ctx context.Context) error {
	// Ensure that foreign keys are enabled
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var fk bool
	err := s.db.QueryRowContext(queryCtx, "PRAGMA foreign_keys").Scan(&fk)
	if err != nil {
		return fmt.Errorf("error checking pragma foreign_keys: %w", err)
	}
	if !fk {
		return errors.New("SQLite is running with foreign keys disabled, which is not supported")
	}

	return nil
}

func (s *SQLiteProvider) initGC() (err error) {
	s.gc, err = cleanup.ScheduleGarbageCollector(cleanup.GCOptions{
		Logger: s.log,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			now := s.clock.Now().UnixMilli()
			return `
				INSERT INTO metadata (key, value)
					VALUES ('last-cleanup', ?)
					ON CONFLICT (key)
					DO UPDATE SET value = ?
						WHERE (? - CAST(value AS integer)) > ?`,
				[]any{now, now, now, arg}
		},
		DeleteExpiredValuesQuery: func() (string, []any) {
			now := s.clock.Now().UnixMilli()

			// In a transaction, delete all expired state and all hosts that have failed health checks
			// Failed hosts are also automatically deleted when a new host is registered, so that query should not delete many rows
			return `
				BEGIN IMMEDIATE TRANSACTION;

				DELETE FROM actor_state
				WHERE
					actor_state_expiration_time IS NOT NULL
					AND actor_state_expiration_time < ?;

				DELETE FROM hosts
				WHERE host_last_health_check < ?;

				COMMIT;`,
				[]any{
					now,
					now - s.cfg.HostHealthCheckDeadline.Milliseconds(),
				}
		},
		CleanupInterval: s.cleanupInterval,
		DB:              sqladapter.AdaptDatabaseSQLConn(s.db),
	})
	return err
}

// Checks if an error returned by the database is a unique constraint violation error, such as a duplicate unique index or primary key.
func isConstraintError(err error) bool {
	// These bits are set on all constraint-related errors
	// https://www.sqlite.org/rescode.html#constraint
	const sqliteConstraintCode = 19

	if err == nil {
		return false
	}

	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}

	return sqliteErr.Code()&sqliteConstraintCode != 0
}

// Returns the placeholder string for an IN clause, and also appends all arguments to appendArgs, starting at position startAppend
// appendArgs must have sufficient length for the arguments being added
func getInPlaceholders(vals []string, appendArgs []any, startAppend int) string {
	b := strings.Builder{}
	b.Grow(len(vals) * 2)
	for i, h := range vals {
		if i > 0 {
			b.WriteString(",?")
		} else {
			b.WriteRune('?')
		}
		appendArgs[startAppend+i] = h
	}
	return b.String()
}
