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
	"sync/atomic"
	"time"

	"modernc.org/sqlite"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/sql/migrations"
	sqlitemigrations "github.com/italypaleale/actors/internal/sql/migrations/sqlite"
)

//go:embed migrations
var migrationScripts embed.FS

type SQLiteProvider struct {
	cfg     components.ProviderConfig
	db      *sql.DB
	running atomic.Bool
	log     *slog.Logger
	timeout time.Duration
}

func NewSQLiteProvider(log *slog.Logger, sqliteOpts SQLiteProviderOptions, providerConfig components.ProviderConfig) (components.ActorProvider, error) {
	var err error

	s := &SQLiteProvider{
		cfg:     providerConfig,
		log:     log,
		timeout: sqliteOpts.Timeout,
	}

	// Set default values
	s.cfg.SetDefaults()
	if s.timeout <= 0 {
		s.timeout = DefaultTimeout
	}

	// The query timeout should be greater than HostHealthCheckDeadline
	if s.timeout >= s.cfg.HostHealthCheckDeadline {
		return nil, fmt.Errorf("the configured host health check deadline ('%v') must be bigger than the query timeout ('%v')", s.timeout, s.cfg.HostHealthCheckDeadline)
	}
	if s.cfg.HostHealthCheckDeadline-s.timeout < 5*time.Second {
		s.log.Warn("The configured host health check deadline is less than 5s more than the query timeout: this could cause issues", "healthCheckDeadline", s.cfg.HostHealthCheckDeadline, "queryTimeout", s.timeout)
	}

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

	return s, nil
}

type SQLiteProviderOptions struct {
	components.ProviderOptions

	// Connection string or path to the SQLite database
	ConnectionString string

	// Timeout for requests to the database
	Timeout time.Duration
}

func (s *SQLiteProvider) Init(ctx context.Context) error {
	// Perform schema migrations
	err := s.performMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to perform schema migrations: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	<-ctx.Done()
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
