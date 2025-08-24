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
	providerOpts components.ProviderOptions
	db           *sql.DB
	running      atomic.Bool
	log          *slog.Logger
	timeout      time.Duration
}

func NewSQLiteProvider(ctx context.Context, sqliteOpts SQLiteProviderOptions, providerOpts components.ProviderOptions) (components.ActorProvider, error) {
	var err error

	s := &SQLiteProvider{
		providerOpts: providerOpts,
		log:          providerOpts.Logger,
		timeout:      sqliteOpts.Timeout,
	}

	// Ensure the logger is non-nil
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}

	// Set default health check deadline
	if s.providerOpts.HostHealthCheckDeadline <= time.Second {
		s.providerOpts.HostHealthCheckDeadline = components.HostHealthCheckDeadline
	}

	// Set default timeout if empty
	if s.timeout <= 0 {
		s.timeout = DefaultTimeout
	}

	// Warn if the timeout is greater than HostHealthCheckDeadline
	if s.timeout >= s.providerOpts.HostHealthCheckDeadline {
		s.log.Warn("The configured query host health check deadline is not bigger than the query timeout, this could cause issues")
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

	// Migrate schema
	err = s.performMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to perform migrations: %w", err)
	}

	return s, nil
}

type SQLiteProviderOptions struct {
	// Connection string or path to the SQLite database
	ConnectionString string

	// Timeout for requests to the database
	Timeout time.Duration
}

func (s *SQLiteProvider) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	<-ctx.Done()
	return nil
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
