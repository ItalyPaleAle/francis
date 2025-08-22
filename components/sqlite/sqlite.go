package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync/atomic"

	// Blank import for the sqlite driver
	_ "modernc.org/sqlite"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/sql/migrations"
	sqlitemigrations "github.com/italypaleale/actors/internal/sql/migrations/sqlite"
)

//go:embed migrations
var migrationScripts embed.FS

type SQLiteProvider struct {
	opts    components.ProviderOptions
	db      *sql.DB
	running atomic.Bool
	log     *slog.Logger
}

func NewSQLiteProvider(ctx context.Context, connStr string, log *slog.Logger, opts components.ProviderOptions) (components.ActorProvider, error) {
	var err error

	s := &SQLiteProvider{
		opts: opts,
		log:  log,
	}

	// Parse the connection string
	if connStr == "" {
		connStr = DefaultConnectionString
	}
	connStr, err = ParseConnectionString(connStr, s.log)
	if err != nil {
		return nil, fmt.Errorf("connection string for SQLite is not valid")
	}

	// Open the database
	s.db, err = sql.Open("sqlite", connStr)
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

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref components.ActorRef, name string, req components.SetAlarmReq) error {
	return nil
}

func (s *SQLiteProvider) DeleteAlarm(ctx context.Context, ref components.ActorRef, name string) error {
	return nil
}

func (s *SQLiteProvider) GetState(ctx context.Context, ref components.ActorRef) ([]byte, error) {
	return nil, nil
}

func (s *SQLiteProvider) SetState(ctx context.Context, ref components.ActorRef, data []byte) error {
	return nil
}

func (s *SQLiteProvider) DeleteState(ctx context.Context, ref components.ActorRef) error {
	return nil
}
