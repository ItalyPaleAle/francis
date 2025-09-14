package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/sql/cleanup"
	"github.com/italypaleale/actors/internal/sql/migrations"
	postgresmigrations "github.com/italypaleale/actors/internal/sql/migrations/postgres"
	"github.com/italypaleale/actors/internal/sql/sqladapter"
)

var (
	//go:embed migrations
	migrationScripts embed.FS
)

const (
	DefaultTimeout         = 5 * time.Second
	DefaultCleanupInterval = 10 * time.Minute
)

type PostgresProvider struct {
	cfg             components.ProviderConfig
	db              *pgxpool.Pool
	running         atomic.Bool
	log             *slog.Logger
	timeout         time.Duration
	cleanupInterval time.Duration
	gc              cleanup.GarbageCollector
	clock           clock.WithTicker
}

func NewPostgresProvider(log *slog.Logger, postgresOpts PostgresProviderOptions, providerConfig components.ProviderConfig) (*PostgresProvider, error) {
	err := providerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("provider configuration is not valid: %w", err)
	}

	p := &PostgresProvider{
		cfg:             providerConfig,
		log:             log,
		timeout:         postgresOpts.Timeout,
		cleanupInterval: postgresOpts.CleanupInterval,
		clock:           postgresOpts.clock,
		db:              postgresOpts.DB,
	}

	// Set default values
	if p.timeout <= 0 {
		p.timeout = DefaultTimeout
	}
	if p.cleanupInterval == 0 {
		// A zero value means the default
		p.cleanupInterval = DefaultCleanupInterval
	} else if p.cleanupInterval < 0 {
		// A negative value means disabled
		p.cleanupInterval = 0
	}
	if p.clock == nil {
		p.clock = clock.RealClock{}
	}

	// The query timeout should be greater than HostHealthCheckDeadline
	if p.timeout >= p.cfg.HostHealthCheckDeadline {
		return nil, fmt.Errorf("the configured host health check deadline ('%v') must be bigger than the query timeout ('%v')", p.timeout, p.cfg.HostHealthCheckDeadline)
	}
	if p.cfg.HostHealthCheckDeadline-p.timeout < 5*time.Second {
		p.log.Warn("The configured host health check deadline is less than 5s more than the query timeout: this could cause issues", "healthCheckDeadline", p.cfg.HostHealthCheckDeadline, "queryTimeout", p.timeout)
	}

	// Open a database connection unless we have one passed in already
	if p.db == nil {
		if postgresOpts.ConnectionString == "" {
			return nil, errors.New("missing property ConnectionString in Postgres options")
		}

		// TODO: Validate and sanitize connstring
		// See: https://github.com/dapr/components-contrib/blob/main/common/authentication/postgresql/metadata.go
		// Also set TZ to UTC: https://stackoverflow.com/questions/6663765/postgres-default-timezone

		// Open the database
		connCtx, cancel := context.WithTimeout(context.Background(), p.timeout)
		defer cancel()
		p.db, err = pgxpool.New(connCtx, postgresOpts.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Postgres database: %w", err)
		}
	}

	return p, nil
}

type PostgresProviderOptions struct {
	components.ProviderOptions

	// Connection string for the Postgres database
	// This allows the provider to establish a new database connection
	ConnectionString string

	// Connection to an existing database
	DB *pgxpool.Pool

	// Timeout for requests to the database
	Timeout time.Duration

	// Interval at which to perform garbage collection
	CleanupInterval time.Duration

	// Clock, used to pass a mock one for testing
	clock clock.WithTicker
}

// TODO: Rename to "p"
func (s *PostgresProvider) Init(ctx context.Context) error {
	// Perform schema migrations
	err := s.performMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to perform schema migrations: %w", err)
	}

	return nil
}

func (s *PostgresProvider) Run(ctx context.Context) error {
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

func (s *PostgresProvider) HealthCheckInterval() time.Duration {
	// The recommended health check interval is the deadline, less the query timeout, less 1s, then rounded down to the closest 5s
	interval := (s.cfg.HostHealthCheckDeadline - s.timeout - time.Second).Truncate(time.Second)
	interval = interval - time.Duration(int64(interval.Seconds())%5)*time.Second

	// ...however, there's a minimum of 1s
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

func (s *PostgresProvider) RenewLeaseInterval() time.Duration {
	// The recommended interval is the bigger of: the lease duration less 10s, or half of the lease duration
	if s.cfg.AlarmsLeaseDuration < 20*time.Second {
		return s.cfg.AlarmsLeaseDuration / 2
	}

	return s.cfg.AlarmsLeaseDuration - 10*time.Second
}

func (s *PostgresProvider) performMigrations(ctx context.Context) error {
	m := postgresmigrations.Migrations{
		DB:                s.db,
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
			s.log.InfoContext(ctx, "Performing Postgres database migration", slog.String("migration", e))
			_, err := m.DB.Exec(ctx, string(data))
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

func (s *PostgresProvider) initGC() (err error) {
	s.gc, err = cleanup.ScheduleGarbageCollector(cleanup.GCOptions{
		Logger: s.log,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `
				INSERT INTO metadata (key, value)
					VALUES ('last-cleanup', LOCALTIMESTAMP::text)
				ON CONFLICT (key)
					DO UPDATE SET value = EXCLUDED.value
				WHERE (EXTRACT('epoch' FROM LOCALTIMESTAMP - metadata.value::timestamp) * 1000)::bigint > $1`,
				[]any{arg}
		},
		DeleteExpiredValuesQueries: map[string]cleanup.DeleteExpiredValuesQueryFn{
			"hosts": func() (string, func() []any) {
				q := `DELETE FROM hosts WHERE host_last_health_check < (LOCALTIMESTAMP - $1)`
				return q, func() []any {
					return []any{s.cfg.HostHealthCheckDeadline}
				}
			},
			"actor_state": func() (string, func() []any) {
				q := `
				DELETE FROM actor_state
				WHERE
					actor_state_expiration_time IS NOT NULL
					AND actor_state_expiration_time < LOCALTIMESTAMP
				`
				return q, func() []any {
					return nil
				}
			},
		},
		CleanupInterval: s.cleanupInterval,
		DB:              sqladapter.AdaptPgxConn(s.db),
	})
	return err
}
