package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	sqladapter "github.com/italypaleale/go-sql-utils/adapter/sql"
	"github.com/italypaleale/go-sql-utils/cleanup"
	sqlinstrument "github.com/italypaleale/go-sql-utils/instrument"
	sqliteinstrument "github.com/italypaleale/go-sql-utils/instrument/sqlite"
	"github.com/italypaleale/go-sql-utils/migrations"
	sqlitemigrations "github.com/italypaleale/go-sql-utils/migrations/sqlite"
	gosqlsqlite "github.com/italypaleale/go-sql-utils/sqlite"
	"k8s.io/utils/clock"
	"modernc.org/sqlite"

	"github.com/italypaleale/francis/components"
)

var (
	//go:embed migrations
	migrationScripts embed.FS

	//go:embed queries/fetch-upcoming-alarms-no-constraints.sql
	queryFetchUpcomingAlarmsNoConstraints string

	//go:embed queries/fetch-upcoming-alarms-with-constraints.sql
	queryFetchUpcomingAlarmsWithConstraints string
)

const (
	DefaultBusyTimeout      = 2500 * time.Millisecond
	DefaultConnectionString = "data.db"
	DefaultTimeout          = 5 * time.Second
	DefaultCleanupInterval  = 10 * time.Minute
	DefaultTablePrefix      = "francis"
)

type SQLiteProvider struct {
	cfg             components.ProviderConfig
	db              *sql.DB
	ownsDB          bool
	closed          atomic.Bool
	running         atomic.Bool
	log             *slog.Logger
	timeout         time.Duration
	cleanupInterval time.Duration
	gc              cleanup.GarbageCollector
	clock           clock.WithTicker
	tablePrefix     string

	// Fetch-upcoming-alarm queries, with the table prefix already applied, computed once at construction
	fetchUpcomingAlarmsNoConstraintsQuery   string
	fetchUpcomingAlarmsWithConstraintsQuery string
}

func NewSQLiteProvider(log *slog.Logger, sqliteOpts SQLiteProviderOptions, providerConfig components.ProviderConfig) (*SQLiteProvider, error) {
	err := providerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("provider configuration is not valid: %w", err)
	}

	s := &SQLiteProvider{
		cfg:             providerConfig,
		log:             log,
		timeout:         sqliteOpts.Timeout,
		cleanupInterval: sqliteOpts.CleanupInterval,
		clock:           sqliteOpts.clock,
		db:              sqliteOpts.DB,
	}

	// Resolve the table prefix
	// An unset (empty) prefix falls back to the default
	tablePrefix := sqliteOpts.TablePrefix
	if tablePrefix == "" {
		tablePrefix = DefaultTablePrefix
	}
	if tablePrefix != "" {
		// A non-empty prefix is stored with a trailing separator so tables are named e.g. "francis_hosts"
		s.tablePrefix = tablePrefix + "_"
	}

	// Pre-compute the prefixed fetch-upcoming-alarm queries so the hot path doesn't re-format them on every fetch
	s.fetchUpcomingAlarmsNoConstraintsQuery = s.q(queryFetchUpcomingAlarmsNoConstraints)
	s.fetchUpcomingAlarmsWithConstraintsQuery = s.q(queryFetchUpcomingAlarmsWithConstraints)

	// Set default values
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

	// Open a database connection unless we have one passed in already
	if s.db == nil {
		// Prepare the connector so the instrumented driver keeps the SQLite connection setup and pool constraints
		if sqliteOpts.ConnectionString == "" {
			sqliteOpts.ConnectionString = DefaultConnectionString
		}
		connector, err := gosqlsqlite.NewConnector(gosqlsqlite.ConnectOpts{
			ConnString: sqliteOpts.ConnectionString,
			Logger:     s.log,
		})
		if err != nil {
			return nil, fmt.Errorf("connection string for SQLite is not valid: %w", err)
		}

		// Open the database through the instrumented driver, so every statement is traced and optionally logged
		s.db, err = sqliteinstrument.Open(connector, &sqlinstrument.Options{
			Log:               s.log,
			QueryLog:          sqliteOpts.QueryLog.Enabled,
			IncludeParameters: sqliteOpts.QueryLog.IncludeParameters,
			SlowThreshold:     sqliteOpts.QueryLog.SlowThreshold,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open SQLite database: %w", err)
		}

		// The provider owns this connection, so Close is responsible for closing it
		s.ownsDB = true
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

	// Prefix added to the name of every table (and other schema object) used by the provider
	// When set, tables are named "<prefix>_<table>", e.g. with prefix "francis" the hosts table is "francis_hosts"
	// Defaults to "francis" when empty
	TablePrefix string

	// QueryLog controls optional SQL statement logging when this constructor opens the database connection
	// When a connection is passed in via DB, the caller can add statement tracing and logging by opening the database with instrument/sqlite.Open from go-sql-utils
	QueryLog components.QueryLogConfig

	// OperationLog is applied by the host and runtime provider factory
	// Direct callers of this low-level constructor can apply it explicitly with instrument.WrapProvider
	OperationLog components.OperationLogConfig

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
	// Reset the running flag on exit, so the provider can be run again
	defer s.running.Store(false)

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

// Close releases the resources owned by the provider
func (s *SQLiteProvider) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	if !s.ownsDB || s.db == nil {
		return nil
	}

	// The database connection is closed only if the provider established it
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close SQLite database: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) HealthCheckPolicy() *components.HealthCheckPolicy {
	return s.cfg.HealthCheckPolicy()
}

func (s *SQLiteProvider) RenewLeaseInterval() time.Duration {
	// The recommended interval is the bigger of: the lease duration less 10s, or half of the lease duration
	if s.cfg.AlarmsLeaseDuration < 20*time.Second {
		return s.cfg.AlarmsLeaseDuration / 2
	}

	return s.cfg.AlarmsLeaseDuration - 10*time.Second
}

func (s *SQLiteProvider) performMigrations(ctx context.Context) error {
	m := sqlitemigrations.Migrations{
		Pool:              s.db,
		MetadataTableName: s.tablePrefix + "metadata",
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

	migrationFns := make([]migrations.MigrationFn, len(names))
	for i, e := range names {
		data, err := migrationScripts.ReadFile(path.Join("migrations", e))
		if err != nil {
			return fmt.Errorf("error reading migration script '%s': %w", e, err)
		}

		// Apply the table prefix to the script's "%s" placeholders
		script := s.q(string(data))

		migrationFns[i] = func(ctx context.Context) error {
			s.log.InfoContext(ctx, "Performing SQLite database migration", slog.String("migration", e))
			_, err := m.GetConn().ExecContext(ctx, script)
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
				INSERT INTO ` + s.tablePrefix + `metadata (key, value)
					VALUES ('last-cleanup', ?)
					ON CONFLICT (key)
					DO UPDATE SET value = ?
						WHERE (? - CAST(value AS integer)) > ?`,
				[]any{now, now, now, arg}
		},
		DeleteExpiredValuesQueries: map[string]cleanup.DeleteExpiredValuesQueryFn{
			"hosts": func() (string, func() []any) {
				q := `DELETE FROM ` + s.tablePrefix + `hosts WHERE host_last_health_check < ?`
				return q, func() []any {
					now := s.clock.Now()
					return []any{
						now.Add(-1 * s.cfg.HostHealthCheckDeadline).UnixMilli(),
					}
				}
			},
			"actor_state": func() (string, func() []any) {
				q := `
				DELETE FROM ` + s.tablePrefix + `actor_state
				WHERE
					actor_state_expiration_time IS NOT NULL
					AND actor_state_expiration_time < ?
				`
				return q, func() []any {
					now := s.clock.Now()
					return []any{
						now.UnixMilli(),
					}
				}
			},
		},
		CleanupInterval: s.cleanupInterval,
		DB:              sqladapter.AdaptDatabaseSQLConn(s.db),
	})
	return err
}

// q applies the configured table prefix to a query loaded from an embedded SQL file (migration scripts and query files)
// In those files, every table (and other schema object) name is written with a "%s" placeholder immediately before it (e.g. "%shosts"), which this replaces with the prefix
// Note: Temporary tables are connection-local and thus never prefixed
func (s *SQLiteProvider) q(query string) string {
	n := strings.Count(query, "%s")
	if n == 0 {
		return query
	}

	args := make([]any, n)
	for i := range args {
		args[i] = s.tablePrefix
	}

	// The only value interpolated here is the statically-derived table prefix, so there's no risk of SQL injection
	// #nosec G201
	return fmt.Sprintf(query, args...)
}

// Checks if an error returned by the database is a unique constraint violation error, such as a duplicate unique index or primary key.
func isConstraintError(err error) bool {
	// These bits are set on all constraint-related errors
	// https://www.sqlite.org/rescode.html#constraint
	const sqliteConstraintCode = 19

	if err == nil {
		return false
	}

	sqliteErr, ok := errors.AsType[*sqlite.Error](err)
	if !ok {
		return false
	}

	return sqliteErr.Code()&sqliteConstraintCode != 0
}

// Returns the placeholder string for an IN clause, and also appends all arguments to appendArgs, starting at position startAppend
// appendArgs must have sufficient length for the arguments being added
func getInPlaceholders(vals []string, appendArgs []any, startAppend int) string {
	l := len(vals)
	switch l {
	case 0:
		return ""
	case 1:
		appendArgs[startAppend] = vals[0]
		return "?"
	default:
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
}

// querier is an interface that is implemented by both *sql.DB and *sql.Tx
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
