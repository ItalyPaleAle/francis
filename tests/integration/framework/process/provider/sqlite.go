//go:build integration

package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gosqlsqlite "github.com/italypaleale/go-sql-utils/sqlite"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host/local"
)

// stallAcquireTimeout bounds how long Stall waits to take the connection it parks, so a scenario fails loudly rather than hanging if the pool never frees one up
const stallAcquireTimeout = 30 * time.Second

// migrateTimeout is the time limit for the initial schema migration, which can be long because no scenario measures it
const migrateTimeout = 2 * time.Minute

// sqliteBackend is a multi-host backend backed by a shared on-disk SQLite file
//
// A file is used rather than :memory:, which is per-connection and not shareable, so multiple hosts can open independent connections to the same database and coordinate through it
// The provider applies WAL and foreign_keys itself, and rejects foreign_keys if it is set in the connection string
type sqliteBackend struct {
	opts Options

	dir     string
	connStr string

	// handles are the per-consumer database handles, in the order they were handed out, and are only populated when the backend is stallable
	handles []*sql.DB
	// stalled holds the connection parked for each stalled consumer, keyed by consumer index
	stalled map[int]*sql.Conn
}

func (b *sqliteBackend) Variant() Variant {
	return SQLite
}

func (b *sqliteBackend) Run(t *testing.T) {
	t.Helper()

	// Create a temp directory to hold the shared database file, owned by this backend rather than by t.TempDir
	// Cleanups run in reverse order of registration, and the framework registers this backend's before starting it, so a directory registered here would be removed before Cleanup closes the database handles the backend holds
	// Windows refuses to delete a file that is still open, so the removal has to be sequenced by us
	//nolint:usetesting // t.TempDir is what this deliberately avoids: its cleanup would run before this backend closes its handles
	dir, err := os.MkdirTemp("", "francis-integration-")
	require.NoError(t, err, "failed to create the temp directory for the SQLite database")
	b.dir = dir

	// A generous busy_timeout lets concurrent hosts wait out each other's write locks
	path := filepath.Join(b.dir, "it.db")
	b.connStr = "file:" + path + "?_pragma=busy_timeout(15000)"
	b.stalled = make(map[int]*sql.Conn)

	// Run the schema migrations once, before any host or runtime starts
	// On a fresh database the migrations are the slowest part of provider initialization, and on slow runners they can take longer than the short request timeouts the fault scenarios use
	// With the schema in place, each host and runtime only reads the migration level when it initializes
	b.migrate(t)
}

// migrate runs the provider's schema migrations on the shared database, using a temporary provider that it closes when done
// The provider opens the database from the connection string instead of using providerOptions, because on a stallable backend providerOptions gives out a consumer's database handle
func (b *sqliteBackend) migrate(t *testing.T) {
	t.Helper()

	p, err := sqlite.NewSQLiteProvider(slog.New(slog.DiscardHandler), b.sharedOptions(), providerConfig(b.opts))
	require.NoError(t, err, "failed to create the SQLite provider that migrates the database")

	// Close the provider even if the migrations fail, because Windows can't delete a database file that is still open
	ctx, cancel := context.WithTimeout(t.Context(), migrateTimeout)
	defer cancel()
	err = p.Init(ctx)
	closeErr := p.Close()
	require.NoError(t, err, "failed to migrate the SQLite database")
	require.NoError(t, closeErr, "failed to close the SQLite provider that migrated the database")
}

func (b *sqliteBackend) LocalHostOption(t *testing.T) local.HostOption {
	t.Helper()
	return local.WithSQLiteProvider(b.providerOptions(t))
}

func (b *sqliteBackend) NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider {
	t.Helper()

	p, err := sqlite.NewSQLiteProvider(log, b.providerOptions(t), providerConfig(b.opts))
	require.NoError(t, err, "failed to create SQLite provider")

	return p
}

func (b *sqliteBackend) ProviderOptions(t *testing.T) components.ProviderOptions {
	t.Helper()
	return b.providerOptions(t)
}

// providerOptions builds the options for one consumer of the shared store
// A stallable backend gives each consumer its own database handle it can later choke, while otherwise the provider opens the connection itself from the shared connection string
func (b *sqliteBackend) providerOptions(t *testing.T) sqlite.SQLiteProviderOptions {
	t.Helper()
	require.NotEmpty(t, b.connStr, "SQLite backend used before Run")

	opts := b.sharedOptions()
	if b.opts.Stallable {
		opts.ConnectionString = ""
		opts.DB = b.newHandle(t)
	}

	return opts
}

// sharedOptions returns the provider options that open the shared database from its connection string
func (b *sqliteBackend) sharedOptions() sqlite.SQLiteProviderOptions {
	return sqlite.SQLiteProviderOptions{
		ConnectionString: b.connStr,
		Timeout:          b.opts.QueryTimeout,
		// Disable background GC to keep test logs quiet and behavior deterministic
		CleanupInterval: -1,
	}
}

// newHandle opens a database handle the backend owns, so a scenario can stall the consumer it belongs to by exhausting its pool
// The connection string is normalized the way the provider would, because a caller-supplied handle skips the provider's own parsing while it still checks that foreign keys are on
func (b *sqliteBackend) newHandle(t *testing.T) *sql.DB {
	t.Helper()

	connStr, _, _, _, err := gosqlsqlite.ParseConnectionString(b.connStr, slog.New(slog.DiscardHandler))
	require.NoError(t, err, "failed to parse the SQLite connection string")

	db, err := sql.Open("sqlite", connStr)
	require.NoError(t, err, "failed to open the SQLite database")

	b.handles = append(b.handles, db)
	return db
}

// Stall parks the only connection the consumer's pool is allowed to open, so every provider call it makes waits for a connection until Unstall and fails once its query timeout expires
// This is what a SQLite database that has gone busy looks like to one host, without affecting any other host sharing the same file
func (b *sqliteBackend) Stall(t *testing.T, consumer int) {
	t.Helper()

	db := b.handle(t, consumer)
	_, already := b.stalled[consumer]
	require.False(t, already, "consumer %d is already stalled", consumer)

	// Take a connection first, then shrink the pool around it, so the pool can never hand a second one out
	// A call already holding a connection at this point runs to completion, and its connection is closed on release
	ctx, cancel := context.WithTimeout(t.Context(), stallAcquireTimeout)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err, "failed to acquire the connection to park for consumer %d", consumer)
	db.SetMaxOpenConns(1)

	b.stalled[consumer] = conn
}

// Unstall releases the parked connection and restores the pool
func (b *sqliteBackend) Unstall(t *testing.T, consumer int) {
	t.Helper()

	conn, ok := b.stalled[consumer]
	if !ok {
		return
	}
	delete(b.stalled, consumer)

	_ = conn.Close()
	b.handle(t, consumer).SetMaxOpenConns(0)
}

// handle returns the database handle of a consumer, failing the scenario if the backend was not built to be stallable
func (b *sqliteBackend) handle(t *testing.T, consumer int) *sql.DB {
	t.Helper()

	require.True(t, b.opts.Stallable, "the SQLite backend was not built with Stallable set")
	require.Less(t, consumer, len(b.handles), "consumer %d has not taken a database handle yet", consumer)
	return b.handles[consumer]
}

func (b *sqliteBackend) Cleanup(t *testing.T) {
	t.Helper()

	// Release anything still parked, then close the handles the backend owns
	for consumer := range b.stalled {
		b.Unstall(t, consumer)
	}

	errs := make([]error, 0)
	for i, db := range b.handles {
		err := db.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close the database handle of consumer %d: %w", i, err))
			continue
		}

		// A connection left checked out survives Close and keeps the database file open, which Windows then refuses to delete
		if db.Stats().OpenConnections != 0 {
			errs = append(errs, fmt.Errorf("consumer %d still holds a database connection after close", i))
			continue
		}
	}
	b.handles = nil

	// There shouldn't have been any error
	require.NoError(t, errors.Join(errs...))

	if b.dir == "" {
		return
	}

	// Removal is asserted rather than ignored, since a failure that outlasts the retries means something still holds the database file open
	err := removeAllWithRetry(b.dir)
	require.NoError(t, err, "failed to remove the temp directory holding the SQLite database")

	b.dir = ""
}

// removeRetryTimeout bounds how long Cleanup keeps retrying the removal of the temp directory, so a database file that is really still open fails the scenario instead of hanging it
const removeRetryTimeout = 5 * time.Second

// removeAllWithRetry removes a directory tree, retrying with backoff until removeRetryTimeout if the removal fails
// On Windows another process, such as an antivirus scanner or the search indexer, can briefly hold a file that was just closed, which fails the removal with a sharing violation
// Go's t.TempDir cleanup retries for the same reason, but this backend manages its own directory so it has to do the same itself
func removeAllWithRetry(dir string) error {
	deadline := time.Now().Add(removeRetryTimeout)
	delay := 10 * time.Millisecond
	for {
		err := os.RemoveAll(dir)
		if err == nil || time.Now().Add(delay).After(deadline) {
			return err
		}

		// t.Context is already canceled while cleanups run, so this waits on the clock alone
		time.Sleep(delay)
		delay = min(delay*2, 500*time.Millisecond)
	}
}
