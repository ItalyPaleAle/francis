//go:build integration

package provider

import (
	"context"
	"database/sql"
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

	// Create a temp directory to hold the shared database file
	b.dir = t.TempDir()

	// A generous busy_timeout lets concurrent hosts wait out each other's write locks
	path := filepath.Join(b.dir, "it.db")
	b.connStr = "file:" + path + "?_pragma=busy_timeout(15000)"
	b.stalled = make(map[int]*sql.Conn)
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

	opts := sqlite.SQLiteProviderOptions{
		ConnectionString: b.connStr,
		Timeout:          b.opts.QueryTimeout,
		// Disable background GC to keep test logs quiet and behavior deterministic
		CleanupInterval: -1,
	}
	if b.opts.Stallable {
		opts.ConnectionString = ""
		opts.DB = b.newHandle(t)
	}

	return opts
}

// newHandle opens a database handle the backend owns, so a scenario can stall the consumer it belongs to by exhausting its pool
// The connection string is normalized the way the provider would, because a caller-supplied handle skips the provider's own parsing while it still checks that foreign keys are on
func (b *sqliteBackend) newHandle(t *testing.T) *sql.DB {
	t.Helper()

	connStr, _, _, err := gosqlsqlite.ParseConnectionString(b.connStr, slog.New(slog.DiscardHandler))
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
	for _, db := range b.handles {
		_ = db.Close()
	}
	b.handles = nil

	if b.dir == "" {
		return
	}

	_ = os.RemoveAll(b.dir)
	b.dir = ""
}
