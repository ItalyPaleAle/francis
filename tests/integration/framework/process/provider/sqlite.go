//go:build integration

package provider

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host/local"
)

// sqliteBackend is a multi-host backend backed by a shared on-disk SQLite file
//
// A file is used rather than :memory:, which is per-connection and not shareable, so multiple hosts can open independent connections to the same database and coordinate through it
// The provider applies WAL and foreign_keys itself, and rejects foreign_keys if it is set in the connection string
type sqliteBackend struct {
	dir     string
	connStr string
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
}

func (b *sqliteBackend) LocalHostOption(t *testing.T) local.HostOption {
	t.Helper()
	require.NotEmpty(t, b.connStr, "SQLite backend used before Run")

	return local.WithSQLiteProvider(sqlite.SQLiteProviderOptions{
		ConnectionString: b.connStr,
		// Disable background GC to keep test logs quiet and behavior deterministic
		CleanupInterval: -1,
	})
}

func (b *sqliteBackend) NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider {
	t.Helper()
	require.NotEmpty(t, b.connStr, "SQLite backend used before Run")

	p, err := sqlite.NewSQLiteProvider(log, sqlite.SQLiteProviderOptions{
		ConnectionString: b.connStr,
		CleanupInterval:  -1,
	}, providerConfig())
	require.NoError(t, err, "failed to create SQLite provider")

	return p
}

func (b *sqliteBackend) ProviderOptions(t *testing.T) components.ProviderOptions {
	t.Helper()
	require.NotEmpty(t, b.connStr, "SQLite backend used before Run")

	return sqlite.SQLiteProviderOptions{
		ConnectionString: b.connStr,
		CleanupInterval:  -1,
	}
}

func (b *sqliteBackend) Cleanup(t *testing.T) {
	t.Helper()

	if b.dir == "" {
		return
	}

	_ = os.RemoveAll(b.dir)
	b.dir = ""
}
