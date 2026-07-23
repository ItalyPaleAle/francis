//go:build integration

package provider

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host/local"
)

// standaloneMemoryBackend is the pure in-memory, single-host provider
// It owns no external store, so Run and Cleanup are no-ops
type standaloneMemoryBackend struct{}

func (b *standaloneMemoryBackend) Variant() Variant {
	return StandaloneMemory
}

func (b *standaloneMemoryBackend) Run(*testing.T) {
	// Nop
}

func (b *standaloneMemoryBackend) Cleanup(*testing.T) {
	// Nop
}

func (b *standaloneMemoryBackend) LocalHostOption(t *testing.T) local.HostOption {
	t.Helper()
	return local.WithStandaloneMemoryProvider(standalone.StandaloneMemoryOptions{
		CleanupInterval: -1,
	})
}

func (b *standaloneMemoryBackend) NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider {
	t.Helper()

	p, err := standalone.NewStandaloneMemory(log, standalone.StandaloneMemoryOptions{
		CleanupInterval: -1,
	}, providerConfig())

	require.NoError(t, err, "failed to create standalone memory provider")
	return p
}

func (b *standaloneMemoryBackend) ProviderOptions(*testing.T) components.ProviderOptions {
	return standalone.StandaloneMemoryOptions{
		CleanupInterval: -1,
	}
}

// standaloneSQLiteBackend is the single-host, in-memory provider with SQLite persistence
// It owns one shared in-memory SQLite connection
type standaloneSQLiteBackend struct {
	db *sql.DB
}

func (b *standaloneSQLiteBackend) Variant() Variant {
	return StandaloneSQLite
}

func (b *standaloneSQLiteBackend) Run(t *testing.T) {
	t.Helper()

	// An in-memory SQLite database lives only as long as its single connection, so cap the pool at one
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "failed to open in-memory SQLite database")
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON")
	require.NoError(t, err, "failed to enable foreign keys")

	b.db = db
}

func (b *standaloneSQLiteBackend) LocalHostOption(t *testing.T) local.HostOption {
	t.Helper()
	require.NotNil(t, b.db, "standalone SQLite backend used before Run")

	return local.WithStandaloneSQLiteProvider(standalone.StandaloneSQLiteOptions{
		DB:              b.db,
		CleanupInterval: -1,
	})
}

func (b *standaloneSQLiteBackend) NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider {
	t.Helper()
	require.NotNil(t, b.db, "standalone SQLite backend used before Run")

	p, err := standalone.NewStandaloneSQLiteBacked(log, standalone.StandaloneSQLiteOptions{
		DB:              b.db,
		CleanupInterval: -1,
	}, providerConfig())
	require.NoError(t, err, "failed to create standalone SQLite provider")
	return p
}

func (b *standaloneSQLiteBackend) ProviderOptions(t *testing.T) components.ProviderOptions {
	t.Helper()
	require.NotNil(t, b.db, "standalone SQLite backend used before Run")

	return standalone.StandaloneSQLiteOptions{
		DB:              b.db,
		CleanupInterval: -1,
	}
}

func (b *standaloneSQLiteBackend) Cleanup(t *testing.T) {
	t.Helper()

	if b.db == nil {
		return
	}

	_ = b.db.Close()
	b.db = nil
}
