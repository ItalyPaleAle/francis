package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// writeProviderConfig writes a minimal config pointing at the given provider connection string and returns its path
func writeProviderConfig(t *testing.T, connString string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`{"provider":{"connectionString": "`+connString+`"}}`), 0o600)
	require.NoError(t, err)
	return path
}

// testProviderConfig mirrors the provider config the CLI builds, with values large enough to satisfy the providers' timeout checks
func testProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   20 * time.Second,
		AlarmsLeaseDuration:       20 * time.Second,
		AlarmsFetchAheadInterval:  2500 * time.Millisecond,
		AlarmsFetchAheadBatchSize: 25,
	}
}

func TestRunBackupRequiresFile(t *testing.T) {
	quietStdoutStderr(t, func() {
		rc := runBackup(t.Context(), nil)
		assert.NotZero(t, rc, "backup without -f should fail")
	})
}

func TestRunRestoreRequiresFile(t *testing.T) {
	quietStdoutStderr(t, func() {
		rc := runRestore(t.Context(), nil)
		assert.NotZero(t, rc, "restore without -f should fail")
	})
}

func TestBackupRestoreRoundTripSQLite(t *testing.T) {
	// Two independent SQLite databases: back up the source, then restore into the initially-empty destination
	srcConn := "file:" + filepath.Join(t.TempDir(), "src.db") + "?_pragma=busy_timeout(15000)"
	dstConn := "file:" + filepath.Join(t.TempDir(), "dst.db") + "?_pragma=busy_timeout(15000)"

	actor := ref.NewActorRef("testType", "actor-1")
	want := []byte("hello-backup")

	// Seed the source with one state entry, using the same provider construction path as the CLI
	seed, err := buildProvider(srcConn, testProviderConfig(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	err = seed.Init(t.Context())
	require.NoError(t, err)
	err = seed.SetState(t.Context(), actor, want, components.SetStateOpts{})
	require.NoError(t, err)

	backupFile := filepath.Join(t.TempDir(), "backup.francis")

	// Back up the source cluster
	t.Setenv("FRANCIS_CONFIG", writeProviderConfig(t, srcConn))
	rc := runBackup(t.Context(), []string{"-f", backupFile})
	require.Zero(t, rc, "backup should succeed")

	info, err := os.Stat(backupFile)
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "backup file should not be empty")

	// Restore into the empty destination cluster
	t.Setenv("FRANCIS_CONFIG", writeProviderConfig(t, dstConn))
	rc = runRestore(t.Context(), []string{"-f", backupFile})
	require.Zero(t, rc, "restore should succeed")

	// The destination now holds the seeded state
	dst, err := buildProvider(dstConn, testProviderConfig(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, dst.Init(t.Context()))

	got, err := dst.GetState(t.Context(), actor)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
