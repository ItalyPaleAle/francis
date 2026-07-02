// Package backup holds cross-provider backup/restore tests
// It lives in its own package so it can import several provider packages at once without creating an import cycle
package backup

import (
	"bytes"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/testutil"
)

// TestCrossProviderBackupRestore dumps a snapshot from one provider and restores it into a provider of a different type, confirming the format is portable
func TestCrossProviderBackupRestore(t *testing.T) {
	// The sample uses only explicit timestamps, and the comparison tolerates the millisecond truncation that SQLite and Postgres apply
	now := time.Now().Truncate(time.Millisecond)

	// crossRestore seeds src, backs it up, restores the snapshot into dst, then asserts a fresh backup of dst matches
	crossRestore := func(t *testing.T, src, dst components.ActorProvider) {
		t.Helper()
		ctx := t.Context()

		comptesting.SeedBackupSample(t, ctx, src, now)

		var srcBuf bytes.Buffer
		err := src.Backup(ctx, &srcBuf)
		require.NoError(t, err)

		srcContents := comptesting.DecodeBackup(t, srcBuf.Bytes())
		require.NotEmpty(t, srcContents.States)
		require.NotEmpty(t, srcContents.DeadJobs)

		err = dst.Restore(ctx, bytes.NewReader(srcBuf.Bytes()))
		require.NoError(t, err)

		var dstBuf bytes.Buffer
		err = dst.Backup(ctx, &dstBuf)
		require.NoError(t, err)

		got := comptesting.DecodeBackup(t, dstBuf.Bytes())
		comptesting.AssertBackupContentsEqual(t, srcContents, got)
	}

	t.Run("memory to sqlite", func(t *testing.T) {
		crossRestore(t, newMemory(t), newSQLite(t))
	})

	t.Run("sqlite to memory", func(t *testing.T) {
		crossRestore(t, newSQLite(t), newMemory(t))
	})

	t.Run("postgres to sqlite", func(t *testing.T) {
		pg := newPostgres(t)
		if pg == nil {
			// Test already skipped
			return
		}

		crossRestore(t, pg, newSQLite(t))
	})

	t.Run("memory to postgres", func(t *testing.T) {
		pg := newPostgres(t)
		if pg == nil {
			// Test already skipped
			return
		}

		crossRestore(t, newMemory(t), pg)
	})
}

func newMemory(t *testing.T) *standalone.StandaloneMemory {
	t.Helper()

	log := slog.New(slog.DiscardHandler)
	opts := standalone.StandaloneMemoryOptions{}
	p, err := standalone.NewStandaloneMemory(log, opts, comptesting.GetProviderConfig())
	require.NoError(t, err)

	err = p.Init(t.Context())
	require.NoError(t, err)

	return p
}

func newSQLite(t *testing.T) *sqlite.SQLiteProvider {
	t.Helper()

	log := slog.New(slog.DiscardHandler)
	opts := sqlite.SQLiteProviderOptions{
		ConnectionString: testutil.SQLiteConnString(t),
	}
	p, err := sqlite.NewSQLiteProvider(log, opts, comptesting.GetProviderConfig())
	require.NoError(t, err)

	err = p.Init(t.Context())
	require.NoError(t, err)

	return p
}

func newPostgres(t *testing.T) *postgres.PostgresProvider {
	t.Helper()

	connString := os.Getenv("TEST_POSTGRES_CONNSTRING")
	if connString == "" {
		t.Log("Skipping Postgres cross-provider legs: TEST_POSTGRES_CONNSTRING is not set")
		return nil
	}

	conn := testutil.PostgresTestDB(t, connString, "roundtrip", true, nil)

	log := slog.New(slog.DiscardHandler)
	opts := postgres.PostgresProviderOptions{
		DB: conn,
	}
	p, err := postgres.NewPostgresProvider(log, opts, comptesting.GetProviderConfig())
	require.NoError(t, err)

	err = p.Init(t.Context())
	require.NoError(t, err)

	return p
}
