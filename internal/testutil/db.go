//go:build unit

package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// SQLiteConnString creates a connection string for an in-memory SQLite DB that's unique for the test
func SQLiteConnString(t *testing.T) string {
	t.Helper()

	// Give the test database a unique name to make sure each test connects to its own DB
	hash := sha256.Sum256([]byte(t.Name()))
	dbName := hex.EncodeToString(hash[0:10])

	// Return the connection string
	return "file:" + dbName + "?mode=memory"
}

// PostgresTestDB creates a pgxpool that connects to a test Postgres database
func PostgresTestDB(t *testing.T, connString string, testSchema string, cleanup bool, afterConnect func(ctx context.Context, c *pgx.Conn) error) (conn *pgxpool.Pool) {
	t.Helper()

	// Parse the connection string
	cfg, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)

	// Set a callback so we can make sure that the schema exists after connecting, and setting the correct search path
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, rErr := c.Exec(queryCtx, `CREATE SCHEMA IF NOT EXISTS "`+testSchema+`"`)
		if rErr != nil {
			return fmt.Errorf("failed to ensure test schema '%s' exists: %w", testSchema, rErr)
		}

		queryCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, rErr = c.Exec(queryCtx, `SET SESSION search_path = "`+testSchema+`", pg_catalog, public`)
		if rErr != nil {
			return fmt.Errorf("failed to set search path for session: %w", rErr)
		}

		// Invoke the supplied afterConnect if present
		if afterConnect != nil {
			rErr = afterConnect(ctx, c)
			if rErr != nil {
				return rErr
			}
		}

		return nil
	}

	// Connect to the database
	conn, err = pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err, "Failed to connect to database")

	// Cleanup function that disconnects and optionally deletes the schema at the end of the tests
	t.Cleanup(func() {
		if cleanup {
			// Use a background context because t.Context() has been canceled already
			queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := conn.Exec(queryCtx, `DROP SCHEMA "`+testSchema+`" CASCADE`)
			require.NoErrorf(t, err, "Failed to drop test schema '%s'", testSchema)
		}

		conn.Close()
	})

	return conn
}
