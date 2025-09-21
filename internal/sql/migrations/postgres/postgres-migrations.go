// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package pgmigrations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/italypaleale/francis/internal/sql/migrations"
	"github.com/italypaleale/francis/internal/sql/sqladapter"
)

// Migrations performs migrations for the database schema
type Migrations struct {
	DB                sqladapter.PGXPoolConn
	MetadataTableName string
	MetadataKey       string
}

// Perform the required migrations
func (m Migrations) Perform(ctx context.Context, migrationFns []migrations.MigrationFn, logger *slog.Logger) error {
	// Ensure the metadata table exists
	// This query uses an "IF NOT EXISTS" so it's safe to be created concurrently
	err := m.EnsureMetadataTable(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to ensure metadata table exists: %w", err)
	}

	// Normally, the way to acquire an exclusive lock in Postgres (commonly used for migrations by other frameworks too) is to use advisory locks
	// However, advisory locks aren't supported in all Postgres-compatible databases, for example CockroachDB
	// So, we're going to write a row in there (not using a transaction, as that causes a table-level lock to be created), ignoring duplicates
	const lockKey = "lock"
	logger.DebugContext(ctx, "Ensuring lock row exists in metadata table", slog.String("lockKey", lockKey))
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = m.DB.Exec(queryCtx, fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, 'lock') ON CONFLICT (key) DO NOTHING", m.MetadataTableName), lockKey)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to ensure lock row '%s' exists: %w", lockKey, err)
	}

	// Now, let's use a transaction on a row in the metadata table as a lock
	logger.DebugContext(ctx, "Starting transaction pre-migration")
	queryCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
	tx, err := m.DB.Begin(queryCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Always rollback the transaction at the end to release the lock, since the value doesn't really matter
	defer func() {
		logger.DebugContext(ctx, "Releasing migration lock")
		queryCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
		rollbackErr := tx.Rollback(queryCtx)
		cancel()
		if rollbackErr != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			logger.ErrorContext(ctx, "Failed to rollback transaction", slog.Any("error", rollbackErr))
			os.Exit(1)
		}
	}()

	// Now, perform a SELECT with FOR UPDATE to lock the row used for locking, and only that row
	// We use a long timeout here as this query may block
	logger.DebugContext(ctx, "Acquiring migration lock")
	queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
	var lock string
	err = tx.QueryRow(queryCtx, fmt.Sprintf("SELECT value FROM %s WHERE key = $1 FOR UPDATE", m.MetadataTableName), lockKey).Scan(&lock)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to acquire migration lock (row-level lock on key '%s'): %w", lockKey, err)
	}
	logger.DebugContext(ctx, "Migration lock acquired")

	return migrations.Migrate(ctx, sqladapter.AdaptPgxConn(m.DB), migrations.MigrationOptions{
		// Yes, we are using fmt.Sprintf for adding a value in a query.
		// This comes from a constant hardcoded at development-time, and cannot be influenced by users. So, no risk of SQL injections here.
		GetVersionQuery: fmt.Sprintf(`SELECT value FROM %s WHERE key = '%s'`, m.MetadataTableName, m.MetadataKey),
		UpdateVersionQuery: func(version string) (string, any) {
			return fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ('%s', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, m.MetadataTableName, m.MetadataKey),
				version
		},
		Migrations: migrationFns,
	}, logger)
}

func (m Migrations) EnsureMetadataTable(ctx context.Context, logger *slog.Logger) (err error) {
	logger.InfoContext(ctx, "Creating metadata table", slog.String("table", m.MetadataTableName))
	// Add an "IF NOT EXISTS" in case another process is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	// Note that this query can fail with error `23505` on constraint `pg_type_typname_nsp_index` if ran in parallel; we will just retry that up to 3 times
	for range 3 {
		_, err = m.DB.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				key text NOT NULL PRIMARY KEY,
				value text NOT NULL
			)`,
			m.MetadataTableName,
		))
		if err == nil {
			break
		}

		// If the error is not a UniqueViolation (23505), abort
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.UniqueViolation {
			return fmt.Errorf("failed to create metadata table: %w", err)
		}

		// Retry after a delay
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}
