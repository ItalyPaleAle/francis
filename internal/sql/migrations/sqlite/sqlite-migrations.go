// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/italypaleale/actors/internal/sql/migrations"
	"github.com/italypaleale/actors/internal/sql/sqladapter"
)

// Migrations performs migrations for the database schema
type Migrations struct {
	Pool              *sql.DB
	MetadataTableName string
	MetadataKey       string

	conn *sql.Conn
}

// Perform the required migrations
func (m *Migrations) Perform(ctx context.Context, migrationFns []migrations.MigrationFn, logger *slog.Logger) (err error) {
	// Get a connection so we can create a transaction
	m.conn, err = m.Pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection from the pool: %w", err)
	}
	defer m.conn.Close()

	// Begin an exclusive transaction
	// We can't use Begin because that doesn't allow us setting the level of transaction
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	_, err = m.conn.ExecContext(queryCtx, "BEGIN EXCLUSIVE TRANSACTION")
	cancel()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback the transaction in a deferred statement to catch errors
	success := false
	defer func() {
		if success {
			return
		}
		queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
		_, rollbackErr := m.conn.ExecContext(queryCtx, "ROLLBACK TRANSACTION")
		cancel()
		if rollbackErr != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving transactions open
			logger.ErrorContext(ctx, "Failed to rollback transaction", slog.Any("error", rollbackErr))
			os.Exit(1)
		}
	}()

	// Perform the migrations
	err = migrations.Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(m.conn), migrations.MigrationOptions{
		// Yes, we are using fmt.Sprintf for adding a value in a query.
		// This comes from a constant hardcoded at development-time, and cannot be influenced by users. So, no risk of SQL injections here.
		GetVersionQuery: fmt.Sprintf(`SELECT value FROM %s WHERE key = '%s'`, m.MetadataTableName, m.MetadataKey),
		UpdateVersionQuery: func(version string) (string, any) {
			return fmt.Sprintf(`REPLACE INTO %s (key, value) VALUES ('%s', ?)`, m.MetadataTableName, m.MetadataKey),
				version
		},
		EnsureMetadataTable: func(ctx context.Context) error {
			// Check if the metadata table exists, which we also use to store the migration level
			queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
			var exists bool
			exists, err = m.tableExists(queryCtx, m.conn)
			cancel()
			if err != nil {
				return fmt.Errorf("failed to check if the metadata table exists: %w", err)
			}

			// If the table doesn't exist, create it
			if !exists {
				queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
				err = m.createMetadataTable(queryCtx, m.conn, logger)
				cancel()
				if err != nil {
					return fmt.Errorf("failed to create metadata table: %w", err)
				}
			}

			return nil
		},
		Migrations: migrationFns,
	}, logger)
	if err != nil {
		return err
	}

	// Commit the transaction
	queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
	_, err = m.conn.ExecContext(queryCtx, "COMMIT TRANSACTION")
	cancel()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Set success to true so we don't also run a rollback
	success = true

	return nil
}

// GetConn returns the active connection.
func (m *Migrations) GetConn() *sql.Conn {
	return m.conn
}

// Returns true if a table exists
func (m Migrations) tableExists(parentCtx context.Context, db sqladapter.DatabaseSQLConn) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	var exists string
	// Returns 1 or 0 as a string if the table exists or not.
	const q = `SELECT EXISTS (
		SELECT name FROM sqlite_master WHERE type='table' AND name = ?
	) AS 'exists'`
	err := db.QueryRowContext(ctx, q, m.MetadataTableName).
		Scan(&exists)
	return exists == "1", err
}

func (m Migrations) createMetadataTable(ctx context.Context, db sqladapter.DatabaseSQLConn, logger *slog.Logger) error {
	logger.InfoContext(ctx, "Creating metadata table", slog.String("table", m.MetadataTableName))

	// Add an "IF NOT EXISTS" in case another process is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			key text NOT NULL PRIMARY KEY,
			value text NOT NULL
		)`,
		m.MetadataTableName,
	))
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}
