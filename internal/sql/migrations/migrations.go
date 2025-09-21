// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package migrations

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/italypaleale/francis/internal/sql/sqladapter"
)

// MigrationOptions contains options for the Migrate function.
type MigrationOptions struct {
	// List of migrations to execute.
	// Each item is a function that receives a context and the database connection, and can execute queries.
	Migrations []MigrationFn

	// EnsureMetadataTable ensures that the metadata table exists.
	EnsureMetadataTable func(ctx context.Context) error

	// GetVersionQuery is the query to execute to load the latest migration version.
	GetVersionQuery string

	// UpdateVersionQuery is a function that returns the query to update the migration version, and the arg.
	UpdateVersionQuery func(version string) (string, any)
}

type (
	MigrationFn         = func(ctx context.Context) error
	MigrationTeardownFn = func() error
)

// Migrate performs database migrations.
func Migrate(ctx context.Context, db sqladapter.DatabaseConn, opts MigrationOptions, logger *slog.Logger) (err error) {
	logger = logger.With(slog.String("component", "migrations"))
	logger.DebugContext(ctx, "Begin migrations")

	// Ensure that the metadata table exists
	if opts.EnsureMetadataTable != nil {
		logger.DebugContext(ctx, "Ensuring metadata table exists")
		err = opts.EnsureMetadataTable(ctx)
		if err != nil {
			return fmt.Errorf("failed to ensure metadata table exists: %w", err)
		}
	}

	// Select the migration level
	logger.DebugContext(ctx, "Loading current migration level")
	var (
		migrationLevelStr string
		migrationLevel    int
	)
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = db.QueryRow(queryCtx, opts.GetVersionQuery).Scan(&migrationLevelStr)
	cancel()
	switch {
	case db.IsNoRowsError(err):
		// If there's no row...
		migrationLevel = 0
	case err != nil:
		return fmt.Errorf("failed to read migration level: %w", err)
	default:
		migrationLevel, err = strconv.Atoi(migrationLevelStr)
		if err != nil || migrationLevel < 0 {
			return fmt.Errorf("invalid migration level found in metadata table: %s", migrationLevelStr)
		}
	}
	logger.DebugContext(ctx, "Loaded current migration level", slog.Int("level", migrationLevel))

	// Perform the migrations
	for i := migrationLevel; i < len(opts.Migrations); i++ {
		logger.InfoContext(ctx, "Performing migration", slog.Int("level", i))
		err = opts.Migrations[i](ctx)
		if err != nil {
			return fmt.Errorf("failed to perform migration %d: %w", i, err)
		}

		query, arg := opts.UpdateVersionQuery(strconv.Itoa(i + 1))
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		_, err = db.Exec(queryCtx, query, arg)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to update migration level in metadata table: %w", err)
		}
	}

	return nil
}
