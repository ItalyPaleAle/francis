// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package transactions

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ExecuteInSqlTransaction executes a function in a transaction for database/sql.
// If the handler returns an error, the transaction is rolled back automatically.
func ExecuteInSqlTransaction[T any](ctx context.Context, log *slog.Logger, db *sql.DB, fn func(ctx context.Context, tx *sql.Tx) (T, error)) (res T, err error) {
	// Start the transaction
	// Note that the context here is tied to the entire transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback in case of failure
	var success bool
	defer func() {
		if success {
			return
		}
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			// Log errors only
			log.ErrorContext(ctx, "Error while attempting to roll back transaction", slog.Any("error", rollbackErr))
		}
	}()

	// Execute the action
	res, err = fn(ctx, tx)
	if err != nil {
		return res, err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	success = true

	return res, nil
}

// ExecuteInPgxTransaction executes a function in a transaction for pgx.
// If the handler returns an error, the transaction is rolled back automatically.
func ExecuteInPgxTransaction[T any](ctx context.Context, log *slog.Logger, db *pgxpool.Pool, timeout time.Duration, fn func(ctx context.Context, tx pgx.Tx) (T, error)) (res T, err error) {
	// Start the transaction
	// Note that the context here is only used for the BEGIN command
	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()
	tx, err := db.Begin(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback in case of failure
	var success bool
	defer func() {
		if success {
			return
		}
		rollbackCtx, rollbackCancel := context.WithTimeout(ctx, timeout)
		defer rollbackCancel()
		rollbackErr := tx.Rollback(rollbackCtx)
		if rollbackErr != nil {
			// Log errors only
			log.ErrorContext(ctx, "Error while attempting to roll back transaction", slog.Any("error", rollbackErr))
		}
	}()

	// Execute the action
	res, err = fn(ctx, tx)
	if err != nil {
		return res, err
	}

	// Commit the transaction
	queryCtx, queryCancel = context.WithTimeout(ctx, timeout)
	defer queryCancel()
	err = tx.Commit(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	success = true

	return res, nil
}
