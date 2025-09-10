// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/italypaleale/actors/internal/sql/sqladapter"
	"k8s.io/utils/clock"
)

type GarbageCollector interface {
	CleanupExpired() error
	io.Closer
}

type DeleteExpiredValuesQueryFn func() (string, func() []any)

type GCOptions struct {
	Logger *slog.Logger

	// Query that must atomically update the "last cleanup time" in the metadata table, but only if the garbage collector hasn't run already.
	// The caller will check the number of affected rows. If zero, it assumes that the GC has ran too recently, and will not proceed to delete expired records.
	// The query receives one parameter that is the last cleanup interval, in milliseconds.
	// The function must return both the query and the argument.
	UpdateLastCleanupQuery func(arg any) (string, []any)

	// Queries that perform the cleanup of all expired rows.
	DeleteExpiredValuesQueries map[string]DeleteExpiredValuesQueryFn

	// Interval to perfm the cleanup.
	CleanupInterval time.Duration

	// Timeout for the cleanup query
	// Default: 5 minutes
	CleanupQueryTimeout time.Duration

	// Database connection.
	// Must be adapted using AdaptDatabaseSQLConn or AdaptPgxConn.
	DB sqladapter.DatabaseConn

	// Optional clock
	Clock clock.WithTicker
}

type gc struct {
	log                        *slog.Logger
	updateLastCleanupQuery     func(arg any) (string, []any)
	deleteExpiredValuesQueries map[string]DeleteExpiredValuesQueryFn
	cleanupInterval            time.Duration
	cleanupQueryTimeout        time.Duration
	db                         sqladapter.DatabaseConn
	clock                      clock.WithTicker

	closed   atomic.Bool
	closedCh chan struct{}
	wg       sync.WaitGroup
}

func ScheduleGarbageCollector(opts GCOptions) (GarbageCollector, error) {
	if opts.DB == nil {
		return nil, errors.New("property DB must be provided")
	}

	if opts.Clock == nil {
		opts.Clock = &clock.RealClock{}
	}

	if opts.CleanupQueryTimeout <= 0 {
		// Deletion can take a long time to complete so we have a long timeout
		opts.CleanupQueryTimeout = 5 * time.Minute
	}

	gc := &gc{
		log:                        opts.Logger,
		updateLastCleanupQuery:     opts.UpdateLastCleanupQuery,
		deleteExpiredValuesQueries: opts.DeleteExpiredValuesQueries,
		cleanupInterval:            opts.CleanupInterval,
		cleanupQueryTimeout:        opts.CleanupQueryTimeout,
		db:                         opts.DB,
		clock:                      opts.Clock,
		closedCh:                   make(chan struct{}),
	}

	// Start the background task only if the interval is positive
	// Interval can be zero in situations like testing
	if opts.CleanupInterval > 0 {
		gc.wg.Add(1)
		go func() {
			defer gc.wg.Done()
			gc.scheduleCleanup()
		}()
	}

	return gc, nil
}

func (g *gc) scheduleCleanup() {
	g.log.Info("Schedule expired data clean up", slog.Duration("interval", g.cleanupInterval))

	ticker := g.clock.NewTicker(g.cleanupInterval)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ticker.C():
			err = g.CleanupExpired()
			if err != nil {
				g.log.Error("Error removing expired data", slog.Any("error", err))
			}
		case <-g.closedCh:
			g.log.Debug("Stopping background cleanup of expired data")
			return
		}
	}
}

// Exposed for testing.
func (g *gc) CleanupExpired() error {
	ctx, cancel := context.WithTimeout(context.Background(), g.cleanupQueryTimeout)
	defer cancel()

	g.wg.Add(1)
	go func() {
		// Wait for context cancellation or closing
		select {
		case <-ctx.Done():
		case <-g.closedCh:
		}
		cancel()
		g.wg.Done()
	}()

	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other processes too
	// We do this outside of a the transaction since it's atomic
	canContinue, err := g.updateLastCleanup(ctx)
	if err != nil {
		return fmt.Errorf("failed to read last cleanup time from database: %w", err)
	}
	if !canContinue {
		g.log.Debug("Last cleanup was performed too recently")
		return nil
	}

	// Delete the expired values
	for name, fn := range g.deleteExpiredValuesQueries {
		query, paramsFn := fn()
		rowsAffected, err := g.db.Exec(ctx, query, paramsFn()...)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		if rowsAffected > 0 {
			g.log.Info("Cleaned up expired rows", slog.String("name", name), slog.Int64("removed", rowsAffected))
		} else {
			g.log.Debug("No expired rows deleted", slog.String("name", name))
		}
	}
	return nil
}

// updateLastCleanup sets the 'last-cleanup' value only if it's less than cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (g *gc) updateLastCleanup(ctx context.Context) (bool, error) {
	// Query parameter: interval in ms
	// Subtract 100ms for some buffer
	query, params := g.updateLastCleanupQuery(g.cleanupInterval.Milliseconds() - 100)

	n, err := g.db.Exec(ctx, query, params...)
	if err != nil {
		return false, fmt.Errorf("error updating last cleanup time: %w", err)
	}

	return n > 0, nil
}

func (g *gc) Close() error {
	defer g.wg.Wait()

	if g.closed.CompareAndSwap(false, true) {
		close(g.closedCh)
	}

	return nil
}
