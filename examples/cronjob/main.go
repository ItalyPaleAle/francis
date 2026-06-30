package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	// Blank import for the SQLite driver
	_ "modernc.org/sqlite"

	"github.com/italypaleale/go-kit/signals"

	"github.com/italypaleale/francis/builtin/cronjob"
	"github.com/italypaleale/francis/host/local"
)

// runtimePSK is the shared cluster key from which the CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts sharing the key authenticate each other with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

func main() {
	// SignalContext cancels the context on SIGINT/SIGTERM, so Ctrl+C drains the host cleanly
	ctx := signals.SignalContext(context.Background())

	err := run(ctx)
	if err != nil {
		slog.Default().Error("Error running cronjob example", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Build a cron job that prints a line every 10 seconds
	// WithImmediate also runs it once right away, if the job hasn't been registered before
	ticker, err := cronjob.New("console-ticker",
		cronjob.WithInterval(10*time.Second),
		cronjob.WithImmediate(),
		cronjob.WithJob(func(ctx context.Context) error {
			log.Info("Cron job tick", slog.Time("at", time.Now()))
			return nil
		}),
	)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite", "file:data.db?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer db.Close()

	// A single-host local cluster, using the standalone SQLite provider for persisting the actor registration across restarts
	h, err := local.NewHost(
		local.WithAddress("127.0.0.1:7571"),
		local.WithLogger(log.With("scope", "actor-host")),
		local.WithStandaloneSQLiteProvider(local.StandaloneSQLiteProviderOptions{
			DB: db,
		}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte(runtimePSK)),
		// Register the cron job as a built-in actor
		// The host bootstraps it once it is ready
		local.WithBuiltInActor(ticker),
		local.WithShutdownGracePeriod(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	log.Info("Starting cron job example")

	// Run blocks until the context is canceled (Ctrl+C) and the host drains
	err = h.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run actor host: %w", err)
	}

	return nil
}
