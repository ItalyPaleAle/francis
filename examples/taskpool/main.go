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

	"github.com/italypaleale/francis/builtin/taskpool"
	"github.com/italypaleale/francis/host/local"
)

// runtimePSK is the shared cluster key from which the CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts sharing the key authenticate each other with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

// convertRequest is the payload of a video-conversion task
type convertRequest struct {
	Source string `msgpack:"source"`
	Target string `msgpack:"target"`
}

func main() {
	// SignalContext cancels the context on SIGINT/SIGTERM, so Ctrl+C drains the host cleanly
	ctx := signals.SignalContext(context.Background())

	err := run(ctx)
	if err != nil {
		slog.Default().Error("Error running taskpool example", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Build a task pool that "converts" videos, running at most two at a time on this host
	// This host also advertises the "gpu" capability, so it can run tasks that require a GPU
	// Run more hosts (sharing the same database and runtime PSK) to process more videos in parallel; each host runs at most its own WithConcurrency tasks at once
	pool, err := taskpool.New("video-convert",
		taskpool.WithConcurrency(2),
		taskpool.WithCapability("gpu"),
		taskpool.WithLogger(log.With("scope", "taskpool")),
		taskpool.WithHandler(func(ctx context.Context, task taskpool.Task) error {
			var req convertRequest
			decErr := task.Decode(&req)
			if decErr != nil {
				return fmt.Errorf("failed to decode task: %w", decErr)
			}

			// Simulate a long-running conversion, respecting cancellation so a draining host stops promptly
			log.Info("Converting video", slog.String("source", req.Source), slog.String("capability", task.Capability()))
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}

			// A task pool does not track results: it is up to you to communicate the outcome
			// In a real app you might write a row to a database, call an API, upload the output, or invoke another actor here
			log.Info("Converted video", slog.String("source", req.Source), slog.String("target", req.Target))
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

	// A single-host local cluster, using the standalone SQLite provider for persisting the queue across restarts
	h, err := local.NewHost(
		local.WithAddress("127.0.0.1:7571"),
		local.WithLogger(log.With("scope", "actor-host")),
		local.WithStandaloneSQLiteProvider(local.StandaloneSQLiteProviderOptions{
			DB: db,
		}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte(runtimePSK)),
		local.WithShutdownGracePeriod(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register the task pool as a built-in actor, before the host starts
	err = h.RegisterBuiltInActor(pool)
	if err != nil {
		return fmt.Errorf("failed to register task pool: %w", err)
	}

	// Submit a handful of tasks once the host is ready, then let them drain across the pool
	go submitTasks(ctx, log, h, pool)

	log.Info("Starting task pool example")

	// Run blocks until the context is canceled (Ctrl+C) and the host drains
	err = h.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run actor host: %w", err)
	}

	return nil
}

// submitTasks waits for the host to be ready and then submits a batch of conversion tasks
func submitTasks(ctx context.Context, log *slog.Logger, h *local.Host, pool *taskpool.TaskPool) {
	// Wait until the host has registered with the provider so submissions have somewhere to run
	select {
	case <-h.Ready():
	case <-ctx.Done():
		return
	}

	svc := pool.Service(h.Service())

	// Submit several plain tasks: any host can run these
	for i := range 5 {
		req := convertRequest{
			Source: fmt.Sprintf("input-%d.mov", i),
			Target: fmt.Sprintf("output-%d.mp4", i),
		}

		taskID, err := svc.Submit(ctx, req)
		if err != nil {
			log.Error("Failed to submit task", slog.Any("error", err))
			continue
		}
		log.Info("Submitted task", slog.String("taskID", taskID), slog.String("source", req.Source))
	}

	// Submit one task that requires a GPU: only hosts advertising the "gpu" capability will run it
	gpuReq := convertRequest{Source: "input-gpu.mov", Target: "output-gpu.mp4"}
	taskID, err := svc.Submit(ctx, gpuReq, taskpool.WithRequiredCapability("gpu"))
	if err != nil {
		log.Error("Failed to submit GPU task", slog.Any("error", err))
		return
	}
	log.Info("Submitted GPU task", slog.String("taskID", taskID), slog.String("source", gpuReq.Source))
}
