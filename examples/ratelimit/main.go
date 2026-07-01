package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/italypaleale/francis/builtin/ratelimit"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host/local"
)

// runtimePSK is the shared cluster key from which the CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts sharing the key authenticate each other with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	err := run(context.Background(), log)
	if err != nil {
		log.Error("Error running ratelimit example", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(parentCtx context.Context, log *slog.Logger) error {
	// The demo cancels this context when it is done, which drains the host and ends the example
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// A rate limiter that admits 5 calls per second per key
	// The limiter is strict by default (no burst slack), so calls for a key are evenly spaced
	limiter, err := ratelimit.New("demo",
		ratelimit.WithRate(5),
	)
	if err != nil {
		return err
	}

	// A single-host local cluster using the in-memory provider: the rate limiter keeps no durable state, so nothing needs to be persisted
	h, err := local.NewHost(
		local.WithAddress("127.0.0.1:7581"),
		local.WithLogger(log.With("scope", "actor-host")),
		local.WithStandaloneMemoryProvider(standalone.StandaloneMemoryOptions{}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte(runtimePSK)),
		// Register the rate limiter as a built-in actor
		local.WithBuiltInActor(limiter),
		local.WithShutdownGracePeriod(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Run the host in the background; the demo drives it once it is ready
	errCh := make(chan error, 1)
	go func() { errCh <- h.Run(ctx) }()

	// Wait for the host to be ready before invoking
	select {
	case <-h.Ready():
	case err = <-errCh:
		return fmt.Errorf("host stopped before becoming ready: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Take is bound to the host's Service
	rl := limiter.Service(h.Service())

	log.Info("Throttling 8 rapid calls for one key, smoothed to 5/second")
	start := time.Now()
	for i := 1; i <= 8; i++ {
		err = rl.Take(ctx, "user-42")
		if err != nil {
			return fmt.Errorf("rate limit take failed: %w", err)
		}
		log.Info("Call admitted", slog.Int("n", i), slog.Duration("elapsed", time.Since(start).Round(time.Millisecond)))
	}

	// A different key has its own limiter, so its first call is admitted immediately
	start = time.Now()
	err = rl.Take(ctx, "user-99")
	if err != nil {
		return fmt.Errorf("rate limit take failed: %w", err)
	}
	log.Info("A different key is not throttled by the first", slog.Duration("elapsed", time.Since(start).Round(time.Millisecond)))

	// The demo is done: stop the host and wait for it to drain
	cancel()
	runErr := <-errCh
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return fmt.Errorf("failed to run actor host: %w", runErr)
	}

	return nil
}
