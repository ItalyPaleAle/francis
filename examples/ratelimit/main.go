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

	// A rate limiter that admits 5 calls per second per key, tolerating a short burst of 3
	// Allow never blocks: it reports whether a call is admitted and, when it is not, how long to wait before retrying
	limiter, err := ratelimit.New("demo",
		ratelimit.WithRate(5),
		ratelimit.WithBurst(3),
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

	// Run the host in the background
	// The demo drives it once it is ready
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

	// Allow is bound to the host's Service
	rl := limiter.Service(h.Service())

	log.Info("Sending 6 rapid calls for one key (rate 5/s, burst 3)")
	for i := 1; i <= 6; i++ {
		allowed, retryAfter, allowErr := rl.Allow(ctx, "user-42")
		if allowErr != nil {
			return fmt.Errorf("rate limit check failed: %w", allowErr)
		}
		if allowed {
			log.Info("Call admitted", slog.Int("n", i))
		} else {
			// retryAfter is what you would return in a Retry-After header alongside a 429
			log.Info("Call throttled", slog.Int("n", i), slog.Duration("retryAfter", retryAfter.Round(time.Millisecond)))
		}
	}

	// A different key has its own bucket, so its first call is admitted immediately
	allowed, _, err := rl.Allow(ctx, "user-99")
	if err != nil {
		return fmt.Errorf("rate limit check failed: %w", err)
	}
	log.Info("A different key is not throttled by the first", slog.Bool("allowed", allowed))

	// The demo is done: stop the host and wait for it to drain
	cancel()
	runErr := <-errCh
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return fmt.Errorf("failed to run actor host: %w", runErr)
	}

	return nil
}
