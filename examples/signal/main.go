package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/italypaleale/francis/builtin/signal"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host/local"
)

// runtimePSK is the shared cluster key from which the CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts sharing the key authenticate each other with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

// deployResult is the payload this example broadcasts to everyone waiting on a deployment
type deployResult struct {
	Version string
	Healthy bool
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	err := run(context.Background(), log)
	if err != nil {
		log.Error("Error running signal example", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(parentCtx context.Context, log *slog.Logger) error {
	// The example cancels this context when it is done, which drains the host and ends the example
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// A signal set whose completions stay readable for an hour, so a caller that shows up late is still answered immediately
	deploys, err := signal.New("deploys",
		signal.WithRetention(time.Hour),
	)
	if err != nil {
		return err
	}

	// A single-host local cluster using the in-memory provider, which is enough for an example that does not outlive the process
	h, err := local.NewHost(
		local.WithAddress("127.0.0.1:7582"),
		local.WithLogger(log.With("scope", "actor-host")),
		local.WithStandaloneMemoryProvider(standalone.StandaloneMemoryOptions{}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte(runtimePSK)),
		local.WithShutdownGracePeriod(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register the signal set as a built-in actor, before the host starts
	err = h.RegisterBuiltInActor(deploys)
	if err != nil {
		return fmt.Errorf("failed to register signal set: %w", err)
	}

	// Run the host in the background
	// The example drives it once it is ready
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.Run(ctx)
	}()

	// Wait for the host to be ready before invoking
	select {
	case <-h.Ready():
		// All good, no-op
	case err = <-errCh:
		return fmt.Errorf("host stopped before becoming ready: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	// The signal operations are bound to the host's Service
	sig := deploys.Service(h.Service())

	const deploymentID = "deploy-2024-11-05"

	// Park several waiters on the same signal, as separate callers would from anywhere in the cluster
	log.Info("Starting waiters", slog.String("deploymentID", deploymentID))
	var waiters sync.WaitGroup
	for i := 1; i <= 5; i++ {
		waiters.Go(func() {
			// This blocks for as long as it takes: a signal has no timeout of its own, so bound it with the context when a caller should give up
			env, wErr := sig.Wait(ctx, deploymentID)
			if wErr != nil {
				log.Error("Waiter gave up", slog.Int("waiter", i), slog.Any("error", wErr))
				return
			}

			var res deployResult
			if env != nil {
				wErr = env.Decode(&res)
				if wErr != nil {
					log.Error("Waiter could not decode the payload", slog.Int("waiter", i), slog.Any("error", wErr))
					return
				}
			}

			log.Info("Waiter released", slog.Int("waiter", i), slog.String("version", res.Version), slog.Bool("healthy", res.Healthy))
		})
	}

	// Give the waiters a moment to park, so the completion really does have to release them
	time.Sleep(500 * time.Millisecond)

	// One caller fires the signal, which returns as soon as the completion is durable
	log.Info("Completing the signal")
	err = sig.Complete(ctx, deploymentID, deployResult{Version: "v2.1.0", Healthy: true})
	if err != nil {
		return fmt.Errorf("failed to complete the signal: %w", err)
	}

	waiters.Wait()

	// A caller arriving after the completion does not block at all: it is answered from the durable record
	log.Info("Waiting again, after the signal has already fired")
	env, err := sig.Wait(ctx, deploymentID)
	if err != nil {
		return fmt.Errorf("failed to wait for the signal: %w", err)
	}

	var late deployResult
	err = env.Decode(&late)
	if err != nil {
		return fmt.Errorf("failed to decode the payload: %w", err)
	}

	log.Info("Late caller answered immediately", slog.String("version", late.Version))

	// A signal fires once: a second completion changes nothing and says so
	err = sig.Complete(ctx, deploymentID, deployResult{Version: "v2.2.0"})
	if errors.Is(err, signal.ErrAlreadyCompleted) {
		log.Info("A second completion was rejected, and the first payload still stands")
	} else if err != nil {
		return fmt.Errorf("failed to complete the signal: %w", err)
	}

	// Check is the non-blocking version, for callers that want to decide for themselves whether to wait
	_, completed, err := sig.Check(ctx, "deploy-that-never-ran")
	if err != nil {
		return fmt.Errorf("failed to check the signal: %w", err)
	}
	log.Info("A signal that never fired reads as pending", slog.Bool("completed", completed))

	// The example is done: stop the host and wait for it to drain
	cancel()
	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("failed to run actor host: %w", err)
	}

	return nil
}
