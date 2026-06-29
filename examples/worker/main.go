package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/signals"

	"github.com/italypaleale/francis/host/local"
)

var (
	log              *slog.Logger
	actorHostAddress string
	workerAddress    string
)

// runtimePSK is the shared cluster key from which the CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts sharing the key authenticate each other with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

func main() {
	flag.StringVar(&actorHostAddress, "actor-host-address", "127.0.0.1:7571", "Address and port for the actor host (peer server) to bind to and advertise to other hosts")
	flag.StringVar(&workerAddress, "worker-address", "127.0.0.1:8081", "Address and port for the example worker to bind to")
	flag.Parse()

	ctx := signals.SignalContext(context.Background())

	err := runWorker(ctx)
	if err != nil {
		slog.Default().Error("Error running worker", slog.Any("error", err))
		os.Exit(1)
	}
}

func runWorker(ctx context.Context) error {
	// Initialize observability, driven by the standard OTEL_* environment variables
	// This installs the global tracer and trace-context propagator, so Francis records and propagates spans across hosts
	shutdownFns, err := initObservability(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize observability: %w", err)
	}

	// Options for the host
	opts := []local.HostOption{
		local.WithAddress(actorHostAddress),
		local.WithLogger(log.With("scope", "actor-host")),
		local.WithSQLiteProvider(local.SQLiteProviderOptions{
			ConnectionString: "data.db",
		}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte(runtimePSK)),
		local.WithShutdownGracePeriod(10 * time.Second),
	}

	// Create a new actor host
	h, err := local.NewHost(opts...)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register all supported actors
	err = h.RegisterActor("myactor", NewMyActor, local.RegisterActorOptions{
		IdleTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to register actor 'myactor': %w", err)
	}

	// Get the service
	actorService := h.Service()

	// Run the actor host and the control server
	// This blocks until the context is canceled and the services have drained
	err = servicerunner.
		NewServiceRunner(
			h.Run,
			runControlServer(actorService),
		).
		Run(ctx)

	// Run all the shutdown methods
	// The context has already been canceled so we use a background one here
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	shutdownErr := servicerunner.
		NewServiceRunner(shutdownFns...).
		Run(shutdownCtx)
	if shutdownErr != nil {
		log.Error("Error flushing telemetry on shutdown", slog.Any("error", shutdownErr))
	}

	if err != nil {
		return fmt.Errorf("error running services: %w", err)
	}

	return nil
}
