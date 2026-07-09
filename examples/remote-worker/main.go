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
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

	"github.com/italypaleale/francis/host/remote"
)

var (
	log              *slog.Logger
	actorHostAddress string
	workerAddress    string
	runtimeAddress   string
)

// hostBootstrapPSK is the shared secret the host proves to the runtime when it first joins
// It must match the runtime's bootstrap.hostPSK in config.yaml
const hostBootstrapPSK = "example-host-bootstrap-psk-change-me"

func main() {
	flag.StringVar(&actorHostAddress, "actor-host-address", "127.0.0.1:7571", "Address and port for the actor host (peer server) to bind to and advertise to other hosts")
	flag.StringVar(&workerAddress, "worker-address", "127.0.0.1:8081", "Address and port for the example worker's control server to bind to")
	flag.StringVar(&runtimeAddress, "runtime-address", "127.0.0.1:7400", "Address and port of the Francis runtime to connect to")
	flag.Parse()

	log = initLogger(slog.LevelDebug)

	ctx := signals.SignalContext(context.Background())

	err := runWorker(ctx)
	if err != nil {
		log.Error("Error running worker", slog.Any("error", err))
		os.Exit(1)
	}
}

func runWorker(ctx context.Context) error {
	// Options for the host
	// Unlike the local example, this host does not embed a data store: it connects to the standalone runtime, which owns placement, state, and alarms
	opts := []remote.HostOption{
		remote.WithAddress(actorHostAddress),
		remote.WithRuntimeAddresses(runtimeAddress),
		// The host bootstraps with a shared PSK, the runtime then issues it a workload certificate, and all later connections (to the runtime and to peer hosts) use mTLS
		remote.WithHostBootstrapPSK([]byte(hostBootstrapPSK)),
		// This example trusts the runtime on first connection
		// For production, pin the cluster CA with remote.WithPinnedCA instead (see "runtime print-ca")
		remote.WithUnsafeNoPinnedCA(),
		remote.WithLogger(log.With("scope", "actor-host")),
		remote.WithShutdownGracePeriod(10 * time.Second),
	}

	// Create a new actor host
	h, err := remote.NewHost(opts...)
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register all supported actors
	err = h.RegisterActor("myactor", NewMyActor,
		remote.WithIdleTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to register actor 'myactor': %w", err)
	}

	// Get the service
	actorService := h.Service()

	err = servicerunner.
		NewServiceRunner(
			h.Run,
			runControlServer(actorService),
		).
		Run(ctx)
	if err != nil {
		return fmt.Errorf("error running services: %w", err)
	}

	return nil
}

func initLogger(level slog.Level) *slog.Logger {
	var handler slog.Handler
	if isatty.IsTerminal(os.Stdout.Fd()) {
		// Enable colors if we have a TTY
		handler = tint.NewHandler(os.Stdout, &tint.Options{
			TimeFormat: time.StampMilli,
			Level:      level,
		})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	}

	return slog.New(handler)
}
