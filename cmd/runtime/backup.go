package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/italypaleale/francis/components"
	timeutils "github.com/italypaleale/francis/internal/time"
	"github.com/italypaleale/go-kit/observability"
)

// runBackup writes a portable snapshot of all persistent data to a file, or to stdout with "-f -"
// It opens a fresh connection to the configured database but does not register a host, so it is safe to run against a live cluster: the provider takes the snapshot inside a consistent read-only transaction
// The caller owns ctx and its cancellation, since a backup can stream a large amount of data
func runBackup(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)

	var file string
	fs.StringVar(&file, "f", "", "File to write the backup to; use '-' to write to stdout")
	_ = fs.Parse(args)

	if file == "" {
		fmt.Fprintln(os.Stderr, "Error: the -f flag is required (use '-' to write to stdout)")
		return 1
	}

	provider, log, shutdownFn, err := cliProvider(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	defer func() {
		_ = shutdownFn(context.WithoutCancel(ctx))
	}()

	err = provider.Init(ctx)
	if err != nil {
		log.Error("Error initializing provider", slog.Any("error", err))
		return 1
	}

	// Resolve the destination
	// Writing to stdout must not close it, and only a real file is cleaned up on failure
	var (
		w       io.Writer
		closeFn func() error
		cleanup func()
	)
	if file == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(file)
		if err != nil {
			log.Error("Error creating output file", slog.Any("error", err))
			return 1
		}

		// Buffer the writes, since the provider encodes many small records
		buf := bufio.NewWriter(f)
		w = buf
		closeFn = func() error {
			flushErr := buf.Flush()
			closeErr := f.Close()
			if flushErr != nil {
				return flushErr
			}
			return closeErr
		}
		cleanup = func() {
			_ = os.Remove(file)
		}
	}

	err = provider.Backup(ctx, w)
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		log.Error("Error creating backup", slog.Any("error", err))
		return 1
	}

	if closeFn != nil {
		err = closeFn()
		if err != nil {
			cleanup()
			log.Error("Error finalizing backup file", slog.Any("error", err))
			return 1
		}
		log.Info("Backup written", slog.String("file", file))
	}

	return 0
}

// runRestore wipes all existing persistent data and loads a snapshot from a file, or from stdin with "-f -"
// It refuses to run while any host is connected to the same database, since restoring underneath live hosts would corrupt running actors
// The caller owns ctx and its cancellation, since a restore can stream a large amount of data
func runRestore(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)

	var file string
	fs.StringVar(&file, "f", "", "File to read the backup from; use '-' to read from stdin")
	_ = fs.Parse(args)

	if file == "" {
		fmt.Fprintln(os.Stderr, "Error: the -f flag is required (use '-' to read from stdin)")
		return 1
	}

	provider, log, shutdownFn, err := cliProvider(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	defer func() {
		_ = shutdownFn(context.WithoutCancel(ctx))
	}()

	err = provider.Init(ctx)
	if err != nil {
		log.Error("Error initializing provider", slog.Any("error", err))
		return 1
	}

	// Resolve the source
	var r io.Reader
	if file == "-" {
		r = bufio.NewReader(os.Stdin)
	} else {
		f, err := os.Open(file)
		if err != nil {
			log.Error("Error opening backup file", slog.Any("error", err))
			return 1
		}
		defer func() {
			_ = f.Close()
		}()
		r = bufio.NewReader(f)
	}

	err = provider.Restore(ctx, r)

	// The provider refuses to restore while hosts are connected
	if errors.Is(err, components.ErrHostsConnected) {
		log.Error("Refusing to restore because one or more hosts are connected to the cluster. Stop all hosts connected to this database before restoring.")
		return 2
	} else if err != nil {
		log.Error("Error restoring backup", slog.Any("error", err))
		return 1
	}

	log.Info("Restore complete")
	return 0
}

// cliProvider loads the configuration and builds the actor provider for the backup and restore subcommands
// The provider only opens a database connection (it does not register a host)
func cliProvider(ctx context.Context) (components.ActorProvider, *slog.Logger, func(context.Context) error, error) {
	// Load config
	configPath, err := resolveConfigPath()
	if err != nil {
		return nil, nil, nil, err
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, nil, nil, err
	}

	// Init a logger that writes to stderr
	// We cannot write to stdout because it can be used for output
	logOpts := cfg.getObservabilityInitLogOpts()
	logOpts.Writer = os.Stderr
	log, logShutdown, err := observability.InitLogs(ctx, logOpts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize logging: %w", err)
	}

	// Mirror how run() resolves the provider config
	// The host health check deadline matters here because it defines which hosts count as "connected" when a restore checks for live hosts
	providerCfg := components.NewProviderConfig()
	providerCfg.HostHealthCheckDeadline = timeutils.ParseDurationDefault(cfg.HealthCheckDeadline, components.DefaultHostHealthCheckDeadline)

	provider, err := buildProvider(cfg.Provider.ConnectionString, providerCfg, log)
	if err != nil {
		return nil, nil, nil, err
	}

	return provider, log, logShutdown, nil
}
