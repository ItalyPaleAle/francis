package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/signals"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/providerfactory"
	"github.com/italypaleale/francis/internal/runtime"
	timeutils "github.com/italypaleale/francis/internal/time"
)

func main() {
	ctx := signals.SignalContext(context.Background())

	// Check if there's a subcommand
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			// Probes the locally-running runtime over WebTransport, for use as the Docker HEALTHCHECK
			retCode := runHealthcheck(os.Args[2:])
			os.Exit(retCode)
		case "print-ca":
			// Drives and prints the cluster CA so operators can pin it out-of-band
			retCode := runPrintCA(os.Args[2:])
			os.Exit(retCode)
		case "backup":
			// Streams a portable snapshot of all persistent data to a file (or stdout)
			retCode := runBackup(ctx, os.Args[2:])
			os.Exit(retCode)
		case "restore":
			// Loads a snapshot from a file (or stdin), wiping existing data
			retCode := runRestore(ctx, os.Args[2:])
			os.Exit(retCode)
		case "version":
			// Prints out the application version
			runVersion()
			os.Exit(0)
		}
	}

	// Resolve the config file from the FRANCIS_CONFIG env var or the well-known paths
	configPath, err := resolveConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}
	cfg.SetLoadedConfigPath(configPath)

	err = run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running runtime: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config) error {
	// Initialize observability before anything else so the runtime logs through the OpenTelemetry-bridged logger and traces/metrics are exported
	obs, err := initObservability(ctx, cfg)
	if err != nil {
		return err
	}
	log := obs.log

	// Register telemetry shutdown first so later resource defers run while exporters are still active
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), observabilityShutdownTimeout)
		defer shutdownCancel()

		shutdownErr := servicerunner.
			NewServiceRunner(obs.shutdownServices()...).
			Run(shutdownCtx)
		if shutdownErr != nil {
			log.Error("Error flushing telemetry on shutdown", slog.Any("error", shutdownErr))
		}
	}()

	// Resolve durations, falling back to sensible defaults
	healthCheckDeadline := timeutils.ParseDurationDefault(cfg.HealthCheckDeadline, components.DefaultHostHealthCheckDeadline)
	alarmsLeaseDuration := timeutils.ParseDurationDefault(cfg.AlarmsLeaseDuration, components.DefaultAlarmsLeaseDuration)
	alarmsPollInterval := timeutils.ParseDurationDefault(cfg.AlarmsPollInterval, 1500*time.Millisecond)
	shutdownGracePeriod := timeutils.ParseDurationDefault(cfg.ShutdownGracePeriod, 30*time.Second)
	workloadCertTTL := timeutils.ParseDurationDefault(cfg.WorkloadCertTTL, time.Hour)

	providerCfg := components.ProviderConfig{
		HostHealthCheckDeadline:   healthCheckDeadline,
		AlarmsLeaseDuration:       alarmsLeaseDuration,
		AlarmsFetchAheadInterval:  components.DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatchSize,
		MaxHosts:                  cfg.MaxHosts,
	}

	provider, err := buildProvider(cfg.Provider, providerCfg, log)
	if err != nil {
		return fmt.Errorf("failed to build provider: %w", err)
	}

	// The runtime does not own the provider it is given, so this function closes it once the runtime has drained
	defer func() {
		closeErr := provider.Close()
		if closeErr != nil {
			log.Error("Error closing provider on shutdown", slog.Any("error", closeErr))
		}
	}()

	// Derive the runtime PSKs the cluster CA is built from
	psks, err := cfg.parsePSKs()
	if err != nil {
		return err
	}

	// Select the host bootstrap method
	bootstrapOpt, err := bootstrapOption(cfg.Bootstrap)
	if err != nil {
		return err
	}

	opts := []runtime.RuntimeOption{
		runtime.WithBind(cfg.Bind),
		runtime.WithRuntimePSKs(psks...),
		runtime.WithWorkloadCertTTL(workloadCertTTL),
		bootstrapOpt,
		runtime.WithLogger(log.With("scope", "runtime")),
		runtime.WithMeter(obs.meter),
		runtime.WithHostHealthCheckDeadline(healthCheckDeadline),
		runtime.WithAlarmsPollInterval(alarmsPollInterval),
		runtime.WithShutdownGracePeriod(shutdownGracePeriod),
	}
	if cfg.RuntimeID != "" {
		opts = append(opts, runtime.WithRuntimeID(cfg.RuntimeID))
	}

	rt, err := runtime.NewRuntime(provider, opts...)
	if err != nil {
		return fmt.Errorf("failed to create runtime: %w", err)
	}

	// Run the runtime
	// This blocks until the context is canceled and the runtime has drained
	runErr := servicerunner.
		NewServiceRunner(rt.Run).
		Run(ctx)

	return runErr
}

// bootstrapOption builds the runtime option for the configured host bootstrap method
func bootstrapOption(cfg bootstrapConfig) (runtime.RuntimeOption, error) {
	switch strings.ToLower(cfg.Method) {
	case "psk":
		if cfg.HostPSK == "" {
			return nil, errors.New("bootstrap.hostPSK is required for PSK bootstrap")
		}
		return runtime.WithHostBootstrapPSK([]byte(cfg.HostPSK)), nil
	case "jwt":
		jcfg := bootstrapauth.JWTConfig{
			Issuer:   cfg.JWT.Issuer,
			Audience: cfg.JWT.Audience,
			JWKSURL:  cfg.JWT.JWKSURL,
		}

		if cfg.JWT.StaticJWKS != "" {
			jcfg.StaticJWKS = json.RawMessage(cfg.JWT.StaticJWKS)
		}

		return runtime.WithHostBootstrapJWT(jcfg), nil
	case "":
		return nil, errors.New("bootstrap.method is required (psk or jwt)")
	default:
		return nil, fmt.Errorf("unsupported bootstrap method %q", cfg.Method)
	}
}

// buildProvider constructs the actor provider, inferring the backend from the connection string scheme
// Surrounding whitespace is removed before the connection string is passed to the provider
func buildProvider(cfg providerConfig, providerCfg components.ProviderConfig, log *slog.Logger) (components.ActorProvider, error) {
	connString := strings.TrimSpace(cfg.ConnectionString)
	if connString == "" {
		return nil, errors.New("provider.connectionString is required")
	}

	// Resolve the independent SQL and provider-operation log settings
	queryLog := components.QueryLogConfig{
		Enabled:           cfg.QueryLog.Enabled,
		IncludeParameters: cfg.QueryLog.IncludeParameters,
		SlowThreshold:     cfg.QueryLog.GetSlowThreshold(),
	}
	operationLog := components.OperationLogConfig{
		Enabled:       cfg.OperationLog.Enabled,
		SlowThreshold: cfg.OperationLog.GetSlowThreshold(),
	}

	connStringLC := strings.ToLower(connString)
	var opts components.ProviderOptions
	switch {
	// Postgres connection strings begin with "postgres://" or "postgresql://"
	case strings.HasPrefix(connStringLC, "postgres://"), strings.HasPrefix(connStringLC, "postgresql://"):
		opts = postgres.PostgresProviderOptions{
			ConnectionString: connString,
			QueryLog:         queryLog,
			OperationLog:     operationLog,
		}

	// The non-durable in-memory store is selected with the literal "memory" or the "memory://" scheme
	case connStringLC == "memory", strings.HasPrefix(connStringLC, "memory://"):
		opts = standalone.StandaloneMemoryOptions{
			OperationLog: operationLog,
		}

	// Anything else is treated as a SQLite file path or DSN
	default:
		opts = sqlite.SQLiteProviderOptions{
			ConnectionString: connString,
			QueryLog:         queryLog,
			OperationLog:     operationLog,
		}
	}

	// The factory also wraps the provider so every provider method call is traced
	return providerfactory.New(log, opts, providerCfg)
}
