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
	timeutils "github.com/italypaleale/francis/internal/time"
	"github.com/italypaleale/francis/runtime"
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
	}

	provider, err := buildProvider(cfg.Provider.ConnectionString, providerCfg, log)
	if err != nil {
		return fmt.Errorf("failed to build provider: %w", err)
	}

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

	// Run all the shutdown methods
	// The context has already been canceled so we use a background one here
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), observabilityShutdownTimeout)
	defer shutdownCancel()
	shutdownErr := servicerunner.
		NewServiceRunner(obs.shutdownServices()...).
		Run(shutdownCtx)
	if shutdownErr != nil {
		log.Error("Error flushing telemetry on shutdown", slog.Any("error", shutdownErr))
	}

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
// The original connection string is passed to the provider,
func buildProvider(connString string, providerCfg components.ProviderConfig, log *slog.Logger) (components.ActorProvider, error) {
	connString = strings.TrimSpace(connString)
	if connString == "" {
		return nil, errors.New("provider.connectionString is required")
	}

	connStringLC := strings.ToLower(connString)
	switch {
	// Postgres connection strings begin with "postgres://" or "postgresql://"
	case strings.HasPrefix(connStringLC, "postgres://"), strings.HasPrefix(connStringLC, "postgresql://"):
		return postgres.NewPostgresProvider(log, postgres.PostgresProviderOptions{
			ConnectionString: connString,
		}, providerCfg)

	// The non-durable in-memory store is selected with the literal "memory" or the "memory://" scheme
	case connStringLC == "memory", strings.HasPrefix(connStringLC, "memory://"):
		return standalone.NewStandaloneMemory(log, standalone.StandaloneMemoryOptions{}, providerCfg)

	// Anything else is treated as a SQLite file path or DSN
	default:
		return sqlite.NewSQLiteProvider(log, sqlite.SQLiteProviderOptions{
			ConnectionString: connString,
		}, providerCfg)
	}
}
