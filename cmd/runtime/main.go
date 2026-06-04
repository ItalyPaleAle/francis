package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/italypaleale/go-kit/signals"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"gopkg.in/yaml.v3"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	timeutils "github.com/italypaleale/francis/internal/time"
	"github.com/italypaleale/francis/runtime"
)

// config is the on-disk configuration for the runtime binary
type config struct {
	Bind     string         `yaml:"bind"`
	TLS      tlsConfig      `yaml:"tls"`
	Provider providerConfig `yaml:"provider"`

	HealthCheckDeadline string `yaml:"healthCheckDeadline"`
	AlarmsPollInterval  string `yaml:"alarmsPollInterval"`
	AlarmsLeaseDuration string `yaml:"alarmsLeaseDuration"`
	ShutdownGracePeriod string `yaml:"shutdownGracePeriod"`

	Log logConfig `yaml:"log"`
}

type tlsConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

type providerConfig struct {
	// Type is one of: sqlite, postgres, memory
	Type             string `yaml:"type"`
	ConnectionString string `yaml:"connectionString"`
}

type logConfig struct {
	Level string `yaml:"level"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	log := initLogger(parseLogLevel(cfg.Log.Level))

	ctx := signals.SignalContext(context.Background())

	err = run(ctx, cfg, log)
	if err != nil {
		log.Error("Error running runtime", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config, log *slog.Logger) error {
	// Resolve durations, falling back to sensible defaults
	healthCheckDeadline := timeutils.ParseDurationDefault(cfg.HealthCheckDeadline, components.DefaultHostHealthCheckDeadline)
	alarmsLeaseDuration := timeutils.ParseDurationDefault(cfg.AlarmsLeaseDuration, components.DefaultAlarmsLeaseDuration)
	alarmsPollInterval := timeutils.ParseDurationDefault(cfg.AlarmsPollInterval, 1500*time.Millisecond)
	shutdownGracePeriod := timeutils.ParseDurationDefault(cfg.ShutdownGracePeriod, 30*time.Second)

	providerCfg := components.ProviderConfig{
		HostHealthCheckDeadline:   healthCheckDeadline,
		AlarmsLeaseDuration:       alarmsLeaseDuration,
		AlarmsFetchAheadInterval:  components.DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatch,
	}

	provider, err := buildProvider(cfg.Provider, providerCfg, log)
	if err != nil {
		return fmt.Errorf("failed to build provider: %w", err)
	}

	// Build the runtime options
	opts := []runtime.RuntimeOption{
		runtime.WithBind(cfg.Bind),
		runtime.WithLogger(log.With("scope", "runtime")),
		runtime.WithHostHealthCheckDeadline(healthCheckDeadline),
		runtime.WithAlarmsLeaseDuration(alarmsLeaseDuration),
		runtime.WithAlarmsPollInterval(alarmsPollInterval),
		runtime.WithShutdownGracePeriod(shutdownGracePeriod),
	}

	// Load the TLS certificate if configured, otherwise the runtime generates a self-signed one
	if cfg.TLS.Cert != "" && cfg.TLS.Key != "" {
		cert, certErr := tls.LoadX509KeyPair(cfg.TLS.Cert, cfg.TLS.Key)
		if certErr != nil {
			return fmt.Errorf("failed to load TLS certificate and key: %w", certErr)
		}
		opts = append(opts, runtime.WithServerTLSCertificate(&cert))
	}

	rt, err := runtime.NewRuntime(provider, opts...)
	if err != nil {
		return fmt.Errorf("failed to create runtime: %w", err)
	}

	return rt.Run(ctx)
}

// buildProvider constructs the actor provider selected in the configuration
func buildProvider(cfg providerConfig, providerCfg components.ProviderConfig, log *slog.Logger) (components.ActorProvider, error) {
	switch cfg.Type {
	case "sqlite":
		return sqlite.NewSQLiteProvider(log, sqlite.SQLiteProviderOptions{
			ConnectionString: cfg.ConnectionString,
		}, providerCfg)
	case "postgres":
		return postgres.NewPostgresProvider(log, postgres.PostgresProviderOptions{
			ConnectionString: cfg.ConnectionString,
		}, providerCfg)
	case "memory":
		return standalone.NewStandaloneMemory(log, standalone.StandaloneMemoryOptions{}, providerCfg)
	case "":
		return nil, errors.New("provider type is required")
	default:
		return nil, fmt.Errorf("unsupported provider type %q", cfg.Type)
	}
}

func loadConfig(path string) (*config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	cfg := &config{
		Bind: ":8443",
	}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func initLogger(level slog.Level) *slog.Logger {
	var handler slog.Handler
	if isatty.IsTerminal(os.Stdout.Fd()) {
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
