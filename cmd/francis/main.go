package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/italypaleale/go-kit/signals"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"gopkg.in/yaml.v3"

	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/francis"
)

// Config represents the configuration file structure
type Config struct {
	Bind     string      `yaml:"bind"`
	TLS      TLSConfig   `yaml:"tls"`
	Provider ProviderConfig `yaml:"provider"`
	Log      LogConfig   `yaml:"log"`

	HealthCheckDeadline  string `yaml:"healthCheckDeadline"`
	AlarmsPollInterval   string `yaml:"alarmsPollInterval"`
	AlarmsLeaseDuration  string `yaml:"alarmsLeaseDuration"`
	ShutdownGracePeriod  string `yaml:"shutdownGracePeriod"`
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// ProviderConfig represents the provider configuration
type ProviderConfig struct {
	Type             string `yaml:"type"`
	ConnectionString string `yaml:"connectionString"`
}

// LogConfig represents logging configuration
type LogConfig struct {
	Level string `yaml:"level"`
}

var (
	configFile string
	log        *slog.Logger
)

func main() {
	flag.StringVar(&configFile, "config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize with a basic logger first
	log = initLogger(slog.LevelInfo)

	// Load configuration
	config, err := loadConfig(configFile)
	if err != nil {
		log.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}

	// Re-initialize logger with configured level
	logLevel := parseLogLevel(config.Log.Level)
	log = initLogger(logLevel)

	ctx := signals.SignalContext(context.Background())

	err = run(ctx, config)
	if err != nil {
		log.Error("Server error", slog.Any("error", err))
		os.Exit(1)
	}
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if config.Bind == "" {
		config.Bind = ":8443"
	}

	return &config, nil
}

func run(ctx context.Context, config *Config) error {
	// Build server options
	opts := []francis.ServerOption{
		francis.WithBindAddress(config.Bind),
		francis.WithLogger(log.With("component", "francis")),
	}

	// Configure TLS
	if config.TLS.Cert == "" || config.TLS.Key == "" {
		return fmt.Errorf("TLS certificate and key are required")
	}

	cert, err := tls.LoadX509KeyPair(config.TLS.Cert, config.TLS.Key)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h3"},
		MinVersion:   tls.VersionTLS13,
	}
	opts = append(opts, francis.WithTLSConfig(tlsConfig))

	// Configure provider
	switch config.Provider.Type {
	case "sqlite":
		opts = append(opts, francis.WithSQLiteProvider(sqlite.SQLiteProviderOptions{
			ConnectionString: config.Provider.ConnectionString,
		}))
	case "postgres":
		opts = append(opts, francis.WithPostgresProvider(postgres.PostgresProviderOptions{
			ConnectionString: config.Provider.ConnectionString,
		}))
	case "standalone-memory", "memory":
		opts = append(opts, francis.WithStandaloneMemoryProvider())
	case "standalone-sqlite":
		opts = append(opts, francis.WithStandaloneSQLiteProvider(standalone.StandaloneSQLiteProviderOptions{
			ConnectionString: config.Provider.ConnectionString,
		}))
	case "standalone-postgres":
		opts = append(opts, francis.WithStandalonePostgresProvider(standalone.StandalonePostgresProviderOptions{
			ConnectionString: config.Provider.ConnectionString,
		}))
	default:
		return fmt.Errorf("unknown provider type: %s", config.Provider.Type)
	}

	// Configure optional durations
	if config.HealthCheckDeadline != "" {
		d, err := time.ParseDuration(config.HealthCheckDeadline)
		if err != nil {
			return fmt.Errorf("invalid healthCheckDeadline: %w", err)
		}
		opts = append(opts, francis.WithHostHealthCheckDeadline(d))
	}

	if config.AlarmsPollInterval != "" {
		d, err := time.ParseDuration(config.AlarmsPollInterval)
		if err != nil {
			return fmt.Errorf("invalid alarmsPollInterval: %w", err)
		}
		opts = append(opts, francis.WithAlarmsPollInterval(d))
	}

	if config.AlarmsLeaseDuration != "" {
		d, err := time.ParseDuration(config.AlarmsLeaseDuration)
		if err != nil {
			return fmt.Errorf("invalid alarmsLeaseDuration: %w", err)
		}
		opts = append(opts, francis.WithAlarmsLeaseDuration(d))
	}

	if config.ShutdownGracePeriod != "" {
		d, err := time.ParseDuration(config.ShutdownGracePeriod)
		if err != nil {
			return fmt.Errorf("invalid shutdownGracePeriod: %w", err)
		}
		opts = append(opts, francis.WithShutdownGracePeriod(d))
	}

	// Create and run server
	server, err := francis.NewServer(opts...)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	log.Info("Starting Francis server", slog.String("bind", config.Bind))
	return server.Run(ctx)
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

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
