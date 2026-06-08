package main

import (
	"context"
	"encoding/json"
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
	"github.com/italypaleale/francis/internal/bootstrapauth"
	timeutils "github.com/italypaleale/francis/internal/time"
	"github.com/italypaleale/francis/runtime"
)

// config is the on-disk configuration for the runtime binary
type config struct {
	Bind        string          `yaml:"bind"`
	RuntimeID   string          `yaml:"runtimeId"`
	RuntimePSKs []string        `yaml:"runtimePSKs"`
	Bootstrap   bootstrapConfig `yaml:"bootstrap"`
	Provider    providerConfig  `yaml:"provider"`

	WorkloadCertTTL     string `yaml:"workloadCertTTL"`
	HealthCheckDeadline string `yaml:"healthCheckDeadline"`
	AlarmsPollInterval  string `yaml:"alarmsPollInterval"`
	AlarmsLeaseDuration string `yaml:"alarmsLeaseDuration"`
	ShutdownGracePeriod string `yaml:"shutdownGracePeriod"`

	Log logConfig `yaml:"log"`
}

// bootstrapConfig selects how joining hosts authenticate
type bootstrapConfig struct {
	// Method is one of: psk, jwt
	Method string `yaml:"method"`
	// HostPSK is the host pre-shared key, used by the psk method
	HostPSK string `yaml:"hostPSK"`
	// JWT configures the jwt method
	JWT jwtConfig `yaml:"jwt"`
}

type jwtConfig struct {
	Issuer     string `yaml:"issuer"`
	Audience   string `yaml:"audience"`
	JWKSURL    string `yaml:"jwksURL"`
	StaticJWKS string `yaml:"staticJWKS"`
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
	// The print-ca subcommand derives and prints the cluster CA so operators can pin it out-of-band
	if len(os.Args) > 1 && os.Args[1] == "print-ca" {
		os.Exit(runPrintCA(os.Args[2:]))
	}

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
	workloadCertTTL := timeutils.ParseDurationDefault(cfg.WorkloadCertTTL, time.Hour)

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

	// Derive the runtime PSKs the cluster CA is built from
	psks, err := parsePSKs(cfg.RuntimePSKs)
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
		runtime.WithHostHealthCheckDeadline(healthCheckDeadline),
		runtime.WithAlarmsLeaseDuration(alarmsLeaseDuration),
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

	return rt.Run(ctx)
}

// runPrintCA derives the cluster CA from the configured runtime PSKs and writes the PEM-encoded certificates to stdout
func runPrintCA(args []string) int {
	fs := flag.NewFlagSet("print-ca", flag.ExitOnError)
	var configPath string
	fs.StringVar(&configPath, "config", "config.yaml", "Path to the configuration file")
	_ = fs.Parse(args)

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		return 1
	}

	psks, err := parsePSKs(cfg.RuntimePSKs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	bundle, err := runtime.CABundlePEM(psks...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error deriving CA: %v\n", err)
		return 1
	}

	for _, pem := range bundle {
		_, _ = os.Stdout.Write(pem)
	}
	return 0
}

// parsePSKs resolves the configured runtime PSK strings, expanding environment variables so secrets can be injected at deploy time
func parsePSKs(in []string) ([][]byte, error) {
	if len(in) == 0 {
		return nil, errors.New("at least one runtime PSK is required (runtimePSKs)")
	}

	out := make([][]byte, len(in))
	for i, s := range in {
		v := os.ExpandEnv(s)
		if v == "" {
			return nil, fmt.Errorf("runtime PSK at index %d is empty", i)
		}
		out[i] = []byte(v)
	}
	return out, nil
}

// bootstrapOption builds the runtime option for the configured host bootstrap method
func bootstrapOption(cfg bootstrapConfig) (runtime.RuntimeOption, error) {
	switch cfg.Method {
	case "psk":
		psk := os.ExpandEnv(cfg.HostPSK)
		if psk == "" {
			return nil, errors.New("bootstrap.hostPSK is required for PSK bootstrap")
		}
		return runtime.WithHostBootstrapPSK([]byte(psk)), nil
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
