package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/signals"
	"github.com/italypaleale/go-kit/utils"
	"gopkg.in/yaml.v3"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/buildinfo"
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

	// loadedConfigPath records where the config was loaded from, for kitconfig.Base
	loadedConfigPath string `yaml:"-"`
	// instanceID is the resolved OpenTelemetry instance ID, cached for kitconfig.Base
	instanceID string `yaml:"-"`
}

// bootstrapConfig selects how joining hosts authenticate
type bootstrapConfig struct {
	// Method is one of: psk, jwt
	Method string `yaml:"method"`
	// HostPSK is the host pre-shared key, used by the "psk" method
	HostPSK string `yaml:"hostPSK"`
	// JWT configures the "jwt" method
	JWT jwtConfig `yaml:"jwt"`
}

type jwtConfig struct {
	Issuer     string `yaml:"issuer"`
	Audience   string `yaml:"audience"`
	JWKSURL    string `yaml:"jwksURL"`
	StaticJWKS string `yaml:"staticJWKS"`
}

type providerConfig struct {
	// ConnectionString selects and configures the data store
	// The backend is inferred from the connection string scheme: "postgres://" or "postgresql://" for PostgreSQL, "memory" (or "memory://") for the non-durable in-memory store, and anything else is treated as a SQLite file path or DSN
	ConnectionString string `yaml:"connectionString"`
}

type logConfig struct {
	Level string `yaml:"level"`
	// JSON logs in JSON format when true, otherwise text or colorized text on a TTY
	JSON bool `yaml:"json"`
}

func main() {
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

	ctx := signals.SignalContext(context.Background())

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
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatch,
	}

	provider, err := buildProvider(cfg.Provider.ConnectionString, providerCfg, log)
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

// parsePSKs resolves the configured runtime PSK strings
func parsePSKs(in []string) ([][]byte, error) {
	if len(in) == 0 {
		return nil, errors.New("at least one runtime PSK is required (runtimePSKs)")
	}

	out := make([][]byte, len(in))
	for i, s := range in {
		if s == "" {
			return nil, fmt.Errorf("runtime PSK at index %d is empty", i)
		}
		out[i] = []byte(s)
	}
	return out, nil
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

// configEnvVar is the environment variable that points to the config file
var configEnvVar = buildinfo.ConfigEnvPrefix + "CONFIG"

// configSearchPaths are the well-known directories searched for a config file when the env var is not set, in order of precedence
var configSearchPaths = []string{".", "~/." + buildinfo.AppName, "/etc/" + buildinfo.AppName}

// configFileNames are the config file names searched for in each well-known path, in order of precedence
// We accept ".yml" and ".json" too, but always load them as YAML (YAML is a superset of JSON)
var configFileNames = []string{"config.yaml", "config.yml", "config.json"}

// resolveConfigPath determines the path to the config file
// It first honors the FRANCIS_CONFIG env var, then falls back to searching the well-known paths
func resolveConfigPath() (string, error) {
	// First, try with the FRANCIS_CONFIG env var
	configFile := os.Getenv(configEnvVar)
	if configFile != "" {
		exists, _ := utils.FileExists(configFile)
		if !exists {
			return "", fmt.Errorf("environment variable %s points to a file that does not exist: %q", configEnvVar, configFile)
		}
		return configFile, nil
	}

	// Otherwise, look in the well-known paths
	configFile = findConfigFiles(configFileNames, configSearchPaths)
	if configFile == "" {
		return "", fmt.Errorf("no configuration file found: set %s or place a config file in one of %s", configEnvVar, strings.Join(configSearchPaths, ", "))
	}

	return configFile, nil
}

// findConfigFiles returns the first existing file among fileNames across all searchPaths, preferring earlier file names
func findConfigFiles(fileNames []string, searchPaths []string) string {
	for _, name := range fileNames {
		path := findConfigFile(name, searchPaths)
		if path != "" {
			return path
		}
	}

	return ""
}

// findConfigFile returns the first searchPath that contains fileName, or an empty string if none does
func findConfigFile(fileName string, searchPaths []string) string {
	for _, path := range searchPaths {
		if path == "" {
			continue
		}

		// Expand a leading "~" to the user's home directory
		path = expandHome(path)

		search := filepath.Join(path, fileName)
		exists, _ := utils.FileExists(search)
		if exists {
			return search
		}
	}

	return ""
}

// expandHome expands a leading "~" in path to the current user's home directory, leaving the path unchanged if it can't be resolved
func expandHome(path string) string {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path
	}

	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}

	if path == "~" {
		return home
	}

	return filepath.Join(home, path[len("~/"):])
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
