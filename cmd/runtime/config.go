package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/italypaleale/go-kit/observability"
	"github.com/italypaleale/go-kit/utils"
	"gopkg.in/yaml.v3"

	"github.com/italypaleale/francis/internal/buildinfo"
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
	MaxHosts int `yaml:"maxHosts"`

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

// parsePSKs resolves the configured runtime PSK strings
func (cfg *config) parsePSKs() ([][]byte, error) {
	if len(cfg.RuntimePSKs) == 0 {
		return nil, errors.New("at least one runtime PSK is required (runtimePSKs)")
	}

	out := make([][]byte, len(cfg.RuntimePSKs))
	for i, s := range cfg.RuntimePSKs {
		if s == "" {
			return nil, fmt.Errorf("runtime PSK at index %d is empty", i)
		}
		out[i] = []byte(s)
	}

	return out, nil
}

// loopbackBindAddr rewrites a runtime bind address as an IPv4 loopback dial target, keeping the port
// The healthcheck runs alongside the runtime, so it always probes the loopback regardless of the bound host
// It uses 127.0.0.1 rather than "localhost" because QUIC dials a single resolved address with no fallback: if "localhost" resolved to ::1 but the runtime binds to an IPv4 address, the probe would fail and needlessly mark the container unhealthy
func (cfg *config) loopbackBindAddr() (string, error) {
	_, port, err := net.SplitHostPort(cfg.Bind)
	if err != nil {
		return "", fmt.Errorf("invalid bind address '%s': %w", cfg.Bind, err)
	}
	if port == "" {
		return "", fmt.Errorf("bind address '%s' has no port", cfg.Bind)
	}

	return net.JoinHostPort("127.0.0.1", port), nil
}

func (cfg *config) getObservabilityInitLogOpts() observability.InitLogsOpts {
	return observability.InitLogsOpts{
		Level:      cfg.Log.Level,
		JSON:       cfg.Log.JSON,
		Config:     cfg,
		AppName:    buildinfo.AppName,
		AppVersion: buildinfo.AppVersion,
	}
}

// resolveConfigPath determines the path to the config file
// It first honors the FRANCIS_CONFIG env var, then falls back to searching the well-known paths
func resolveConfigPath() (string, error) {
	var (
		// configEnvVar is the environment variable that points to the config file
		configEnvVar = buildinfo.ConfigEnvPrefix + "CONFIG"
		// configSearchPaths are the well-known directories searched for a config file when the env var is not set, in order of precedence
		configSearchPaths = []string{".", "~/." + buildinfo.AppName, "/etc/" + buildinfo.AppName}
		// configFileNames are the config file names searched for in each well-known path, in order of precedence
		// We accept ".yml" (…if you really must!) and ".json" too, but always load them as YAML (YAML is a superset of JSON)
		configFileNames = []string{"config.yaml", "config.yml", "config.json"}
	)

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
