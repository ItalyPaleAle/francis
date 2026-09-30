package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/italypaleale/go-kit/observability"
	"github.com/italypaleale/go-kit/utils"
	"gopkg.in/yaml.v3"

	"github.com/italypaleale/francis/internal/buildinfo"
	"github.com/italypaleale/francis/internal/management"
)

// runtimeIDEnvVar is the environment variable that overrides the runtimeId set in the config file
var runtimeIDEnvVar = buildinfo.ConfigEnvPrefix + "RUNTIME_ID"

// advertiseAddressEnvVar is the environment variable that overrides the advertiseAddress set in the config file
var advertiseAddressEnvVar = buildinfo.ConfigEnvPrefix + "ADVERTISE_ADDRESS"

// config is the on-disk configuration for the runtime binary
type config struct {
	Bind             string           `yaml:"bind"`
	RuntimeID        string           `yaml:"runtimeId"`
	AdvertiseAddress string           `yaml:"advertiseAddress"`
	Management       managementConfig `yaml:"management"`
	RuntimePSKs      []string         `yaml:"runtimePSKs"`
	Bootstrap        bootstrapConfig  `yaml:"bootstrap"`
	Provider         providerConfig   `yaml:"provider"`

	WorkloadCertTTL     string `yaml:"workloadCertTTL"`
	HealthCheckDeadline string `yaml:"healthCheckDeadline"`
	AlarmsPollInterval  string `yaml:"alarmsPollInterval"`
	AlarmsLeaseDuration string `yaml:"alarmsLeaseDuration"`
	ShutdownGracePeriod string `yaml:"shutdownGracePeriod"`
	MaxHosts            int    `yaml:"maxHosts"`

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

// managementConfig configures the optional management REST API
type managementConfig struct {
	// Enabled starts the management listener, which is off by default
	Enabled bool `yaml:"enabled"`
	// Bind is the TCP address of the management listener, which defaults to 0.0.0.0:7401
	Bind string `yaml:"bind"`
	// ReadOnlyTokens receive every scope except those ending in ":manage"
	ReadOnlyTokens []string `yaml:"readOnlyTokens"`
	// ManagementTokens receive every scope
	ManagementTokens []string `yaml:"managementTokens"`
	// TLS optionally serves the management API over HTTPS
	TLS managementTLSConfig `yaml:"tls"`
}

type managementTLSConfig struct {
	// CertFile is the path to the PEM-encoded certificate chain
	CertFile string `yaml:"certFile"`
	// KeyFile is the path to the PEM-encoded private key
	KeyFile string `yaml:"keyFile"`
}

// managementServerConfig validates the management configuration and returns the server configuration
func (cfg managementConfig) managementServerConfig() (management.Config, error) {
	res := management.Config{
		Bind:             cfg.Bind,
		ReadOnlyTokens:   cfg.ReadOnlyTokens,
		ManagementTokens: cfg.ManagementTokens,
	}

	switch {
	case cfg.TLS.CertFile == "" && cfg.TLS.KeyFile == "":
		// Serve plain HTTP
	case cfg.TLS.CertFile == "" || cfg.TLS.KeyFile == "":
		return res, errors.New("management.tls.certFile and management.tls.keyFile must be set together")
	default:
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return res, fmt.Errorf("failed to load the management TLS certificate: %w", err)
		}
		res.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	err := res.Validate()
	if err != nil {
		return res, err
	}

	return res, nil
}

type jwtConfig struct {
	Issuer     string `yaml:"issuer"`
	Audience   string `yaml:"audience"`
	JWKSURL    string `yaml:"jwksURL"`
	StaticJWKS string `yaml:"staticJWKS"`
}

type providerConfig struct {
	// ConnectionString selects and configures the data store
	// The backend is inferred from the connection string scheme:
	// - "postgres://" or "postgresql://" for PostgreSQL
	// - "memory" (or "memory://") for the non-durable in-memory store
	// - "standalone:memory" (or "standalone:memory://") for the same non-durable in-memory store
	// - "standalone:postgres://" or "standalone:postgresql://" for the standalone provider with PostgreSQL persistence
	// - any other "standalone:" value for the standalone provider with SQLite persistence
	// - anything else is treated as a SQLite file path or DSN
	ConnectionString string `yaml:"connectionString"`

	// QueryLog configures optional SQL statement logging
	QueryLog queryLogConfig `yaml:"queryLog"`

	// OperationLog configures optional provider-operation logging for every backend
	OperationLog durationLogConfig `yaml:"operationLog"`
}

type queryLogConfig struct {
	durationLogConfig `yaml:",inline"`

	// IncludeParameters includes query parameter values in traces and in SQL logs that include statement text
	// Parameter values may contain sensitive information and are excluded by default
	IncludeParameters bool `yaml:"includeParameters"`
}

type durationLogConfig struct {
	// Enabled logs every matching operation at Debug level with its duration
	Enabled bool `yaml:"enabled"`

	// SlowThreshold logs a Warn record for every matching operation that reaches this duration
	// Non-positive values use the default, which disables slow-operation warnings
	SlowThreshold time.Duration `yaml:"slowThreshold"`
}

// GetSlowThreshold returns the configured threshold or zero when the value is negative
func (cfg durationLogConfig) GetSlowThreshold() time.Duration {
	if cfg.SlowThreshold < 0 {
		return 0
	}

	return cfg.SlowThreshold
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

	// The runtime ID can be set per process with an env var, which overrides the config file
	// This lets replicas that share one config file get a separate identity
	runtimeID := os.Getenv(runtimeIDEnvVar)
	if runtimeID != "" {
		cfg.RuntimeID = runtimeID
	}

	advertiseAddress := os.Getenv(advertiseAddressEnvVar)
	if advertiseAddress != "" {
		cfg.AdvertiseAddress = advertiseAddress
	}

	return cfg, nil
}
