package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_config_loopbackBindAddr(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		path    string
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadConfig(tt.path)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			got, gotErr := cfg.loopbackBindAddr()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("loopbackBindAddr() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("loopbackBindAddr() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("loopbackBindAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoopbackBindAddr(t *testing.T) {
	var cfg config

	tests := []struct {
		bind string
		want string
	}{
		{":8443", "127.0.0.1:8443"},
		{"0.0.0.0:8443", "127.0.0.1:8443"},
		{"127.0.0.1:8443", "127.0.0.1:8443"},
		{"example.com:8443", "127.0.0.1:8443"},
	}
	for _, tt := range tests {
		cfg = config{Bind: tt.bind}
		got, err := cfg.loopbackBindAddr()
		require.NoError(t, err, "bind %q", tt.bind)
		assert.Equal(t, tt.want, got, "bind %q", tt.bind)
	}

	// Missing port and unparseable addresses are rejected
	cfg = config{Bind: "bad"}
	_, err := cfg.loopbackBindAddr()
	require.Error(t, err)

	cfg = config{Bind: ":"}
	_, err = cfg.loopbackBindAddr()
	require.Error(t, err)
}

func TestLoadConfigRuntimeIDEnvOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("runtimeId: from-file\n"), 0o600)
	require.NoError(t, err)

	// Without the env var, the config file value is used
	t.Setenv(runtimeIDEnvVar, "")
	cfg, err := loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "from-file", cfg.RuntimeID)

	// The env var overrides the config file value
	t.Setenv(runtimeIDEnvVar, "francis-1")
	cfg, err = loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "francis-1", cfg.RuntimeID)
}

func TestLoadConfigAdvertiseAddressEnvOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("advertiseAddress: from-file:7400\n"), 0o600)
	require.NoError(t, err)

	// Without the env var, the config file value is used
	t.Setenv(advertiseAddressEnvVar, "")
	cfg, err := loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "from-file:7400", cfg.AdvertiseAddress)

	// The env var overrides the config file value
	t.Setenv(advertiseAddressEnvVar, "francis-1.francis:7400")
	cfg, err = loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "francis-1.francis:7400", cfg.AdvertiseAddress)
}

func TestManagementServerConfig(t *testing.T) {
	readOnly := strings.Repeat("r", 32)
	manage := strings.Repeat("m", 32)

	t.Run("parses the management block", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		err := os.WriteFile(path, []byte("management:\n  enabled: true\n  bind: 0.0.0.0:7401\n  readOnlyTokens: ["+readOnly+"]\n  managementTokens: ["+manage+"]\n  allowedOrigins: [\"HTTP://LocalHost:7402\"]\n"), 0o600)
		require.NoError(t, err)

		cfg, err := loadConfig(path)
		require.NoError(t, err)
		require.True(t, cfg.Management.Enabled)

		res, err := cfg.Management.managementServerConfig()
		require.NoError(t, err)
		assert.Equal(t, "0.0.0.0:7401", res.Bind)
		assert.Equal(t, []string{readOnly}, res.ReadOnlyTokens)
		assert.Equal(t, []string{manage}, res.ManagementTokens)
		assert.Equal(t, []string{"http://localhost:7402"}, res.AllowedOrigins)
		assert.Nil(t, res.TLSConfig)
	})

	t.Run("rejects an invalid allowed origin", func(t *testing.T) {
		_, err := managementConfig{Enabled: true, ReadOnlyTokens: []string{readOnly}, AllowedOrigins: []string{"localhost:7402"}}.managementServerConfig()
		require.ErrorContains(t, err, "management allowed origin at index 0 is invalid")
	})

	t.Run("defaults the bind address", func(t *testing.T) {
		res, err := managementConfig{Enabled: true, ReadOnlyTokens: []string{readOnly}}.managementServerConfig()
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1:7401", res.Bind)
	})

	t.Run("rejects a short token", func(t *testing.T) {
		_, err := managementConfig{Enabled: true, ManagementTokens: []string{"short"}}.managementServerConfig()
		require.Error(t, err)
	})

	t.Run("requires both TLS files", func(t *testing.T) {
		_, err := managementConfig{Enabled: true, ReadOnlyTokens: []string{readOnly}, TLS: managementTLSConfig{CertFile: "cert.pem"}}.managementServerConfig()
		require.ErrorContains(t, err, "must be set together")
	})

	t.Run("fails on a missing TLS file", func(t *testing.T) {
		_, err := managementConfig{Enabled: true, ReadOnlyTokens: []string{readOnly}, TLS: managementTLSConfig{CertFile: "missing.pem", KeyFile: "missing.key"}}.managementServerConfig()
		require.ErrorContains(t, err, "failed to load the management TLS certificate")
	})
}
