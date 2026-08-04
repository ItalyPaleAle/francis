package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func TestLoadConfigParsesMonitoringThresholds(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		wantQuery     time.Duration
		wantOperation time.Duration
		wantErr       bool
	}{
		{
			name: "valid durations",
			yaml: `provider:
  queryLog:
    slowThreshold: 250ms
  operationLog:
    slowThreshold: 2s
`,
			wantQuery:     250 * time.Millisecond,
			wantOperation: 2 * time.Second,
		},
		{
			name: "non-positive durations use defaults",
			yaml: `provider:
  queryLog:
    slowThreshold: 0s
  operationLog:
    slowThreshold: -1s
`,
		},
		{
			name: "invalid duration",
			yaml: `provider:
  queryLog:
    slowThreshold: invalid
`,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			err := os.WriteFile(path, []byte(test.yaml), 0o600)
			require.NoError(t, err)

			cfg, err := loadConfig(path)
			if test.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.wantQuery, cfg.Provider.QueryLog.GetSlowThreshold())
			assert.Equal(t, test.wantOperation, cfg.Provider.OperationLog.GetSlowThreshold())
		})
	}
}

func TestBuildProviderAppliesOperationLoggingToMemoryBackend(t *testing.T) {
	var output bytes.Buffer
	log := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	provider, err := buildProvider(providerConfig{
		ConnectionString: "memory",
		OperationLog: durationLogConfig{
			Enabled: true,
		},
	}, components.NewProviderConfig(), log)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeErr := provider.Close()
		assert.NoError(t, closeErr)
	})

	_, err = provider.GetState(t.Context(), ref.NewActorRef("test", "missing"))
	require.ErrorIs(t, err, components.ErrNoState)
	assert.Contains(t, output.String(), "Executed provider operation")
	assert.Contains(t, output.String(), "method=GetState")
}
