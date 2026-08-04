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
		name           string
		yaml           string
		wantQuery      time.Duration
		wantParameters bool
		wantOperation  time.Duration
		wantErr        bool
	}{
		{
			name: "valid durations",
			yaml: `provider:
  queryLog:
    includeParameters: true
    slowThreshold: 250ms
  operationLog:
    slowThreshold: 2s
`,
			wantQuery:      250 * time.Millisecond,
			wantParameters: true,
			wantOperation:  2 * time.Second,
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
			assert.Equal(t, test.wantParameters, cfg.Provider.QueryLog.IncludeParameters)
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

func TestBuildProviderAppliesQueryParameterLoggingToSQLite(t *testing.T) {
	var output bytes.Buffer
	log := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	provider, err := buildProvider(providerConfig{
		ConnectionString: filepath.Join(t.TempDir(), "provider.db"),
		QueryLog: queryLogConfig{
			durationLogConfig: durationLogConfig{
				Enabled: true,
			},
			IncludeParameters: true,
		},
	}, testProviderConfig(), log)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeErr := provider.Close()
		assert.NoError(t, closeErr)
	})

	err = provider.Init(t.Context())
	require.NoError(t, err)
	output.Reset()

	_, err = provider.GetState(t.Context(), ref.NewActorRef("query-type", "query-id"))
	require.ErrorIs(t, err, components.ErrNoState)
	logOutput := output.String()
	assert.Contains(t, logOutput, `db.query.text="SELECT actor_state_data FROM francis_actor_state WHERE actor_type = ? AND actor_id = ? AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?)"`)
	assert.Contains(t, logOutput, "db.query.parameter.1=query-type")
	assert.Contains(t, logOutput, "db.query.parameter.2=query-id")
	assert.Contains(t, logOutput, "code.file.path=sqlite-state.go")
	assert.Contains(t, logOutput, "code.line.number=")
}
