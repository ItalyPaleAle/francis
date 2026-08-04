//go:build integration

// Package observability verifies provider telemetry through real provider construction paths
package observability

import (
	"bytes"
	"context"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/providerfactory"
	"github.com/italypaleale/francis/tests/integration/framework"
	frameworkprovider "github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	integrationtelemetry "github.com/italypaleale/francis/tests/integration/telemetry"
)

const postgresConnstringEnvVar = "TEST_POSTGRES_CONNSTRING"

var variants = []frameworkprovider.Variant{
	frameworkprovider.StandaloneMemory,
	frameworkprovider.SQLite,
	frameworkprovider.Postgres,
}

func init() {
	for _, variant := range variants {
		suite.Register(&providerTelemetry{variant: variant})
	}
}

type providerTelemetry struct {
	variant frameworkprovider.Variant
}

func (s *providerTelemetry) Name() string {
	return "observability/provider/" + string(s.variant)
}

func (s *providerTelemetry) Setup(*testing.T) []framework.Option {
	return nil
}

func (s *providerTelemetry) Run(t *testing.T) {
	// Enable span collection only for this scenario so the broader integration suite keeps tracing overhead negligible
	spanOffset := integrationtelemetry.StartCapture()
	defer integrationtelemetry.StopCapture()

	// Capture Debug logs so both operation and statement logging are observable
	var output bytes.Buffer
	log := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Build through the shared factory so the integration covers the production provider wrapper
	opts := s.providerOptions(t)
	provider, err := providerfactory.New(log, opts, components.NewProviderConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		closeErr := provider.Close()
		assert.NoError(t, closeErr)
	})

	// Initialize the real provider to execute a representative provider operation and any underlying SQL
	err = provider.Init(t.Context())
	require.NoError(t, err)

	// Every provider must emit operation telemetry through the common decorator
	spanNames := integrationtelemetry.EndedSpanNames(spanOffset)
	assert.True(t, spanNames["provider.Init"], "expected a provider.Init span")
	assert.Contains(t, output.String(), "Executed provider operation")

	// SQL-backed providers must also emit their backend-specific statement telemetry
	switch s.variant {
	case frameworkprovider.SQLite:
		assert.True(t, hasSpanPrefix(spanNames, "sqlite."), "expected a SQLite statement span")
		assert.Contains(t, output.String(), "Executed SQL statement")
	case frameworkprovider.Postgres:
		assert.True(t, hasSpanPrefix(spanNames, "postgresql."), "expected a PostgreSQL statement span")
		assert.Contains(t, output.String(), "Executed SQL statement")
	case frameworkprovider.StandaloneMemory:
		assert.False(t, hasSpanPrefix(spanNames, "sqlite."))
		assert.False(t, hasSpanPrefix(spanNames, "postgresql."))
	}
}

func (s *providerTelemetry) providerOptions(t *testing.T) components.ProviderOptions {
	switch s.variant {
	case frameworkprovider.StandaloneMemory:
		return standalone.StandaloneMemoryOptions{
			CleanupInterval: -1,
			OperationLog: components.OperationLogConfig{
				Enabled: true,
			},
		}
	case frameworkprovider.SQLite:
		return sqlite.SQLiteProviderOptions{
			ConnectionString: "file:" + filepath.Join(t.TempDir(), "telemetry.db"),
			CleanupInterval:  -1,
			QueryLog: components.QueryLogConfig{
				Enabled: true,
			},
			OperationLog: components.OperationLogConfig{
				Enabled: true,
			},
		}
	case frameworkprovider.Postgres:
		return postgresOptions(t)
	default:
		require.FailNow(t, "unsupported observability provider", "variant: %s", s.variant)
		return nil
	}
}

func postgresOptions(t *testing.T) postgres.PostgresProviderOptions {
	// Skip consistently with the existing Postgres integration cases when no database is configured
	connString := strings.TrimSpace(os.Getenv(postgresConnstringEnvVar))
	if connString == "" {
		t.Skipf("set %s to run this scenario", postgresConnstringEnvVar)
	}

	// Isolate migrations and queries in a schema owned by this test
	schema := "test_observability_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin, err := pgxpool.New(t.Context(), connString)
	require.NoError(t, err)
	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	_, err = admin.Exec(t.Context(), "CREATE SCHEMA "+schemaIdentifier)
	require.NoError(t, err)
	t.Cleanup(func() {
		// The provider cleanup runs first, so no instrumented connection remains when the schema is dropped
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, cleanupErr := admin.Exec(ctx, "DROP SCHEMA "+schemaIdentifier+" CASCADE")
		assert.NoError(t, cleanupErr)
		admin.Close()
	})

	return postgres.PostgresProviderOptions{
		ConnectionString: connectionStringWithSearchPath(t, connString, schema),
		CleanupInterval:  -1,
		QueryLog: components.QueryLogConfig{
			Enabled: true,
		},
		OperationLog: components.OperationLogConfig{
			Enabled: true,
		},
	}
}

func connectionStringWithSearchPath(t *testing.T, connString string, schema string) string {
	connStringLC := strings.ToLower(connString)
	if strings.HasPrefix(connStringLC, "postgres://") || strings.HasPrefix(connStringLC, "postgresql://") {
		u, err := url.Parse(connString)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		return u.String()
	}

	return connString + " search_path=" + schema
}

func hasSpanPrefix(names map[string]bool, prefix string) bool {
	for name := range names {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
