//go:build integration

package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host/local"
)

// Env vars holding the Postgres connection string, matching the existing unit tests
const (
	postgresConnstringEnvVar           = "TEST_POSTGRES_CONNSTRING"
	standalonePostgresConnstringEnvVar = "TEST_STANDALONE_POSTGRES_CONNSTRING"
)

// postgresBackend is a backend backed by a real Postgres database
// Each run uses an isolated, randomly-named schema that is dropped on cleanup, and a single shared pool is handed to every consumer so they all operate within that schema
// When standalone is true it drives the standalone (in-memory, Postgres-backed) provider instead of the multi-instance Postgres provider
type postgresBackend struct {
	standalone bool

	schema string
	pool   *pgxpool.Pool
}

func (b *postgresBackend) Variant() Variant {
	if b.standalone {
		return StandalonePostgres
	}
	return Postgres
}

func (b *postgresBackend) envVar() string {
	if b.standalone {
		return standalonePostgresConnstringEnvVar
	}
	return postgresConnstringEnvVar
}

func (b *postgresBackend) Run(t *testing.T) {
	t.Helper()

	// Skip rather than fail when no database is configured, matching the unit tests
	connString := os.Getenv(b.envVar())
	if connString == "" {
		t.Skipf("set %s to run this scenario, e.g. %s=postgres://actors:actors@localhost:5432/actors", b.envVar(), b.envVar())
	}

	b.schema = generateSchemaName(t)
	t.Logf("Postgres test schema: %s", b.schema)

	cfg, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err, "invalid Postgres connection string")

	// Ensure every pooled connection lands in the per-run schema
	schema := b.schema
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := c.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`)
		if err != nil {
			return fmt.Errorf("failed to ensure test schema %q exists: %w", schema, err)
		}

		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = c.Exec(ctx, `SET SESSION search_path = "`+schema+`", pg_catalog, public`)
		if err != nil {
			return fmt.Errorf("failed to set search path for session: %w", err)
		}

		return nil
	}

	b.pool, err = pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err, "failed to connect to Postgres")
}

func (b *postgresBackend) LocalHostOption(t *testing.T) local.HostOption {
	t.Helper()
	require.NotNil(t, b.pool, "Postgres backend used before Run")

	if b.standalone {
		return local.WithStandalonePostgresProvider(standalone.StandalonePostgresOptions{
			DB: b.pool,
		})
	}

	return local.WithPostgresProvider(postgres.PostgresProviderOptions{
		DB:              b.pool,
		CleanupInterval: -1,
	})
}

func (b *postgresBackend) NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider {
	t.Helper()
	require.NotNil(t, b.pool, "Postgres backend used before Run")

	if b.standalone {
		p, err := standalone.NewStandalonePostgresBacked(log, standalone.StandalonePostgresOptions{
			DB: b.pool,
		}, providerConfig())
		require.NoError(t, err, "failed to create standalone Postgres provider")
		return p
	}

	p, err := postgres.NewPostgresProvider(log, postgres.PostgresProviderOptions{
		DB:              b.pool,
		CleanupInterval: -1,
	}, providerConfig())
	require.NoError(t, err, "failed to create Postgres provider")

	return p
}

func (b *postgresBackend) Cleanup(t *testing.T) {
	t.Helper()
	if b.pool == nil {
		return
	}

	// The test context is likely already canceled
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
	defer cancel()
	_, err := b.pool.Exec(ctx, `DROP SCHEMA "`+b.schema+`" CASCADE`)
	require.NoErrorf(t, err, "Failed to drop test schema '%s'", b.schema)

	b.pool.Close()
	b.pool = nil
}

// generateSchemaName returns a random, collision-resistant schema name for one test run
func generateSchemaName(t *testing.T) string {
	t.Helper()

	buf := make([]byte, 5)
	_, err := io.ReadFull(rand.Reader, buf)
	require.NoError(t, err)

	return "test_it_" + hex.EncodeToString(buf)
}
