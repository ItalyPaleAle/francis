package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/components"
)

type PostgresProviderOptions struct {
	components.ProviderOptions

	// Connection string for the Postgres database
	// This allows the provider to establish a new database connection
	ConnectionString string

	// Connection to an existing database
	DB *pgxpool.Pool

	// Timeout for requests to the database
	Timeout time.Duration

	// Interval at which to perform garbage collection
	CleanupInterval time.Duration

	// Clock, used to pass a mock one for testing
	clock clock.WithTicker
}

// GetPgxPoolConfig parses the database connection string and returns the pgxpool.Config object
func (o PostgresProviderOptions) GetPgxPoolConfig() (*pgxpool.Config, error) {
	if o.ConnectionString == "" {
		return nil, errors.New("missing property ConnectionString in Postgres options")
	}

	cfg, err := pgxpool.ParseConfig(o.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("property ConnectionString in Postgres options is invalid: %w", err)
	}

	return cfg, nil
}
