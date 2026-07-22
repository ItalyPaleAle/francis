// Package providerfactory builds an ActorProvider from a set of provider options and configuration
// It is shared by the local host and the ClusterAdmin so both construct providers the same way, from the same set of supported provider option types
package providerfactory

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
)

// New constructs an ActorProvider from the given provider options and configuration
func New(logger *slog.Logger, opts components.ProviderOptions, cfg components.ProviderConfig) (components.ActorProvider, error) {
	switch x := opts.(type) {
	case sqlite.SQLiteProviderOptions:
		p, err := sqlite.NewSQLiteProvider(logger, x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
		return p, nil
	case *sqlite.SQLiteProviderOptions:
		p, err := sqlite.NewSQLiteProvider(logger, *x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
		return p, nil
	case postgres.PostgresProviderOptions:
		p, err := postgres.NewPostgresProvider(logger, x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create Postgres provider: %w", err)
		}
		return p, nil
	case *postgres.PostgresProviderOptions:
		p, err := postgres.NewPostgresProvider(logger, *x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create Postgres provider: %w", err)
		}
		return p, nil
	case standalone.StandaloneMemoryOptions:
		p, err := standalone.NewStandaloneMemory(logger, x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone memory provider: %w", err)
		}
		return p, nil
	case standalone.StandaloneSQLiteOptions:
		p, err := standalone.NewStandaloneSQLiteBacked(logger, x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone SQLite provider: %w", err)
		}
		return p, nil
	case standalone.StandalonePostgresOptions:
		p, err := standalone.NewStandalonePostgresBacked(logger, x, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone Postgres provider: %w", err)
		}
		return p, nil
	case nil:
		return nil, errors.New("option ProviderOptions is required")
	default:
		return nil, fmt.Errorf("unsupported value for ProviderOptions: %T", opts)
	}
}
