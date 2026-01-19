package standalone

import (
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
)

// StandaloneMemory is a pure in-memory provider with no persistence.
// All data is lost when the process exits.
type StandaloneMemory struct {
	*provider
}

// StandaloneMemoryOptions contains options for creating a StandaloneMemory provider.
type StandaloneMemoryOptions struct {
	components.ProviderOptions

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration
}

// NewStandaloneMemory creates a new pure in-memory ActorProvider.
func NewStandaloneMemory(log *slog.Logger, opts StandaloneMemoryOptions, providerConfig components.ProviderConfig) (*StandaloneMemory, error) {
	p, err := newProvider(log, providerOptions{
		ProviderOptions: opts.ProviderOptions,
		Clock:           opts.Clock,
		CleanupInterval: opts.CleanupInterval,
		PersistHook:     &noopPersistHook{},
	}, providerConfig)
	if err != nil {
		return nil, err
	}

	return &StandaloneMemory{provider: p}, nil
}
