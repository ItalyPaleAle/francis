package comptesting

import (
	"context"
	"time"

	"github.com/italypaleale/francis/components"
)

// ActorProviderTesting extends the ManagementProvider interface, which every provider in this module implements, adding test-only methods
type ActorProviderTesting interface {
	components.ManagementProvider

	// CleanupExpired performs garbage collection of expired records
	CleanupExpired(ctx context.Context) error

	// Seed seeds the data into the database
	Seed(ctx context.Context, spec Spec) error

	// Now returns the current time
	// Providers that do not have a mocked clock should respond with time.Now()
	Now() time.Time

	// AdvanceClock advances the clock
	// Providers that do not have a mocked clock should sleep for the given duration
	AdvanceClock(d time.Duration) error

	// GetAllActorState returns all stored actor state
	GetAllActorState(ctx context.Context) (ActorStateSpecCollection, error)

	// GetAllHosts returns all stored hosts, host actor types, active actors, and alarms
	GetAllHosts(ctx context.Context) (Spec, error)
}
