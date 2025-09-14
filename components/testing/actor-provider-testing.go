package comptesting

import (
	"context"
	"time"

	"github.com/italypaleale/actors/components"
)

// ActorProviderTesting extends the ActorProvider interface adding test-only methods
type ActorProviderTesting interface {
	components.ActorProvider

	// CleanupExpired performs garbage collection of expired records
	CleanupExpired() error

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
