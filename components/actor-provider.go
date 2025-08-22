package components

import (
	"context"
	"time"
)

// ActorProvider is the interface implemented by all actor providers
type ActorProvider interface {
	// Run the actor provider
	// This method blocks until the context is canceled
	// If the provider is already running, returns ErrAlreadyRunning
	Run(ctx context.Context) error

	// RegisterHost registers a new actor host.
	// If a host already exists at the same address, returns ErrHostAlreadyRegistered.
	RegisterHost(ctx context.Context, req RegisterHostReq) (RegisterHostRes, error)

	// UpdateActorHost updates the properties for an actor host
	// If the host doesn't exist, returns ErrHostUnregistered.
	UpdateActorHost(ctx context.Context, actorHostID string, req UpdateActorHostReq) error

	// UnregisterHost unregisters an actor host.
	// If the host doesn't exist, returns ErrHostUnregistered.
	UnregisterHost(ctx context.Context, actorHostID string) error

	// LookupActor returns the address of the actor host for a given actor type and ID.
	// If the actor is not currently active on any host, a new actor is created and assigned to a random host; if it's not possible to find an instance capable of hosting the given actor, ErrNoHost is returned instead.
	LookupActor(ctx context.Context, ref ActorRef, opts LookupActorOpts) (LookupActorRes, error)

	// RemoveActor removes an actor from the collection of active actors.
	// If the actor doesn't exist, returns ErrNoActor.
	RemoveActor(ctx context.Context, ref ActorRef) error

	// SetAlarm sets or replaces an alarm configured for an actor.
	SetAlarm(ctx context.Context, ref ActorRef, name string, req SetAlarmReq) error

	// DeleteAlarm removes an alarm configured for an actor.
	// If the alarm doesn't exist, returns ErrNoAlarm.
	DeleteAlarm(ctx context.Context, ref ActorRef, name string) error

	// GetState retrieves the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	GetState(ctx context.Context, ref ActorRef) ([]byte, error)

	// SetState sets the persistent state of an actor.
	SetState(ctx context.Context, ref ActorRef, data []byte) error

	// DeleteState deletes the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	DeleteState(ctx context.Context, ref ActorRef) error
}

// ProviderOptions contains the configuration for the actor provider
type ProviderOptions struct {
	// Maximum interval between pings received from an actor host.
	HostHealthCheck time.Duration

	// Alarms lease duration
	AlarmsLeaseDuration time.Duration

	// Pre-fetch interval for alarms
	AlarmsFetchAheadInterval time.Duration

	// Batch size for pre-fetching alarms
	AlarmsFetchAheadBatch int
}

// RegisterHostReq is the request object for the RegisterHost method.
type RegisterHostReq struct {
	// Host address, where
	Address string
	// List of supported actor types
	ActorTypes []ActorHostType
}

// RegisterHostRes is the response object for the RegisterHost method.
type RegisterHostRes struct {
	// Auto-generated ID of the actor host
	HostID string
}

// UpdateActorHostReq is the request object for the UpdateActorHost method.
type UpdateActorHostReq struct {
	// Updates last health check time
	// If true, will update the value in the database with the current time
	UpdateLastHealthCheck bool

	// List of supported actor types
	// If non-nil, will replace all existing, registered actor types (an empty, non-nil slice indicates no supported actor types)
	ActorTypes []ActorHostType
}

// ActorHostType references a supported actor type.
type ActorHostType struct {
	// Actor type
	ActorType string
	// Idle timeout for the actor type, in seconds
	IdleTimeout int32
	// Maximum number of actors of the given type active on each host
	// Set to 0 for no limit
	ConcurrencyLimit int32
	// Maximum number of alarms of the given type active on each host
	// This cannot be bigger than ConcurrencyLimit
	// Set to 0 for no limit
	AlarmConcurrencyLimit int32
}

// ActorRef references an actor (type and ID).
type ActorRef struct {
	ActorType string
	ActorID   string
}

// LookupActorOpts contains options for LookupActor.
type LookupActorOpts struct {
	// List of hosts on which the actor can be activated.
	// If the actor is active on a different host, ErrNoActorHost is returned.
	Hosts []string
}

// LookupActorRes is the response object for the LookupActor method.
type LookupActorRes struct {
	// Host ID
	HostID string
	// Host address (including port)
	Address string
	// Actor idle timeout, in seconds
	// Note: this is the absolute idle timeout, and not the remaining lifetime of the actor
	IdleTimeout int32
}

// SetAlarmReq is the request object for the SetAlarm method.
type SetAlarmReq struct {
	Data    []byte
	DueTime time.Time
	Period  time.Duration
	TTL     time.Time
}
