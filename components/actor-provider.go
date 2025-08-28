package components

import (
	"context"
	"time"
)

const (
	DefaultHostHealthCheckDeadline  = 20 * time.Second
	DefaultAlarmsLeaseDuration      = 20 * time.Second
	DefaultAlarmsFetchAheadInterval = 2500 * time.Millisecond
	DefaultAlarmsFetchAheadBatch    = 25
)

// ActorProvider is the interface implemented by all actor providers
type ActorProvider interface {
	// Init the actor provider
	Init(ctx context.Context) error

	// Run the actor provider
	// This method blocks until the context is canceled
	// If the provider is already running, returns ErrAlreadyRunning
	Run(ctx context.Context) error

	// RegisterHost registers a new actor host.
	// If a host already exists at the same address, returns ErrHostAlreadyRegistered.
	RegisterHost(ctx context.Context, req RegisterHostReq) (RegisterHostRes, error)

	// UpdateActorHost updates the properties for an actor host
	// If the host doesn't exist, returns ErrHostUnregistered.
	UpdateActorHost(ctx context.Context, hostID string, req UpdateActorHostReq) error

	// UnregisterHost unregisters an actor host.
	// If the host doesn't exist, returns ErrHostUnregistered.
	UnregisterHost(ctx context.Context, hostID string) error

	// LookupActor returns the address of the actor host for a given actor type and ID.
	// If the actor is not currently active on any host, a new actor is created and assigned to a random host; if it's not possible to find an instance capable of hosting the given actor, ErrNoHost is returned instead.
	LookupActor(ctx context.Context, ref ActorRef, opts LookupActorOpts) (LookupActorRes, error)

	// RemoveActor removes an actor from the collection of active actors.
	// If the actor doesn't exist, returns ErrNoActor.
	RemoveActor(ctx context.Context, ref ActorRef) error

	// GetAlarm returns an alarm.
	// It returns ErrNoAlarm if it doesn't exist.
	GetAlarm(ctx context.Context, ref AlarmRef) (GetAlarmRes, error)

	// SetAlarm sets or replaces an alarm configured for an actor.
	SetAlarm(ctx context.Context, ref AlarmRef, req SetAlarmReq) error

	// DeleteAlarm removes an alarm configured for an actor.
	// If the alarm doesn't exist, returns ErrNoAlarm.
	DeleteAlarm(ctx context.Context, ref AlarmRef) error

	// GetState retrieves the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	GetState(ctx context.Context, ref ActorRef) ([]byte, error)

	// SetState sets the persistent state of an actor.
	SetState(ctx context.Context, ref ActorRef, data []byte, opts SetStateOpts) error

	// DeleteState deletes the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	DeleteState(ctx context.Context, ref ActorRef) error

	// HealthCheckInterval returns the recommended health check interval for hosts.
	HealthCheckInterval() time.Duration
}

// ProviderOptions is an empty interface implemented by all options structs for providers
type ProviderOptions interface{}

// ProviderConfig contains the configuration for the actor provider
type ProviderConfig struct {
	// Maximum interval between pings received from an actor host.
	HostHealthCheckDeadline time.Duration

	// Alarms lease duration
	AlarmsLeaseDuration time.Duration

	// Pre-fetch interval for alarms
	AlarmsFetchAheadInterval time.Duration

	// Batch size for pre-fetching alarms
	AlarmsFetchAheadBatchSize int
}

func (o *ProviderConfig) SetDefaults() {
	if o.HostHealthCheckDeadline < time.Second {
		o.HostHealthCheckDeadline = DefaultHostHealthCheckDeadline
	}
	if o.AlarmsLeaseDuration < time.Second {
		o.AlarmsLeaseDuration = DefaultAlarmsLeaseDuration
	}
	if o.AlarmsFetchAheadInterval < 100*time.Millisecond {
		o.AlarmsFetchAheadInterval = DefaultAlarmsFetchAheadInterval
	}
	if o.AlarmsFetchAheadBatchSize <= 0 {
		o.AlarmsFetchAheadBatchSize = DefaultAlarmsFetchAheadBatch
	}
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
	// Idle timeout for the actor type
	IdleTimeout time.Duration
	// Maximum number of actors of the given type active on each host
	// Set to 0 for no limit
	ConcurrencyLimit int32
	// Actor deactivation timeout
	DeactivationTimeout time.Duration
}

// ActorRef references an actor (type and ID).
type ActorRef struct {
	ActorType string
	ActorID   string
}

// String implements fmt.Stringer.
func (r ActorRef) String() string {
	return r.ActorType + "/" + r.ActorID
}

// AlarmRef references an alarm.
type AlarmRef struct {
	ActorType string
	ActorID   string
	Name      string
}

// ActorRef returns the actor reference for the alarm.
func (r AlarmRef) ActorRef() ActorRef {
	return ActorRef{
		ActorType: r.ActorType,
		ActorID:   r.ActorID,
	}
}

// String implements fmt.Stringer.
func (r AlarmRef) String() string {
	return r.ActorType + "/" + r.ActorID + "/" + r.Name
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
	// Actor idle timeout
	// Note: this is the absolute idle timeout, and not the remaining lifetime of the actor
	IdleTimeout time.Duration
}

// Properties for an alarm
type AlarmProperties struct {
	// Due time.
	DueTime time.Time
	// Alarm repetition interval.
	Interval *time.Duration
	// Deadline for repeating alarms.
	TTL *time.Time
	// Data associated with the alarm.
	Data []byte
}

// GetAlarmRes is the response object for the GetAlarm method.
type GetAlarmRes struct {
	AlarmProperties
}

// SetAlarmReq is the request object for the SetAlarm method.
type SetAlarmReq struct {
	AlarmProperties
}

// SetStateOpts contains options for SetState
type SetStateOpts struct {
	TTL time.Duration
}
