package protocol

// This file defines the message DTOs for the Host -> Runtime traffic path
// Each host-initiated operation is paired with its response in this file
// Responses flow Runtime -> Host but belong to the host-initiated conversation
//
// All durations are expressed in milliseconds and all timestamps in Unix milliseconds so the wire format is unambiguous and language-neutral
// DTOs intentionally use explicit exported fields and never embed internal/ref types

// ActorRef references an actor by type and ID
type ActorRef struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
}

// AlarmRef references an alarm by actor type, actor ID, and alarm name
type AlarmRef struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
	Name      string `msgpack:"name"`
}

// AlarmProperties carries the mutable properties of an alarm
type AlarmProperties struct {
	// DueTimeUnixMs is the absolute due time in Unix milliseconds
	DueTimeUnixMs int64 `msgpack:"due"`
	// Interval is the repetition interval as an ISO8601-formatted duration string
	// Empty means non-repeating
	Interval string `msgpack:"interval,omitempty"`
	// TTLUnixMs is the absolute deadline for repeating alarms in Unix milliseconds
	// Zero means no deadline
	TTLUnixMs int64 `msgpack:"ttl,omitempty"`
	// Data is the opaque data associated with the alarm
	Data []byte `msgpack:"data,omitempty"`
}

// ActorHostType describes a supported actor type advertised by a host during registration
type ActorHostType struct {
	ActorType string `msgpack:"type"`
	// IdleTimeoutMs is the idle timeout in milliseconds
	// A negative value means no timeout
	IdleTimeoutMs int64 `msgpack:"idle,omitempty"`
	// ConcurrencyLimit is the maximum number of concurrent actors of this type
	// Zero means no limit
	ConcurrencyLimit int32 `msgpack:"limit,omitempty"`
	// DeactivationTimeoutMs is the deactivation timeout in milliseconds
	DeactivationTimeoutMs int64 `msgpack:"deact,omitempty"`
	// MaxAttempts is the maximum number of attempts when invoking the actor or executing alarms
	MaxAttempts int `msgpack:"maxAttempts,omitempty"`
	// InitialRetryDelayMs is the initial retry delay after a failed attempt, in milliseconds
	InitialRetryDelayMs int64 `msgpack:"retryDelay,omitempty"`
}

// RegisterHostRequest registers or reattaches a host with the runtime
type RegisterHostRequest struct {
	// ProtocolVersion is the highest protocol version the host supports
	ProtocolVersion uint16 `msgpack:"v"`
	// PreviousHostID is set on reconnect to reattach to an existing host registration by stable identity
	PreviousHostID string `msgpack:"prevHostId,omitempty"`
	// Address is the peer address where this host accepts host-to-host invocations
	Address string `msgpack:"address"`
	// ActorTypes is the set of actor types the host can run
	ActorTypes []ActorHostType `msgpack:"actorTypes,omitempty"`
}

// RegisterHostResponse is the runtime's response to RegisterHostRequest
type RegisterHostResponse struct {
	// HostID is the provider-backed stable host identity
	HostID string `msgpack:"hostId"`
	// SessionID identifies this runtime session and is echoed on subsequent messages so the runtime can detect a superseded session
	SessionID string `msgpack:"sessionId"`
	// ProtocolVersion is the negotiated protocol version both ends will use
	ProtocolVersion uint16 `msgpack:"v"`
	// HealthCheckIntervalMs is how often the host should send health checks, in milliseconds
	HealthCheckIntervalMs int64 `msgpack:"healthInterval"`
	// Reattached is true if the runtime reattached to an existing registration rather than creating a new one
	Reattached bool `msgpack:"reattached,omitempty"`
}

// UnregisterHostRequest requests a graceful, drain-oriented shutdown of the host
type UnregisterHostRequest struct {
	// DrainTimeoutMs is an optional hint for how long the host expects to take to drain, in milliseconds
	DrainTimeoutMs int64 `msgpack:"drain,omitempty"`
}

// UnregisterHostResponse acknowledges a graceful shutdown request
// After receiving it the host stops accepting new work and drains its active actors
type UnregisterHostResponse struct {
	// DrainDeadlineUnixMs is when the runtime expects the host to have finished draining, in Unix milliseconds
	DrainDeadlineUnixMs int64 `msgpack:"drainDeadline,omitempty"`
}

// HealthCheckRequest is a periodic health report from the host to the runtime
type HealthCheckRequest struct {
	// ActorTypes optionally replaces the set of supported actor types, when non-nil
	ActorTypes []ActorHostType `msgpack:"actorTypes,omitempty"`
}

// LookupActorRequest asks the runtime for the placement of an actor
type LookupActorRequest struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
	// ActiveOnly looks up only actors that are already active and does not allocate a new placement
	ActiveOnly bool `msgpack:"activeOnly,omitempty"`
	// SkipCache bypasses any runtime-side placement cache and forces a fresh provider lookup
	// Hosts set this when retrying after a stale-placement failure and on the receiving side of a peer invocation to authoritatively confirm ownership before activating an actor
	SkipCache bool `msgpack:"skipCache,omitempty"`
}

// LookupActorResponse carries the resolved placement for an actor
type LookupActorResponse struct {
	// HostID is the stable identity of the host that owns the actor
	HostID string `msgpack:"hostId"`
	// Address is the peer address of the owning host
	Address string `msgpack:"address"`
	// IdleTimeoutMs is the absolute idle timeout for the actor type, in milliseconds
	IdleTimeoutMs int64 `msgpack:"idle,omitempty"`
}

// RemoveActorRequest notifies the runtime that an actor has been deactivated on the sending host
// The host owns the actor lifecycle, so this clears the placement the runtime holds for the actor
type RemoveActorRequest struct {
	ActorRef
}

// GetAlarmRequest retrieves an alarm
type GetAlarmRequest struct {
	AlarmRef
}

// GetAlarmResponse carries the properties of a retrieved alarm
type GetAlarmResponse struct {
	AlarmProperties
}

// SetAlarmRequest creates or replaces an alarm
type SetAlarmRequest struct {
	AlarmRef
	AlarmProperties
}

// DeleteAlarmRequest deletes an alarm
type DeleteAlarmRequest struct {
	AlarmRef
}

// GetStateRequest retrieves the persistent state of an actor
type GetStateRequest struct {
	ActorRef
}

// GetStateResponse carries the persistent state of an actor
type GetStateResponse struct {
	Data []byte `msgpack:"data,omitempty"`
}

// SetStateRequest sets the persistent state of an actor
type SetStateRequest struct {
	ActorRef

	// Data is the MessagePack-encoded actor state
	Data []byte `msgpack:"data,omitempty"`
	// TTLMs is an optional time-to-live for the state, in milliseconds
	// Zero means no TTL
	TTLMs int64 `msgpack:"ttl,omitempty"`
}

// DeleteStateRequest deletes the persistent state of an actor
type DeleteStateRequest struct {
	ActorRef
}
