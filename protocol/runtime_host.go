package protocol

// This file defines the message DTOs for the Runtime -> Host traffic path
// These are operations the runtime initiates against a connected host
// Each runtime-initiated operation is paired with its response in this file
//
// Alarm messages use explicit exported fields rather than internal/ref types
// The lease itself stays runtime-side: the runtime correlates the host's response to
// the in-flight ExecuteAlarmRequest by CorrelationID and keeps renewing the lease meanwhile

// ExecuteAlarmRequest asks a host to execute an alarm for an actor it owns and waits for the response
type ExecuteAlarmRequest struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
	Name      string `msgpack:"name"`
	// AlarmID is the provider-assigned identifier of the alarm occurrence
	AlarmID string `msgpack:"alarmId"`
	// DueTimeUnixMs is the due time of this occurrence, in Unix milliseconds
	DueTimeUnixMs int64 `msgpack:"due"`
	// Attempts is the number of prior execution attempts for this occurrence
	Attempts int `msgpack:"attempts,omitempty"`
	// Data is the opaque data associated with the alarm
	Data []byte `msgpack:"data,omitempty"`
}

// ExecuteAlarmResponse is the host's response to ExecuteAlarmRequest
type ExecuteAlarmResponse struct {
	// ExecutionTimeUnixMs is when the host executed the alarm, in Unix milliseconds
	ExecutionTimeUnixMs int64 `msgpack:"executed,omitempty"`
}

// TerminateActorRequest asks a host to terminate an active actor it owns
type TerminateActorRequest struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
}

// DisconnectRequest instructs a host to gracefully drain and reconnect to another runtime replica
type DisconnectRequest struct {
	// Reason is an optional human-readable reason for the disconnect
	Reason string `msgpack:"reason,omitempty"`
	// ReconnectAfterMs hints how long the host should wait before reconnecting, in milliseconds
	ReconnectAfterMs int64 `msgpack:"reconnectAfter,omitempty"`
}
