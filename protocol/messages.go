// Package protocol defines the message types for communication between Francis clients and the Francis service.
// All messages are encoded using MessagePack.
package protocol

import (
	"io"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/internal/ref"
)

// Message type identifiers
const (
	// Client -> Francis requests
	MsgTypeRegisterClient         = "register_client"
	MsgTypeUnregisterClient       = "unregister_client"
	MsgTypeHealthCheck            = "health_check"
	MsgTypeLookupActor            = "lookup_actor"
	MsgTypeGetAlarm               = "get_alarm"
	MsgTypeSetAlarm               = "set_alarm"
	MsgTypeDeleteAlarm            = "delete_alarm"
	MsgTypeGetState               = "get_state"
	MsgTypeSetState               = "set_state"
	MsgTypeDeleteState            = "delete_state"
	MsgTypeAlarmCompleted         = "alarm_completed"
	MsgTypeActorTerminateComplete = "actor_terminate_complete"

	// Francis -> Client notifications
	MsgTypeExecuteAlarm   = "execute_alarm"
	MsgTypeTerminateActor = "terminate_actor"
)

// Error codes
const (
	ErrCodeSuccess             = ""
	ErrCodeInvalidMessage      = "invalid_message"
	ErrCodeInternalError       = "internal_error"
	ErrCodeClientNotRegistered = "client_not_registered"
	ErrCodeActorNotFound       = "actor_not_found"
	ErrCodeNoHostAvailable     = "no_host_available"
	ErrCodeAlarmNotFound       = "alarm_not_found"
	ErrCodeStateNotFound       = "state_not_found"
	ErrCodeActorTypeNotFound   = "actor_type_not_found"
	ErrCodeHostUnregistered    = "host_unregistered"
	ErrCodeInvalidRequest      = "invalid_request"
)

// Message is the base wrapper for all protocol messages
type Message struct {
	// Type identifies the message type
	Type string `msgpack:"type"`
	// ID is a unique identifier for request-response correlation
	ID string `msgpack:"id,omitempty"`
	// Payload is the message-specific data
	Payload []byte `msgpack:"payload,omitempty"`
	// Error is set for response messages when an error occurred
	Error *ErrorPayload `msgpack:"error,omitempty"`
}

// ErrorPayload contains error information
type ErrorPayload struct {
	Code    string `msgpack:"code"`
	Message string `msgpack:"message,omitempty"`
}

// Encode serializes the message to msgpack
func (m *Message) Encode(w io.Writer) error {
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(w)
	return enc.Encode(m)
}

// DecodeMessage deserializes a message from msgpack
func DecodeMessage(r io.Reader) (*Message, error) {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.Reset(r)

	var msg Message
	err := dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// EncodePayload serializes a payload struct to msgpack bytes
func EncodePayload(payload any) ([]byte, error) {
	return msgpack.Marshal(payload)
}

// DecodePayload deserializes a payload from msgpack bytes
func DecodePayload[T any](data []byte) (*T, error) {
	var v T
	err := msgpack.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Client -> Francis Messages

// RegisterClientReq is sent by clients to register with Francis
type RegisterClientReq struct {
	// Address where the client can be reached for peer-to-peer communication
	Address string `msgpack:"address"`
	// List of actor types this client can host
	ActorTypes []ActorTypeRegistration `msgpack:"actor_types"`
}

// ActorTypeRegistration contains registration info for an actor type
type ActorTypeRegistration struct {
	// Actor type name
	ActorType string `msgpack:"actor_type"`
	// Idle timeout for actors of this type
	IdleTimeout time.Duration `msgpack:"idle_timeout"`
	// Maximum concurrent actors of this type (0 = unlimited)
	ConcurrencyLimit int32 `msgpack:"concurrency_limit"`
	// Timeout for deactivation calls
	DeactivationTimeout time.Duration `msgpack:"deactivation_timeout"`
	// Maximum retry attempts for alarms
	MaxAttempts int `msgpack:"max_attempts"`
	// Initial retry delay for failed alarm executions
	InitialRetryDelay time.Duration `msgpack:"initial_retry_delay"`
}

// RegisterClientRes is the response to RegisterClientReq
type RegisterClientRes struct {
	// Assigned client ID (also used as host ID)
	ClientID string `msgpack:"client_id"`
}

// UnregisterClientReq is sent by clients to unregister from Francis
type UnregisterClientReq struct {
	ClientID string `msgpack:"client_id"`
}

// UnregisterClientRes is the response to UnregisterClientReq
type UnregisterClientRes struct{}

// HealthCheckReq is sent periodically by clients
type HealthCheckReq struct {
	ClientID string `msgpack:"client_id"`
}

// HealthCheckRes is the response to HealthCheckReq
type HealthCheckRes struct{}

// LookupActorReq requests the location of an actor
type LookupActorReq struct {
	ActorType  string `msgpack:"actor_type"`
	ActorID    string `msgpack:"actor_id"`
	ActiveOnly bool   `msgpack:"active_only,omitempty"`
}

// LookupActorRes contains the actor location
type LookupActorRes struct {
	// Host ID where the actor is located
	HostID string `msgpack:"host_id"`
	// Address of the host (for peer-to-peer communication)
	Address string `msgpack:"address"`
	// Idle timeout for the actor
	IdleTimeout time.Duration `msgpack:"idle_timeout"`
}

// GetAlarmReq requests alarm details
type GetAlarmReq struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	AlarmName string `msgpack:"alarm_name"`
}

// GetAlarmRes contains the alarm details
type GetAlarmRes struct {
	DueTime  time.Time `msgpack:"due_time"`
	Interval string    `msgpack:"interval,omitempty"`
	TTL      time.Time `msgpack:"ttl,omitempty"`
	Data     []byte    `msgpack:"data,omitempty"`
}

// SetAlarmReq creates or updates an alarm
type SetAlarmReq struct {
	ActorType string    `msgpack:"actor_type"`
	ActorID   string    `msgpack:"actor_id"`
	AlarmName string    `msgpack:"alarm_name"`
	DueTime   time.Time `msgpack:"due_time"`
	Interval  string    `msgpack:"interval,omitempty"`
	TTL       time.Time `msgpack:"ttl,omitempty"`
	Data      []byte    `msgpack:"data,omitempty"`
}

// SetAlarmRes is the response to SetAlarmReq
type SetAlarmRes struct{}

// DeleteAlarmReq removes an alarm
type DeleteAlarmReq struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	AlarmName string `msgpack:"alarm_name"`
}

// DeleteAlarmRes is the response to DeleteAlarmReq
type DeleteAlarmRes struct{}

// GetStateReq requests actor state
type GetStateReq struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
}

// GetStateRes contains the actor state
type GetStateRes struct {
	// State data (msgpack-encoded)
	Data []byte `msgpack:"data"`
}

// SetStateReq saves actor state
type SetStateReq struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	// State data (msgpack-encoded)
	Data []byte `msgpack:"data"`
	// Optional TTL
	TTL time.Duration `msgpack:"ttl,omitempty"`
}

// SetStateRes is the response to SetStateReq
type SetStateRes struct{}

// DeleteStateReq removes actor state
type DeleteStateReq struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
}

// DeleteStateRes is the response to DeleteStateReq
type DeleteStateRes struct{}

// AlarmCompletedReq is sent by clients after executing an alarm
type AlarmCompletedReq struct {
	ClientID string `msgpack:"client_id"`
	// Alarm reference
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	AlarmName string `msgpack:"alarm_name"`
	// Lease ID for the alarm
	LeaseID string `msgpack:"lease_id"`
	// Status of execution
	Status AlarmExecutionStatus `msgpack:"status"`
	// Error message if failed
	ErrorMessage string `msgpack:"error_message,omitempty"`
	// Execution time
	ExecutionTime time.Time `msgpack:"execution_time"`
	// Attempt number
	Attempt int `msgpack:"attempt"`
}

// AlarmExecutionStatus indicates the result of alarm execution
type AlarmExecutionStatus string

const (
	AlarmStatusCompleted AlarmExecutionStatus = "completed"
	AlarmStatusFailed    AlarmExecutionStatus = "failed"
	AlarmStatusRetryable AlarmExecutionStatus = "retryable"
	AlarmStatusAbandoned AlarmExecutionStatus = "abandoned"
)

// AlarmCompletedRes is the response to AlarmCompletedReq
type AlarmCompletedRes struct {
	// NextDueTime is set if the alarm should be re-queued (for retries or repeating alarms)
	NextDueTime *time.Time `msgpack:"next_due_time,omitempty"`
	// KeepLease indicates whether to keep the lease for immediate re-execution
	KeepLease bool `msgpack:"keep_lease,omitempty"`
}

// ActorTerminateCompleteReq is sent by clients after terminating an actor
type ActorTerminateCompleteReq struct {
	ClientID  string `msgpack:"client_id"`
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	Success   bool   `msgpack:"success"`
	Error     string `msgpack:"error,omitempty"`
}

// ActorTerminateCompleteRes is the response to ActorTerminateCompleteReq
type ActorTerminateCompleteRes struct{}

// Francis -> Client Notifications

// ExecuteAlarmNotification tells a client to execute an alarm
type ExecuteAlarmNotification struct {
	// Alarm reference
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	AlarmName string `msgpack:"alarm_name"`
	// Lease information
	LeaseID string    `msgpack:"lease_id"`
	DueTime time.Time `msgpack:"due_time"`
	// Alarm data (msgpack-encoded)
	Data []byte `msgpack:"data,omitempty"`
	// Attempt number (1-based)
	Attempt int `msgpack:"attempt"`
}

// TerminateActorNotification tells a client to terminate an actor
type TerminateActorNotification struct {
	ActorType string `msgpack:"actor_type"`
	ActorID   string `msgpack:"actor_id"`
	// Reason for termination
	Reason string `msgpack:"reason,omitempty"`
}

// Helper functions to create ref types from protocol messages

// ToActorRef converts lookup request to ActorRef
func (r *LookupActorReq) ToActorRef() ref.ActorRef {
	return ref.NewActorRef(r.ActorType, r.ActorID)
}

// ToAlarmRef converts alarm request to AlarmRef
func (r *GetAlarmReq) ToAlarmRef() ref.AlarmRef {
	return ref.NewAlarmRef(r.ActorType, r.ActorID, r.AlarmName)
}

// ToAlarmRef converts set alarm request to AlarmRef
func (r *SetAlarmReq) ToAlarmRef() ref.AlarmRef {
	return ref.NewAlarmRef(r.ActorType, r.ActorID, r.AlarmName)
}

// ToAlarmRef converts delete alarm request to AlarmRef
func (r *DeleteAlarmReq) ToAlarmRef() ref.AlarmRef {
	return ref.NewAlarmRef(r.ActorType, r.ActorID, r.AlarmName)
}

// ToActorRef converts state request to ActorRef
func (r *GetStateReq) ToActorRef() ref.ActorRef {
	return ref.NewActorRef(r.ActorType, r.ActorID)
}

// ToActorRef converts set state request to ActorRef
func (r *SetStateReq) ToActorRef() ref.ActorRef {
	return ref.NewActorRef(r.ActorType, r.ActorID)
}

// ToActorRef converts delete state request to ActorRef
func (r *DeleteStateReq) ToActorRef() ref.ActorRef {
	return ref.NewActorRef(r.ActorType, r.ActorID)
}
