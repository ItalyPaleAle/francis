package protocol

import (
	"errors"
	"fmt"
	"maps"
	"time"
)

// ErrorCode is a stable, machine-readable protocol error code
// Codes are part of the wire contract and must not be renamed once shipped
type ErrorCode string

const (
	// ErrCodeInternal is an unexpected internal error
	ErrCodeInternal ErrorCode = "internal"
	// ErrCodeBadRequest indicates the request was malformed or invalid
	ErrCodeBadRequest ErrorCode = "bad_request"
	// ErrCodeUnauthorized indicates the request failed authentication or authorization
	ErrCodeUnauthorized ErrorCode = "unauthorized"
	// ErrCodeProtocolVersion indicates the peer's protocol version is unsupported
	ErrCodeProtocolVersion ErrorCode = "protocol_version"
	// ErrCodeDeadlineExceeded indicates the operation deadline elapsed
	ErrCodeDeadlineExceeded ErrorCode = "deadline_exceeded"
	// ErrCodeCanceled indicates the operation was canceled
	ErrCodeCanceled ErrorCode = "canceled"
	// ErrCodeSessionSuperseded indicates the session was superseded by a newer one for the same host
	ErrCodeSessionSuperseded ErrorCode = "session_superseded"
	// ErrCodeHostUnregistered indicates the host registration is no longer valid
	ErrCodeHostUnregistered ErrorCode = "host_unregistered"
	// ErrCodeHostDraining indicates the target host is draining and cannot accept new work, retry after a delay
	ErrCodeHostDraining ErrorCode = "host_draining"
	// ErrCodeHostMismatch indicates the request reached the wrong host, likely due to stale placement
	ErrCodeHostMismatch ErrorCode = "host_mismatch"
	// ErrCodeRetryLater indicates the operation cannot complete now and the caller should retry after a delay
	ErrCodeRetryLater ErrorCode = "retry_later"
	// ErrCodeOverloaded indicates the target has too many in-flight requests and the caller should retry after a delay
	ErrCodeOverloaded ErrorCode = "overloaded"
	// ErrCodeNoHost indicates no host is available to place the actor
	ErrCodeNoHost ErrorCode = "no_host"
	// ErrCodeActorNotHosted indicates the actor is not active on the target host
	ErrCodeActorNotHosted ErrorCode = "actor_not_hosted"
	// ErrCodeActorNotActive indicates an active-only lookup found no active actor, and is not retryable
	ErrCodeActorNotActive ErrorCode = "actor_not_active"
	// ErrCodeActorHalted indicates the actor is being halted, retry after a delay
	ErrCodeActorHalted ErrorCode = "actor_halted"
	// ErrCodeActorTypeUnsupported indicates the actor type is not supported in the cluster
	ErrCodeActorTypeUnsupported ErrorCode = "actor_type_unsupported"
	// ErrCodeInvokeModeUnsupported indicates the actor type does not support the requested invocation mode
	ErrCodeInvokeModeUnsupported ErrorCode = "invoke_mode_unsupported"
	// ErrCodeInvokeFailed indicates the actor method invocation returned an error
	ErrCodeInvokeFailed ErrorCode = "invoke_failed"
	// ErrCodeStateNotFound indicates the requested actor state does not exist
	ErrCodeStateNotFound ErrorCode = "state_not_found"
	// ErrCodeAlarmNotFound indicates the requested alarm does not exist
	ErrCodeAlarmNotFound ErrorCode = "alarm_not_found"
	// ErrCodeTransportFailure indicates the request was delivered to the peer but the response was not received
	// The peer actor may have executed the request, so the caller must not auto-retry without an idempotency mechanism to avoid double-execution
	ErrCodeTransportFailure ErrorCode = "transport_failure"
)

// MetadataRetryAfterMs is a metadata key carrying a retry-after hint in milliseconds
// It mirrors the RetryAfterMs field and exists for callers that inspect metadata generically
const MetadataRetryAfterMs = "retryAfterMs"

// Error is a structured protocol error that can be serialized across the wire
type Error struct {
	// Code is the stable machine-readable error code
	Code ErrorCode `msgpack:"code"`
	// Message is an optional human-readable description
	Message string `msgpack:"msg,omitempty"`
	// RetryAfterMs hints how long the caller should wait before retrying, in milliseconds
	// Only meaningful for retryable errors such as retry_later, host_draining, and actor_halted
	RetryAfterMs int64 `msgpack:"retry,omitempty"`
	// Metadata carries optional structured detail about the error
	Metadata map[string]string `msgpack:"meta,omitempty"`
}

// NewError returns a new protocol error with the given code and message
func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

// NewErrorf returns a new protocol error with a formatted message
func NewErrorf(code ErrorCode, format string, args ...any) *Error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("protocol error [%s]", e.Code)
	}
	return fmt.Sprintf("protocol error [%s]: %s", e.Code, e.Message)
}

// Is compares protocol errors by their code, so errors.Is works against a sentinel built with NewError
func (e *Error) Is(target error) bool {
	var t *Error
	ok := errors.As(target, &t)
	if !ok {
		return false
	}
	return t.Code == e.Code
}

// WithRetryAfter sets the retry-after hint and returns the same error for chaining
func (e *Error) WithRetryAfter(d time.Duration) *Error {
	if d > 0 {
		e.RetryAfterMs = d.Milliseconds()
	}
	return e
}

// WithMetadata merges the provided metadata into the error and returns it for chaining
func (e *Error) WithMetadata(md map[string]string) *Error {
	if len(md) == 0 {
		return e
	}
	if e.Metadata == nil {
		e.Metadata = make(map[string]string, len(md))
	}
	maps.Copy(e.Metadata, md)
	return e
}

// RetryAfter returns the retry-after hint as a duration, if one is set
func (e *Error) RetryAfter() (time.Duration, bool) {
	if e.RetryAfterMs <= 0 {
		return 0, false
	}
	return time.Duration(e.RetryAfterMs) * time.Millisecond, true
}

// Retryable reports whether the caller should invalidate any cached placement and retry after a delay
func (e *Error) Retryable() bool {
	switch e.Code {
	case ErrCodeRetryLater, ErrCodeOverloaded, ErrCodeHostDraining, ErrCodeActorHalted, ErrCodeHostMismatch, ErrCodeActorNotHosted:
		return true
	default:
		return false
	}
}
