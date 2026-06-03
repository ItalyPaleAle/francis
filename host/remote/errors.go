package remote

import (
	"context"
	"errors"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/protocol"
)

// errActorMethodUnsupported is returned when an actor does not implement the method required for an invocation or alarm
// It maps to a non-retryable protocol error so the caller does not keep retrying
var errActorMethodUnsupported = errors.New("actor does not implement the requested method")

// isProtocolErrorCode reports whether err is a protocol error carrying the given code
func isProtocolErrorCode(err error, code protocol.ErrorCode) bool {
	if err == nil {
		return false
	}

	perr, ok := errors.AsType[*protocol.Error](err)
	if !ok {
		return false
	}
	return perr.Code == code
}

// protocolErrorToActor maps a structured protocol error returned by the runtime or a peer to the public actor error
// Errors without a specific mapping are returned unchanged
func protocolErrorToActor(err error) error {
	perr, ok := errors.AsType[*protocol.Error](err)
	if !ok {
		return err
	}

	switch perr.Code {
	case protocol.ErrCodeActorNotHosted:
		return actor.ErrActorNotHosted
	case protocol.ErrCodeActorNotActive:
		return actor.ErrActorNotActive
	case protocol.ErrCodeActorHalted:
		return actor.ErrActorHalted
	case protocol.ErrCodeActorTypeUnsupported:
		return actor.ErrActorTypeUnsupported
	case protocol.ErrCodeStateNotFound:
		return actor.ErrStateNotFound
	case protocol.ErrCodeAlarmNotFound:
		return actor.ErrAlarmNotFound
	default:
		return err
	}
}

// invokeErrorToProtocol maps an error returned while invoking a local actor to a structured protocol error for a peer caller
func invokeErrorToProtocol(err error) *protocol.Error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errActorMethodUnsupported):
		// The actor type cannot service this call, so retrying will never succeed
		return protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, err.Error())
	case errors.Is(err, actor.ErrActorHalted):
		// The actor is halting, so the caller should re-resolve placement and retry
		return protocol.NewError(protocol.ErrCodeActorHalted, "actor is halted")
	case errors.Is(err, actor.ErrActorTypeUnsupported):
		return protocol.NewError(protocol.ErrCodeActorTypeUnsupported, "actor type is not supported on this host")
	case errors.Is(err, context.Canceled):
		return protocol.NewError(protocol.ErrCodeCanceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return protocol.NewError(protocol.ErrCodeDeadlineExceeded, err.Error())
	default:
		return protocol.NewErrorf(protocol.ErrCodeInvokeFailed, "actor invocation failed: %v", err)
	}
}
