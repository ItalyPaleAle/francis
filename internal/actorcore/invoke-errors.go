package actorcore

import (
	"context"
	"errors"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/protocol"
)

// ErrActorMethodUnsupported is returned when an actor does not implement the method required for an invocation or alarm
// It maps to a non-retryable protocol error so the caller does not keep retrying
var ErrActorMethodUnsupported = errors.New("actor does not implement the requested method")

// protocolErrorToActor maps a structured protocol error returned by a peer to the public actor error
// Errors without a specific mapping are returned unchanged
func protocolErrorToActor(err error) error {
	perr, ok := errors.AsType[*protocol.Error](err)
	if !ok {
		return err
	}

	switch perr.Code {
	case protocol.ErrCodeActorNotHosted, protocol.ErrCodeHostMismatch:
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

// InvokeErrorToProtocol maps an error returned while invoking a local actor to a structured protocol error for a peer or runtime caller
func InvokeErrorToProtocol(err error) *protocol.Error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrActorMethodUnsupported):
		// The actor type cannot service this call, so retrying will never succeed
		return protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, err.Error())
	case errors.Is(err, actor.ErrActorNotHosted):
		// The actor is owned by another host, so the caller should re-resolve placement and retry
		return protocol.NewError(protocol.ErrCodeActorNotHosted, "actor is not hosted here")
	case errors.Is(err, actor.ErrActorNotActive):
		// An active-only invocation found the actor inactive on this host
		return protocol.NewError(protocol.ErrCodeActorNotActive, "actor is not currently active")
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
