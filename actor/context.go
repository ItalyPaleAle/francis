package actor

import (
	"context"
)

type contextKey int

const (
	requestIDKey contextKey = iota
	haltingKey
)

// RequestIDFromContext returns the request ID stamped into the context by the framework for the current invocation.
// For Invoke calls this is a UUID that is stable across placement-stale retries, so the actor can detect and handle a duplicate call.
// For Alarm calls this is the provider-assigned alarm ID, stable across retry attempts for the same alarm occurrence.
// Deactivate calls carry no request ID, so an empty string is returned.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

// WithRequestID returns a context carrying the given request ID.
// This is called by the framework before invoking actor methods.
// Actor implementations should use RequestIDFromContext to read it.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// HaltingFromContext returns a channel that is closed when the actor running the current invocation begins halting, because its host is shutting down or the actor is being deactivated
// A handler that blocks for a long time should select on it and return promptly, so a graceful shutdown does not have to wait out the host's shutdown grace period before the invocation's context is cancelled
// It returns nil for a context that carries no halt signal, and since a receive on a nil channel blocks forever, selecting on the result is always safe
func HaltingFromContext(ctx context.Context) <-chan struct{} {
	v, _ := ctx.Value(haltingKey).(chan struct{})
	return v
}

// WithHalting returns a context carrying the channel that is closed when the actor begins halting
// This is called by the framework before invoking actor methods
// Actor implementations should use HaltingFromContext to read it
func WithHalting(ctx context.Context, haltCh chan struct{}) context.Context {
	return context.WithValue(ctx, haltingKey, haltCh)
}
