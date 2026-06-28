package actor

import (
	"context"
)

type contextKey int

const requestIDKey contextKey = iota

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
