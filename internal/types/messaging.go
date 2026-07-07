package types

import (
	"context"
)

type InvokeOpts struct {
	ActiveOnly bool
}

type contextKey int

const readOnlyKey contextKey = iota

// WithReadOnly returns a context marked as read-only, stamped by the framework before calling an actor's Peek/PeekStream method.
func WithReadOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, readOnlyKey, true)
}

// IsReadOnly reports whether ctx was marked read-only by WithReadOnly, i.e. whether the current invocation is a Peek/PeekStream rather than an Invoke/InvokeStream.
func IsReadOnly(ctx context.Context) bool {
	v, _ := ctx.Value(readOnlyKey).(bool)
	return v
}
