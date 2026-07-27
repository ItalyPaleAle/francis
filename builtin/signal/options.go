package signal

import (
	"time"
)

const (
	// defaultRetention is how long a completed signal is kept when WithRetention is not set
	defaultRetention = 24 * time.Hour
	// defaultIdleTimeout is how long a signal's actor stays in memory after its last call when WithIdleTimeout is not set
	defaultIdleTimeout = 5 * time.Minute
	// defaultMaxPayloadSize caps a completion payload when WithMaxPayloadSize is not set
	defaultMaxPayloadSize = 64 << 10 // 64 KiB
)

// signalOptions accumulates the configuration applied by the Option builders
type signalOptions struct {
	// retention is how long a completed signal's payload is kept, set by WithRetention
	// Zero uses the default, and a negative value means the completion never expires
	retention time.Duration
	// idleTimeout overrides how long a signal's actor stays in memory after its last call
	// Zero or negative uses the default
	idleTimeout time.Duration
	// maxPayloadSize caps the size of a completion payload, in bytes
	// Zero or negative uses the default
	maxPayloadSize int
}

// Option configures a signal set
type Option func(*signalOptions)

// WithRetention sets how long a completed signal's payload is kept, which is the window during which a Wait arriving after the completion returns immediately
// It defaults to 24 hours
// A negative value keeps completions forever, which suits a bounded set of signals whose callers may arrive arbitrarily late
//
// Set this longer than the longest lateness you expect from any caller: once the window passes, the completion record is gone and the signal becomes indistinguishable from one that never fired, so a late Wait blocks instead of returning
func WithRetention(d time.Duration) Option {
	return func(o *signalOptions) {
		o.retention = d
	}
}

// WithIdleTimeout overrides how long a signal's actor is kept in memory after its last call before it is deactivated
// It defaults to 5 minutes
//
// An actor with callers parked on it is never deactivated, however long it sits there, since the framework only deactivates an actor that has no invocation in flight
func WithIdleTimeout(d time.Duration) Option {
	return func(o *signalOptions) {
		o.idleTimeout = d
	}
}

// WithMaxPayloadSize caps the size of a completion payload, in bytes, measured after MessagePack encoding
// It defaults to 64 KiB
//
// The payload is stored in the signal's state row and returned to every caller waiting on it, so a large payload is paid for once in storage and once per caller
func WithMaxPayloadSize(n int) Option {
	return func(o *signalOptions) {
		o.maxPayloadSize = n
	}
}
