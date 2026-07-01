package ratelimit

import (
	"time"
)

// rateLimitOptions accumulates the configuration applied by the Option builders
type rateLimitOptions struct {
	// rate is the number of calls admitted per period
	rate int
	// per is the window the rate applies over
	// Defaults to 1 second when empty
	per time.Duration
	// burst is the token bucket's capacity: the maximum number of calls that may be admitted instantly before throttling kicks in, set by WithBurst
	// It defaults to zero, which is treated as a capacity of one, so calls are admitted strictly one at a time
	burst int
	// idleTimeout overrides how long an idle key's limiter is kept in memory
	// Setting to zero or negative uses the default idle timeout, which is the double of the "per" window with a minimum of 1 minute
	idleTimeout time.Duration
}

// Option configures a rate limit actor
type Option func(*rateLimitOptions)

// WithRate sets the number of calls admitted per period
// It is required and must be greater than zero
// Combine it with WithPer to change the period, which defaults to one second (so WithRate(100) alone is 100 calls per second)
func WithRate(rate int) Option {
	return func(o *rateLimitOptions) {
		o.rate = rate
	}
}

// WithPer sets the window the rate applies over (e.g. WithRate(2) with WithPer(time.Minute) is two calls per minute)
// It defaults to one second
func WithPer(period time.Duration) Option {
	return func(o *rateLimitOptions) {
		o.per = period
	}
}

// WithBurst sets the token bucket's capacity: how many calls may be admitted instantly before the limiter starts rejecting, refilling at the configured rate
// By default the capacity is one, so calls are admitted strictly one at a time and any excess is rejected until the bucket refills
// Pass this to allow short bursts above the steady rate (e.g. WithRate(10) with WithBurst(20) sustains 10/s but tolerates a spike of 20)
func WithBurst(burst int) Option {
	return func(o *rateLimitOptions) {
		o.burst = burst
	}
}

// WithIdleTimeout overrides how long a key's in-memory limiter is kept after its last call before the actor is deactivated and the key's throttle state is dropped
// It defaults to double the period (the WithPer window), with a minimum of one minute
// A shorter value reclaims memory faster when many distinct keys are limited, at the cost of resetting throttle state for keys that fall idle
func WithIdleTimeout(d time.Duration) Option {
	return func(o *rateLimitOptions) {
		o.idleTimeout = d
	}
}
