package ratelimit

import (
	"time"
)

// rateLimitOptions accumulates the configuration applied by the Option builders
type rateLimitOptions struct {
	// rate is the number of calls admitted per period
	rate int
	// per is the window the rate applies over; zero means the go.uber.org/ratelimit default of one second
	per time.Duration
	// slack is the burst allowance carried over from unspent calls, set by WithSlack; it defaults to zero, which makes the limiter strict
	slack int
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

// WithSlack sets the burst allowance, letting the limiter accumulate up to slack unspent calls for a later burst
// By default the limiter is strict (no slack), so calls for a key are evenly spaced; pass this to opt into bursting
func WithSlack(slack int) Option {
	return func(o *rateLimitOptions) {
		o.slack = slack
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
