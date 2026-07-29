package components

import (
	"fmt"
	"time"
)

// Default values for HealthCheckPolicy
const (
	// DefaultHealthCheckAttemptTimeout bounds a single health check attempt
	// It is smaller than the general provider request timeout
	DefaultHealthCheckAttemptTimeout = 3500 * time.Millisecond
	// DefaultHealthCheckRetryDelay is the delay between health check attempts
	DefaultHealthCheckRetryDelay = 250 * time.Millisecond
	// DefaultHealthCheckMaxAttempts is how many times a host tries a health check before giving up (including the initial attempt)
	DefaultHealthCheckMaxAttempts = 3
	// DefaultMinHealthCheckInterval is the shortest gap that will be scheduled between health checks
	DefaultMinHealthCheckInterval = time.Second
)

// HealthCheckPolicy controls how hosts perform healthchecks with the runtime actor runtime
// Default values are recommended for all production deployments: a nil policy, or any field left unset, means the default for that field
type HealthCheckPolicy struct {
	// AttemptTimeout is the timeout for each healthcheck attempt
	AttemptTimeout time.Duration

	// RetryDelay is the constant delay between attempts
	RetryDelay time.Duration

	// MaxAttempts is how many times a host tries a health check before giving up and shutting down
	MaxAttempts int

	// MinInterval is the shortest gap that will be scheduled between health checks
	MinInterval time.Duration
}

// NewHealthCheckPolicy returns a HealthCheckPolicy with all default values
func NewHealthCheckPolicy() *HealthCheckPolicy {
	return &HealthCheckPolicy{
		AttemptTimeout: DefaultHealthCheckAttemptTimeout,
		RetryDelay:     DefaultHealthCheckRetryDelay,
		MaxAttempts:    DefaultHealthCheckMaxAttempts,
		MinInterval:    DefaultMinHealthCheckInterval,
	}
}

// EffectiveAttemptTimeout returns the timeout for each attempt, applying the default when unset
func (p *HealthCheckPolicy) EffectiveAttemptTimeout() time.Duration {
	if p == nil || p.AttemptTimeout <= 0 {
		return DefaultHealthCheckAttemptTimeout
	}

	return p.AttemptTimeout
}

// EffectiveRetryDelay returns the delay between attempts, applying the default when unset
func (p *HealthCheckPolicy) EffectiveRetryDelay() time.Duration {
	if p == nil || p.RetryDelay <= 0 {
		return DefaultHealthCheckRetryDelay
	}

	return p.RetryDelay
}

// EffectiveMaxAttempts returns how many attempts a host makes before giving up, applying the default when unset
func (p *HealthCheckPolicy) EffectiveMaxAttempts() uint {
	if p == nil || p.MaxAttempts <= 0 || p.MaxAttempts > 10 {
		return DefaultHealthCheckMaxAttempts
	}

	return uint(p.MaxAttempts)
}

// EffectiveMinInterval returns the shortest gap scheduled between health checks, applying the default when unset
func (p *HealthCheckPolicy) EffectiveMinInterval() time.Duration {
	if p == nil || p.MinInterval <= 0 {
		return DefaultMinHealthCheckInterval
	}

	return p.MinInterval
}

// Budget is the wall time a full sequence of attempts can consume, covering every attempt and the delays between them
func (p *HealthCheckPolicy) Budget() time.Duration {
	//#nosec G115 -- attempts validated to be between 1 and 10
	attempts := time.Duration(p.EffectiveMaxAttempts())
	return attempts*p.EffectiveAttemptTimeout() + (attempts-1)*p.EffectiveRetryDelay()
}

// MinDeadline is the shortest health check deadline this policy can be honored within
// Anything below it cannot hold a full sequence of attempts plus a gap before the next one
func (p *HealthCheckPolicy) MinDeadline() time.Duration {
	return p.Budget() + p.EffectiveMinInterval()
}

// Interval returns the gap to leave between health checks for a given deadline, which is whatever the deadline leaves once the retry budget is set aside
func (p *HealthCheckPolicy) Interval(deadline time.Duration) time.Duration {
	return max(deadline-p.Budget(), p.EffectiveMinInterval())
}

// String implements fmt.Stringer
// It explains what the policy's minimum deadline is made of, for a validation error a reader can act on
func (p *HealthCheckPolicy) String() string {
	return fmt.Sprintf("%d attempts of %v with %v between them, plus a minimum interval of %v", p.EffectiveMaxAttempts(), p.EffectiveAttemptTimeout(), p.EffectiveRetryDelay(), p.EffectiveMinInterval())
}
