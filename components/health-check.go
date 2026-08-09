package components

import (
	"time"

	"github.com/cenkalti/backoff/v5"
)

const (
	maxHealthCheckRetryBudget = 10 * time.Second
	healthCheckMaxAttempts    = 3
)

// HealthCheckPolicy schedules one sequence of health check attempts within the time reserved before a host's deadline
// The policy is stateful and must not be shared by concurrent retry sequences
type HealthCheckPolicy struct {
	deadline time.Duration
	attempts int
}

// NewHealthCheckPolicy builds a fresh retry sequence for a health check deadline
func NewHealthCheckPolicy(deadline time.Duration) *HealthCheckPolicy {
	return &HealthCheckPolicy{deadline: deadline}
}

// Deadline returns the maximum allowed gap between successful health checks
func (p *HealthCheckPolicy) Deadline() time.Duration {
	return p.deadline
}

// Interval returns the delay before a new sequence of health check attempts begins
func (p *HealthCheckPolicy) Interval() time.Duration {
	return p.deadline - p.Budget()
}

// Budget returns half the deadline capped at ten seconds
func (p *HealthCheckPolicy) Budget() time.Duration {
	return min(max(p.deadline/2, time.Duration(0)), maxHealthCheckRetryBudget)
}

// AttemptTimeout gives each of the three attempts one quarter of the retry budget
func (p *HealthCheckPolicy) AttemptTimeout() time.Duration {
	return p.Budget() / 4
}

// MaxAttempts returns the total number of attempts in this sequence including the initial attempt
func (p *HealthCheckPolicy) MaxAttempts() int {
	return healthCheckMaxAttempts
}

// Attempts returns how many failed attempts have been observed by NextRetryDelay in the current sequence
func (p *HealthCheckPolicy) Attempts() int {
	return p.attempts
}

// NextRetryDelay records a failed attempt and returns the exponential delay before the next one
// The two delays use the remaining quarter of the budget in a one-to-two ratio
// backoff.Stop is returned after the final allowed attempt fails
func (p *HealthCheckPolicy) NextRetryDelay() time.Duration {
	p.attempts++
	if p.attempts >= p.MaxAttempts() {
		return backoff.Stop
	}

	return (p.Budget() / 12) << (p.attempts - 1)
}

// NextBackOff lets the policy be consumed directly by retry loops that use the backoff.BackOff contract
func (p *HealthCheckPolicy) NextBackOff() time.Duration {
	return p.NextRetryDelay()
}

// Reset starts a new sequence with no attempts recorded
func (p *HealthCheckPolicy) Reset() {
	p.attempts = 0
}
