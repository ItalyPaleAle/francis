package components

import (
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderConfigValidate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		cfg := NewProviderConfig()
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("rejects a deadline without room for retries", func(t *testing.T) {
		for _, deadline := range []time.Duration{0, time.Second} {
			cfg := NewProviderConfig()
			cfg.HostHealthCheckDeadline = deadline

			err := cfg.Validate()
			require.ErrorContains(t, err, "HostHealthCheckDeadline")
		}
	})

	t.Run("returns a fresh health check policy for each sequence", func(t *testing.T) {
		cfg := NewProviderConfig()
		first := cfg.HealthCheckPolicy()
		second := cfg.HealthCheckPolicy()

		assert.NotSame(t, first, second)
		first.NextRetryDelay()
		assert.Equal(t, 1, first.Attempts())
		assert.Zero(t, second.Attempts())
	})

	t.Run("rejects the other invalid values", func(t *testing.T) {
		for name, mutate := range map[string]func(cfg *ProviderConfig){
			"AlarmsLeaseDuration":       func(cfg *ProviderConfig) { cfg.AlarmsLeaseDuration = 999 * time.Millisecond },
			"AlarmsFetchAheadInterval":  func(cfg *ProviderConfig) { cfg.AlarmsFetchAheadInterval = 99 * time.Millisecond },
			"AlarmsFetchAheadBatchSize": func(cfg *ProviderConfig) { cfg.AlarmsFetchAheadBatchSize = 0 },
			"MaxHosts":                  func(cfg *ProviderConfig) { cfg.MaxHosts = -1 },
		} {
			t.Run(name, func(t *testing.T) {
				cfg := NewProviderConfig()
				mutate(&cfg)

				err := cfg.Validate()
				require.Error(t, err)
				require.ErrorContains(t, err, name)
			})
		}
	})
}

func TestHealthCheckPolicy(t *testing.T) {
	t.Run("retry budget is half the deadline up to ten seconds", func(t *testing.T) {
		policy := NewHealthCheckPolicy(20 * time.Second)

		assert.Equal(t, 20*time.Second, policy.Deadline())
		assert.Equal(t, 10*time.Second, policy.Interval())
		assert.Equal(t, 10*time.Second, policy.Budget())
		assert.Equal(t, 2500*time.Millisecond, policy.AttemptTimeout())
		assert.Equal(t, 3, policy.MaxAttempts())
		assertPolicyFitsBudget(t, policy, []time.Duration{10 * time.Second / 12, 2 * (10 * time.Second / 12)})
	})

	t.Run("long deadlines use the same capped policy", func(t *testing.T) {
		policy := NewHealthCheckPolicy(2 * time.Minute)

		assert.Equal(t, 110*time.Second, policy.Interval())
		assert.Equal(t, 10*time.Second, policy.Budget())
		assert.Equal(t, 3, policy.MaxAttempts())
		assertPolicyFitsBudget(t, policy, []time.Duration{10 * time.Second / 12, 2 * (10 * time.Second / 12)})
	})

	t.Run("short deadlines scale the policy to the available budget", func(t *testing.T) {
		policy := NewHealthCheckPolicy(4 * time.Second)

		assert.Equal(t, 2*time.Second, policy.Interval())
		assert.Equal(t, 2*time.Second, policy.Budget())
		assert.Equal(t, 500*time.Millisecond, policy.AttemptTimeout())
		assertPolicyFitsBudget(t, policy, []time.Duration{2 * time.Second / 12, 2 * (2 * time.Second / 12)})
	})

	t.Run("reset starts a fresh attempt sequence", func(t *testing.T) {
		policy := NewHealthCheckPolicy(20 * time.Second)

		assert.Equal(t, 10*time.Second/12, policy.NextRetryDelay())
		assert.Equal(t, 1, policy.Attempts())
		policy.Reset()
		assert.Zero(t, policy.Attempts())
		assert.Equal(t, 10*time.Second/12, policy.NextRetryDelay())
	})
}

func assertPolicyFitsBudget(t *testing.T, policy *HealthCheckPolicy, wantDelays []time.Duration) {
	t.Helper()

	var totalDelay time.Duration
	for attempt, wantDelay := range wantDelays {
		delay := policy.NextRetryDelay()
		assert.Equal(t, wantDelay, delay, "retry delay after attempt %d", attempt+1)
		totalDelay += delay
	}

	assert.Equal(t, backoff.Stop, policy.NextRetryDelay())
	assert.Equal(t, policy.MaxAttempts(), policy.Attempts())
	total := totalDelay
	for range policy.MaxAttempts() {
		total += policy.AttemptTimeout()
	}
	assert.LessOrEqual(t, total, policy.Budget())
	assert.Less(t, policy.Budget()-total, 5*time.Nanosecond)
}
