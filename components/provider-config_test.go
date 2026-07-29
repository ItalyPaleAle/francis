//go:build unit

package components

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderConfigValidate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		cfg := NewProviderConfig()
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("an unset health check policy behaves as the defaults", func(t *testing.T) {
		cfg := NewProviderConfig()
		cfg.HealthCheck = nil

		err := cfg.Validate()
		require.NoError(t, err)
		assert.Equal(t, NewHealthCheckPolicy().MinDeadline(), cfg.HealthCheck.MinDeadline(), "a nil policy reads as the defaults")
	})

	t.Run("rejects a deadline shorter than the retry budget allows", func(t *testing.T) {
		cfg := NewProviderConfig()
		minDeadline := cfg.HealthCheck.MinDeadline()

		cfg.HostHealthCheckDeadline = minDeadline - time.Millisecond
		err := cfg.Validate()
		require.Error(t, err)
		require.ErrorContains(t, err, "HostHealthCheckDeadline")

		cfg.HostHealthCheckDeadline = minDeadline
		err = cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("the minimum deadline follows the configured policy", func(t *testing.T) {
		cfg := NewProviderConfig()
		cfg.HealthCheck = &HealthCheckPolicy{
			AttemptTimeout: time.Second,
			RetryDelay:     100 * time.Millisecond,
			MaxAttempts:    3,
			MinInterval:    500 * time.Millisecond,
		}
		require.Less(t, cfg.HealthCheck.MinDeadline(), NewHealthCheckPolicy().MinDeadline())

		cfg.HostHealthCheckDeadline = 4 * time.Second
		err := cfg.Validate()
		require.NoError(t, err, "a deadline the shortened policy can be honored within must be accepted")

		cfg.HealthCheck = NewHealthCheckPolicy()
		err = cfg.Validate()
		require.Error(t, err)
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
	t.Run("budget covers every attempt and the delays between them", func(t *testing.T) {
		p := HealthCheckPolicy{
			AttemptTimeout: 2 * time.Second,
			RetryDelay:     500 * time.Millisecond,
			MaxAttempts:    3,
			MinInterval:    time.Second,
		}
		assert.Equal(t, 7*time.Second, p.Budget(), "three attempts of 2s with two 500ms delays between them")
		assert.Equal(t, 8*time.Second, p.MinDeadline())
	})

	t.Run("a sequence started on schedule ends as the deadline expires", func(t *testing.T) {
		p := NewHealthCheckPolicy()
		tt := []time.Duration{
			p.MinDeadline(),
			20 * time.Second,
			30 * time.Second,
			time.Minute,
		}
		for _, deadline := range tt {
			assert.LessOrEqualf(t, p.Interval(deadline)+p.Budget(), deadline, "a host with a %s deadline would still be retrying past it", deadline)
		}
	})

	t.Run("the zero value and a nil policy behave as the defaults", func(t *testing.T) {
		defaults := NewHealthCheckPolicy()
		for name, p := range map[string]*HealthCheckPolicy{
			"zero value": {},
			"nil":        nil,
		} {
			assert.Equal(t, defaults.Budget(), p.Budget(), name)
			assert.Equal(t, defaults.MinDeadline(), p.MinDeadline(), name)
			assert.Equal(t, defaults.Interval(time.Minute), p.Interval(time.Minute), name)
			assert.Equal(t, defaults.String(), p.String(), name)
		}
	})

	t.Run("unset fields fall back to their defaults when read", func(t *testing.T) {
		p := &HealthCheckPolicy{MaxAttempts: 5}
		assert.Equal(t, uint(5), p.EffectiveMaxAttempts())
		assert.Equal(t, DefaultHealthCheckAttemptTimeout, p.EffectiveAttemptTimeout())
		assert.Equal(t, DefaultHealthCheckRetryDelay, p.EffectiveRetryDelay())
		assert.Equal(t, DefaultMinHealthCheckInterval, p.EffectiveMinInterval())
	})

	t.Run("the interval floors at the minimum", func(t *testing.T) {
		p := NewHealthCheckPolicy()
		assert.Equal(t, p.MinInterval, p.Interval(p.Budget()))
		assert.Equal(t, p.MinInterval, p.Interval(0))
	})
}
