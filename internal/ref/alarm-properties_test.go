package ref

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlarmProperties_NextExecution(t *testing.T) {
	base := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("no interval returns zero time", func(t *testing.T) {
		a := AlarmProperties{}
		next, err := a.NextExecution(base)
		require.NoError(t, err)
		assert.True(t, next.IsZero())
	})

	t.Run("valid interval returns correct next time", func(t *testing.T) {
		a := AlarmProperties{Interval: "PT1H"}
		next, err := a.NextExecution(base)
		require.NoError(t, err)
		assert.Equal(t, base.Add(time.Hour), next)
	})

	t.Run("TTL exceeded returns zero time", func(t *testing.T) {
		ttl := base.Add(30 * time.Minute)
		a := AlarmProperties{Interval: "PT1H", TTL: &ttl}
		next, err := a.NextExecution(base)
		require.NoError(t, err)
		assert.True(t, next.IsZero())
	})

	t.Run("TTL not yet exceeded returns next time", func(t *testing.T) {
		ttl := base.Add(2 * time.Hour)
		a := AlarmProperties{Interval: "PT1H", TTL: &ttl}
		next, err := a.NextExecution(base)
		require.NoError(t, err)
		assert.Equal(t, base.Add(time.Hour), next)
	})

	t.Run("corrupt interval returns error, not zero", func(t *testing.T) {
		// Trailing garbage
		a := AlarmProperties{Interval: "PT1Hgarbage"}
		_, err := a.NextExecution(base)
		require.Error(t, err)
	})

	t.Run("zero interval returns error", func(t *testing.T) {
		a := AlarmProperties{Interval: "PT0S"}
		_, err := a.NextExecution(base)
		require.Error(t, err)
	})

	t.Run("negative interval returns error", func(t *testing.T) {
		a := AlarmProperties{Interval: "PT-1H"}
		_, err := a.NextExecution(base)
		require.Error(t, err)
	})
}

func TestNextExecutionCron(t *testing.T) {
	base := time.Date(2026, 6, 28, 12, 2, 0, 0, time.UTC)

	t.Run("cron computes the next occurrence", func(t *testing.T) {
		p := AlarmProperties{Cron: "*/5 * * * *"}
		next, err := p.NextExecution(base)
		require.NoError(t, err)
		assert.Equal(t, time.Date(2026, 6, 28, 12, 5, 0, 0, time.UTC), next)
	})

	t.Run("cron takes precedence over interval", func(t *testing.T) {
		// A nonsensical interval would error if it were used, proving cron wins
		p := AlarmProperties{Cron: "*/5 * * * *", Interval: "not-a-duration"}
		next, err := p.NextExecution(base)
		require.NoError(t, err)
		assert.Equal(t, time.Date(2026, 6, 28, 12, 5, 0, 0, time.UTC), next)
	})

	t.Run("cron past TTL returns zero", func(t *testing.T) {
		ttl := base.Add(time.Minute)
		p := AlarmProperties{Cron: "*/5 * * * *", TTL: &ttl}
		next, err := p.NextExecution(base)
		require.NoError(t, err)
		assert.True(t, next.IsZero())
	})

	t.Run("invalid cron returns an error", func(t *testing.T) {
		p := AlarmProperties{Cron: "not a cron"}
		_, err := p.NextExecution(base)
		require.Error(t, err)
	})
}
