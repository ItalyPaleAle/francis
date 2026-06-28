package actor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobPropertiesValidate(t *testing.T) {
	t.Run("empty properties are valid", func(t *testing.T) {
		err := JobProperties{}.Validate()
		require.NoError(t, err)
	})

	t.Run("valid interval", func(t *testing.T) {
		err := JobProperties{Interval: "PT1H"}.Validate()
		require.NoError(t, err)
	})

	t.Run("valid cron", func(t *testing.T) {
		err := JobProperties{Cron: "*/5 * * * *"}.Validate()
		require.NoError(t, err)
	})

	t.Run("interval and cron are mutually exclusive", func(t *testing.T) {
		err := JobProperties{Interval: "PT1H", Cron: "* * * * *"}.Validate()
		require.ErrorContains(t, err, "mutually exclusive")
	})

	t.Run("due time and delay are mutually exclusive", func(t *testing.T) {
		err := JobProperties{DueTime: time.Now(), Delay: time.Minute}.Validate()
		require.ErrorContains(t, err, "mutually exclusive")
	})

	t.Run("negative delay is rejected", func(t *testing.T) {
		err := JobProperties{Delay: -time.Minute}.Validate()
		require.ErrorContains(t, err, "negative")
	})

	t.Run("malformed interval is rejected", func(t *testing.T) {
		err := JobProperties{Interval: "not-a-duration"}.Validate()
		require.ErrorContains(t, err, "invalid job interval")
	})

	t.Run("zero interval is rejected", func(t *testing.T) {
		err := JobProperties{Interval: "PT0S"}.Validate()
		require.ErrorContains(t, err, "greater than zero")
	})

	t.Run("malformed cron is rejected", func(t *testing.T) {
		err := JobProperties{Cron: "not a cron"}.Validate()
		require.ErrorContains(t, err, "invalid job cron")
	})
}

func TestJobPropertiesEffectiveDueTime(t *testing.T) {
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)

	t.Run("delay wins", func(t *testing.T) {
		p := JobProperties{Delay: 5 * time.Minute}
		d := p.EffectiveDueTime(now)
		assert.Equal(t, now.Add(5*time.Minute), d)
	})

	t.Run("absolute due time", func(t *testing.T) {
		due := now.Add(time.Hour)
		p := JobProperties{DueTime: due}
		d := p.EffectiveDueTime(now)
		assert.Equal(t, due, d)
	})

	t.Run("immediate when neither is set", func(t *testing.T) {
		p := JobProperties{}
		d := p.EffectiveDueTime(now)
		assert.Equal(t, now, d)
	})
}

func TestNewJobProperties(t *testing.T) {
	t.Run("options build the properties", func(t *testing.T) {
		due := time.Now().Add(time.Hour)
		ttl := time.Now().Add(24 * time.Hour)
		p, err := newJobProperties(
			WithJobDueTime(due),
			WithJobInterval("PT30M"),
			WithJobTTL(ttl),
			WithIdempotencyKey("key-1"),
		)
		require.NoError(t, err)
		assert.Equal(t, due, p.DueTime)
		assert.Equal(t, "PT30M", p.Interval)
		assert.Equal(t, ttl, p.TTL)
		assert.Equal(t, "key-1", p.IdempotencyKey)
	})

	t.Run("invalid options return an error", func(t *testing.T) {
		_, err := newJobProperties(WithJobInterval("PT1H"), WithJobCron("* * * * *"))
		require.Error(t, err)
	})
}

func TestJobStatusString(t *testing.T) {
	assert.Equal(t, "pending", JobStatusPending.String())
	assert.Equal(t, "active", JobStatusActive.String())
	assert.Equal(t, "dead-lettered", JobStatusDeadLettered.String())
}
