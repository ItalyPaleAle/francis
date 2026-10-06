package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionalTimeUTC(t *testing.T) {
	t.Run("zero time is nil", func(t *testing.T) {
		assert.Nil(t, OptionalTimeUTC(time.Time{}))
	})

	t.Run("time is converted to UTC", func(t *testing.T) {
		in := time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("UTC+2", 2*60*60))

		got := OptionalTimeUTC(in)
		require.NotNil(t, got)
		assert.Equal(t, time.UTC, got.Location())
		assert.True(t, in.Equal(*got))
	})
}

func TestTimePtrUTC(t *testing.T) {
	t.Run("nil is nil", func(t *testing.T) {
		assert.Nil(t, TimePtrUTC(nil))
	})

	t.Run("time is copied in UTC", func(t *testing.T) {
		zone := time.FixedZone("UTC+2", 2*60*60)
		in := time.Date(2026, 1, 2, 3, 4, 5, 6, zone)

		got := TimePtrUTC(&in)
		require.NotNil(t, got)
		assert.NotSame(t, &in, got)
		assert.Equal(t, time.UTC, got.Location())
		assert.True(t, in.Equal(*got))

		// The caller's time keeps its location
		assert.Equal(t, zone, in.Location())
	})

	t.Run("zero time is kept", func(t *testing.T) {
		in := time.Time{}

		got := TimePtrUTC(&in)
		require.NotNil(t, got)
		assert.True(t, got.IsZero())
	})
}
