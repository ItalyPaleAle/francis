package time

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTime(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	rfc := now.Format(time.RFC3339)
	ms := now.UnixMilli()

	t.Run("RFC3339 string", func(t *testing.T) {
		parsed, err := ParseTime(rfc)
		require.NoError(t, err)
		assert.True(t, parsed.Equal(now))
	})

	t.Run("UNIX ms as string", func(t *testing.T) {
		parsed, err := ParseTime(strconv.FormatInt(ms, 10))
		require.NoError(t, err)
		assert.True(t, parsed.Equal(now))
	})

	t.Run("UNIX ms as float64", func(t *testing.T) {
		parsed, err := ParseTime(float64(ms))
		require.NoError(t, err)
		assert.True(t, parsed.Equal(now))
	})

	t.Run("UNIX ms as int64", func(t *testing.T) {
		parsed, err := ParseTime(int64(ms))
		require.NoError(t, err)
		assert.True(t, parsed.Equal(now))
	})

	t.Run("empty string returns zero time", func(t *testing.T) {
		parsed, err := ParseTime("")
		require.NoError(t, err)
		assert.True(t, parsed.IsZero())
	})

	t.Run("nil returns zero time", func(t *testing.T) {
		parsed, err := ParseTime(nil)
		require.NoError(t, err)
		assert.True(t, parsed.IsZero())
	})

	t.Run("invalid string", func(t *testing.T) {
		_, err := ParseTime("notatime")
		assert.Error(t, err)
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := ParseTime([]byte("123"))
		assert.Error(t, err)
	})
}

func TestParseDuration_Parser(t *testing.T) {
	t.Run("ISO8601 string", func(t *testing.T) {
		out, err := ParseDuration("PT1H2M3.004S")
		require.NoError(t, err)
		assert.Equal(t, "PT1H2M3.004S", out)
	})

	t.Run("Go duration string", func(t *testing.T) {
		out, err := ParseDuration("1h2m3s4ms")
		require.NoError(t, err)
		assert.Equal(t, "PT1H2M3.004S", out)
	})

	t.Run("ms as string", func(t *testing.T) {
		out, err := ParseDuration("1234")
		require.NoError(t, err)
		assert.Equal(t, "PT1.234S", out)
	})

	t.Run("ms as float64", func(t *testing.T) {
		out, err := ParseDuration(float64(1500))
		require.NoError(t, err)
		assert.Equal(t, "PT1.500S", out)
	})

	t.Run("ms as int64", func(t *testing.T) {
		out, err := ParseDuration(int64(2000))
		require.NoError(t, err)
		assert.Equal(t, "PT2S", out)
	})

	t.Run("empty string returns empty", func(t *testing.T) {
		out, err := ParseDuration("")
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("nil returns zero duration", func(t *testing.T) {
		out, err := ParseDuration(nil)
		require.NoError(t, err)
		assert.Equal(t, zeroDuration, out)
	})

	t.Run("invalid string", func(t *testing.T) {
		_, err := ParseDuration("notaduration")
		assert.Error(t, err)
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := ParseDuration([]byte("123"))
		assert.Error(t, err)
	})
}
