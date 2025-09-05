package actor

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlarmProperties_UnmarshalJSON(t *testing.T) {
	t.Run("RFC3339 timestamps and ISO8601 duration", func(t *testing.T) {
		jsonData := `{
			"dueTime": "2024-11-25T10:30:00Z",
			"interval": "PT1H30M",
			"ttl": "2024-11-26T10:30:00Z",
			"data": "SGVsbG8gV29ybGQ="
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		require.NoError(t, err)

		expectedDue, _ := time.Parse(time.RFC3339, "2024-11-25T10:30:00Z")
		expectedTTL, _ := time.Parse(time.RFC3339, "2024-11-26T10:30:00Z")

		assert.True(t, alarm.DueTime.Equal(expectedDue))
		assert.Equal(t, "PT1H30M", alarm.Interval)
		assert.True(t, alarm.TTL.Equal(expectedTTL))
		assert.Equal(t, []byte("Hello World"), alarm.Data)
	})

	t.Run("UNIX milliseconds and Go duration string", func(t *testing.T) {
		dueMs := time.Date(2024, 12, 25, 10, 30, 0, 0, time.UTC).UnixMilli()
		ttlMs := time.Date(2024, 12, 26, 10, 30, 0, 0, time.UTC).UnixMilli()

		jsonData := `{
			"dueTime": ` + strconv.FormatInt(dueMs, 10) + `,
			"interval": "1h30m",
			"ttl": ` + strconv.FormatInt(ttlMs, 10) + `,
			"data": null
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		require.NoError(t, err)

		expectedDue := time.Date(2024, 12, 25, 10, 30, 0, 0, time.UTC)
		expectedTTL := time.Date(2024, 12, 26, 10, 30, 0, 0, time.UTC)

		assert.True(t, alarm.DueTime.Equal(expectedDue))
		assert.Equal(t, "PT1H30M", alarm.Interval)
		assert.True(t, alarm.TTL.Equal(expectedTTL))
		assert.Nil(t, alarm.Data)
	})

	t.Run("milliseconds as strings and numbers", func(t *testing.T) {
		jsonData := `{
			"dueTime": "1703505000000",
			"interval": 5400000,
			"ttl": 1703591400000
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		require.NoError(t, err)

		expectedDue := time.UnixMilli(1703505000000)
		expectedTTL := time.UnixMilli(1703591400000)

		assert.True(t, alarm.DueTime.Equal(expectedDue))
		assert.Equal(t, "PT1H30M", alarm.Interval) // 5400000ms = 1.5 hours
		assert.True(t, alarm.TTL.Equal(expectedTTL))
	})

	t.Run("zero values", func(t *testing.T) {
		jsonData := `{
			"dueTime": "",
			"interval": "",
			"ttl": null
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		require.NoError(t, err)

		assert.True(t, alarm.DueTime.IsZero())
		assert.Equal(t, "", alarm.Interval)
		assert.True(t, alarm.TTL.IsZero())
	})

	t.Run("invalid dueTime", func(t *testing.T) {
		jsonData := `{
			"dueTime": "invalid",
			"interval": "PT1H",
			"ttl": "2024-11-25T10:30:00Z"
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid dueTime")
	})

	t.Run("invalid interval", func(t *testing.T) {
		jsonData := `{
			"dueTime": "2024-11-25T10:30:00Z",
			"interval": "invalid",
			"ttl": "2024-11-25T10:30:00Z"
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid interval")
	})

	t.Run("invalid ttl", func(t *testing.T) {
		jsonData := `{
			"dueTime": "2024-11-25T10:30:00Z",
			"interval": "PT1H",
			"ttl": "invalid"
		}`

		var alarm AlarmProperties
		err := json.Unmarshal([]byte(jsonData), &alarm)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid ttl")
	})
}
