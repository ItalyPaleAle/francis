// This code was adapted from https://github.com/dapr/kit/tree/v0.15.4/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDuration(t *testing.T) {
	t.Run("parse time.Duration", func(t *testing.T) {
		d, err := ParseDurationString("0h30m0s")
		require.NoError(t, err)
		assert.Equal(t, time.Minute*30, d.Time)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
	})

	t.Run("parse ISO 8601 duration", func(t *testing.T) {
		d, err := ParseDurationString("P1MT2H10M3S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 1, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Hour*2+time.Minute*10+time.Second*3, d.Time)

		d, err = ParseDurationString("P2W")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 14, d.Days)
		assert.Equal(t, time.Duration(0), d.Time)

		d, err = ParseDurationString("PT1S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second, d.Time)

		d, err = ParseDurationString("P1M")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 1, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Duration(0), d.Time)

		d, err = ParseDurationString("PT1M")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Minute, d.Time)

		d, err = ParseDurationString("P0D")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Duration(0), d.Time)

		d, err = ParseDurationString("PT0S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Duration(0), d.Time)

		// This is technically invalid because it's out of order, but we'll accept anyways
		d, err = ParseDurationString("P1M2D")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 1, d.Months)
		assert.Equal(t, 2, d.Days)
		assert.Equal(t, time.Duration(0), d.Time)
	})

	t.Run("fraction of seconds", func(t *testing.T) {
		d, err := ParseDurationString("PT1.002S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second+2*time.Millisecond, d.Time)

		d, err = ParseDurationString("PT1.02S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second+20*time.Millisecond, d.Time)

		d, err = ParseDurationString("PT1.020S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second+20*time.Millisecond, d.Time)

		d, err = ParseDurationString("PT1.2S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second+200*time.Millisecond, d.Time)

		d, err = ParseDurationString("PT1.200S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second+200*time.Millisecond, d.Time)

		d, err = ParseDurationString("PT1.000S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, time.Second, d.Time)

		d, err = ParseDurationString("PT0.003S")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, 3*time.Millisecond, d.Time)

		_, err = ParseDurationString("PT1.0001S")
		require.Error(t, err)

		_, err = ParseDurationString("PT.0001S")
		require.Error(t, err)
	})

	t.Run("fractions are allowed for seconds only", func(t *testing.T) {
		_, err := ParseDurationString("P1.1Y")
		require.Error(t, err)

		_, err = ParseDurationString("P1.1M")
		require.Error(t, err)

		_, err = ParseDurationString("P1.1D")
		require.Error(t, err)

		_, err = ParseDurationString("PT1.1H")
		require.Error(t, err)

		_, err = ParseDurationString("PT1.1M")
		require.Error(t, err)
	})

	t.Run("parse ISO8610 and calculate with leap year", func(t *testing.T) {
		d, err := ParseDurationString("P1Y2M3D")
		require.NoError(t, err)

		// 2020 is a leap year
		start, _ := time.Parse("2006-01-02 15:04:05", "2020-02-03 11:12:13")
		target := start.AddDate(d.Years, d.Months, d.Days).Add(d.Time)
		expect, _ := time.Parse("2006-01-02 15:04:05", "2021-04-06 11:12:13")
		assert.Equal(t, expect, target)

		// 2019 is not a leap year
		start, _ = time.Parse("2006-01-02 15:04:05", "2019-02-03 11:12:13")
		target = start.AddDate(d.Years, d.Months, d.Days).Add(d.Time)
		expect, _ = time.Parse("2006-01-02 15:04:05", "2020-04-06 11:12:13")
		assert.Equal(t, expect, target)
	})

	t.Run("parse RFC3339 datetime", func(t *testing.T) {
		_, err := ParseDurationString(time.Now().Add(time.Minute).Format(time.RFC3339))
		require.Error(t, err)
	})

	t.Run("parse empty string", func(t *testing.T) {
		_, err := ParseDurationString("")
		require.Error(t, err)
	})

	t.Run("invalid ISO8601 duration", func(t *testing.T) {
		// Doesn't start with P
		_, err := ParseDurationString("10D1M")
		require.Error(t, err)

		// Invalid formats
		_, err = ParseDurationString("P")
		require.Error(t, err)
		_, err = ParseDurationString("PM")
		require.Error(t, err)
		_, err = ParseDurationString("PT1D")
		require.Error(t, err)
		_, err = ParseDurationString("P_D")
		require.Error(t, err)
		_, err = ParseDurationString("PTxS")
		require.Error(t, err)
	})
}

func TestDuration_String(t *testing.T) {
	tests := []struct {
		name string
		d    Duration
		exp  string
	}{
		{"zero", Duration{}, "PT0S"},
		{"hours", Duration{Time: 2 * time.Hour}, "PT2H"},
		{"minutes", Duration{Time: 5 * time.Minute}, "PT5M"},
		{"seconds", Duration{Time: 7 * time.Second}, "PT7S"},
		{"milliseconds", Duration{Time: 123 * time.Millisecond}, "PT0.123S"},
		{"seconds milliseconds", Duration{Time: 8*time.Second + 45*time.Millisecond}, "PT8.045S"},
		{"hours minutes seconds", Duration{Time: 1*time.Hour + 2*time.Minute + 3*time.Second}, "PT1H2M3S"},
		{"hours minutes seconds milliseconds", Duration{Time: 1*time.Hour + 2*time.Minute + 3*time.Second + 9*time.Millisecond}, "PT1H2M3.009S"},
		{"less than 1ms is zero", Duration{Time: 1 * time.Nanosecond}, "PT0S"},
		{"days and less than 1ms has no time", Duration{Time: 1 * time.Nanosecond, Days: 1}, "P1D"},
		{"hours minutes seconds nanoseconds", Duration{Time: 1*time.Hour + 2*time.Minute + 3*time.Second + 9*time.Nanosecond}, "PT1H2M3S"},
		{"full string", Duration{Years: 1, Months: 2, Days: 3, Time: 4*time.Hour + 5*time.Minute + 6*time.Second + 7*time.Millisecond}, "P1Y2M3DT4H5M6.007S"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.d.String()
			assert.Equal(t, tt.exp, got)
		})
	}
}

func TestParseISO8601Duration(t *testing.T) {
	tests := []struct {
		input string
		exp   Duration
	}{
		{"PT0S", Duration{}},
		{"PT2H", Duration{Time: 2 * time.Hour}},
		{"PT5M", Duration{Time: 5 * time.Minute}},
		{"PT7S", Duration{Time: 7 * time.Second}},
		{"P1Y2M3DT4H5M6S", Duration{Years: 1, Months: 2, Days: 3, Time: 4*time.Hour + 5*time.Minute + 6*time.Second}},
		{"P3DT12H", Duration{Days: 3, Time: 12 * time.Hour}},
		{"PT1H2M3S", Duration{Time: 1*time.Hour + 2*time.Minute + 3*time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			d, err := ParseISO8601Duration(tt.input)
			require.NoError(t, err)

			assert.Equal(t, tt.exp.Years, d.Years)
			assert.Equal(t, tt.exp.Months, d.Months)
			assert.Equal(t, tt.exp.Days, d.Days)
			assert.Equal(t, tt.exp.Time.Milliseconds(), d.Time.Milliseconds())
		})
	}
}

func TestDuration_String_Parse_Roundtrip(t *testing.T) {
	inputs := []string{
		"PT0S",
		"PT2H",
		"PT5M",
		"PT7S",
		"P1Y2M3DT4H5M6S",
		"PT1H2M3S",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			d, err := ParseISO8601Duration(input)
			require.NoError(t, err)

			got := d.String()
			assert.Equal(t, input, got)
		})
	}
}

func TestDuration_FromString(t *testing.T) {
	t.Run("ISO8601 duration", func(t *testing.T) {
		var d Duration
		err := d.FromString("P1Y2M3DT4H5M6.007S")
		require.NoError(t, err)
		assert.Equal(t, 1, d.Years)
		assert.Equal(t, 2, d.Months)
		assert.Equal(t, 3, d.Days)
		assert.Equal(t, 4*time.Hour+5*time.Minute+6*time.Second+7*time.Millisecond, d.Time)
	})

	t.Run("Go duration string", func(t *testing.T) {
		var d Duration
		err := d.FromString("1h2m3s7ms")
		require.NoError(t, err)
		assert.Equal(t, 0, d.Years)
		assert.Equal(t, 0, d.Months)
		assert.Equal(t, 0, d.Days)
		assert.Equal(t, 1*time.Hour+2*time.Minute+3*time.Second+7*time.Millisecond, d.Time)
	})

	t.Run("empty string resets", func(t *testing.T) {
		d := Duration{Years: 1, Months: 1, Days: 1, Time: time.Second}
		err := d.FromString("")
		require.NoError(t, err)
		assert.True(t, d.IsZero())
	})

	t.Run("invalid string errors", func(t *testing.T) {
		var d Duration
		err := d.FromString("notaduration")
		assert.Error(t, err)
	})
}

func TestDuration_Reset(t *testing.T) {
	d := Duration{Years: 1, Months: 2, Days: 3, Time: 4 * time.Hour}
	d.Reset()
	assert.Equal(t, 0, d.Years)
	assert.Equal(t, 0, d.Months)
	assert.Equal(t, 0, d.Days)
	assert.Equal(t, time.Duration(0), d.Time)
}

func TestDuration_IsZero(t *testing.T) {
	assert.True(t, Duration{}.IsZero())
	assert.True(t, Duration{Time: 0}.IsZero())
	assert.True(t, Duration{Time: time.Nanosecond}.IsZero())
	assert.False(t, Duration{Time: time.Millisecond}.IsZero())
	assert.False(t, Duration{Days: 1}.IsZero())
	assert.False(t, Duration{Months: 1}.IsZero())
	assert.False(t, Duration{Years: 1}.IsZero())
}
