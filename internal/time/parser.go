package time

import (
	"errors"
	"strconv"
	"time"
)

// ParseTime parses an input as time, supporting:
// - Timestamps formatted as RFC3339/ISO8601
// - UNIX timestamps, in milliseconds, parsed from numbers or strings
// This method is optimized to be used in a custom JSON unmarshaler.
func ParseTime(val any) (t time.Time, err error) {
	switch x := val.(type) {
	case string:
		// If it's an empty string, assume it's the zero time
		if x == "" {
			return time.Time{}, nil
		}

		// Try to parse the string as a UNIX timestamp (in t)
		ms, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			return time.UnixMilli(ms), nil
		}

		// Try to parse as RFC339/ISO8601 (standard for JSON)
		t, err = time.Parse(time.RFC3339, x)
		if err == nil {
			return t, nil
		}

		return time.Time{}, errors.New("invalid time string")
	case float64:
		return time.UnixMilli(int64(x)), nil
	case int64:
		return time.UnixMilli(x), nil
	case nil:
		// Zero time
		return time.Time{}, nil
	default:
		return time.Time{}, errors.New("invalid type")
	}
}

// ParseDuration parses an input as a time duration, supporting:
// - Duration formatted as ISO8601 strings
// - Go duration strings
// - Number of milliseconds, parsed from numbers or strings
// It returns a duration always formatted as ISO8601 string.
// This method is optimized to be used in a custom JSON unmarshaler.
func ParseDuration(val any) (string, error) {
	var dur Duration

	switch x := val.(type) {
	case string:
		// If it's an empty string, no duration
		if x == "" {
			return "", nil
		}

		// Try parsing as a number, indicating a number of ms
		ms, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			dur.Time = time.Duration(ms) * time.Millisecond
			return dur.String(), nil
		}

		// Try to parse as a duration string
		dur, err = ParseDurationString(x)
		if err == nil {
			return dur.String(), nil
		}

		return "", errors.New("invalid duration string")
	case float64:
		dur.Time = time.Duration(x) * time.Millisecond
		return dur.String(), nil
	case int64:
		dur.Time = time.Duration(x) * time.Millisecond
		return dur.String(), nil
	case nil:
		return zeroDuration, nil
	default:
		return "", errors.New("invalid type")
	}
}
