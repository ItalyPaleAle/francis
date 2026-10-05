package utils

import (
	"time"
)

// OptionalTimeUTC returns nil for the zero time, otherwise a pointer to t in UTC
// It turns a time whose zero value means unset into an optional one, such as for a JSON field that is omitted when empty
func OptionalTimeUTC(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}

	u := t.UTC()
	return &u
}

// TimePtrUTC returns nil for a nil time, otherwise a pointer to a copy of *t in UTC
// The time is copied, so the caller's value keeps its location
func TimePtrUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}

	u := t.UTC()
	return &u
}
