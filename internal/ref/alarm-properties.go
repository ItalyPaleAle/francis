package ref

import (
	"time"

	timeutils "github.com/italypaleale/francis/internal/time"
)

// Properties for an alarm
type AlarmProperties struct {
	// Due time.
	DueTime time.Time
	// Alarm repetition interval, as a ISO8601-formatted duration string.
	Interval string
	// Deadline for repeating alarms.
	TTL *time.Time
	// Data associated with the alarm.
	Data []byte
}

// NextExecution returns the time the alarm is executed next.
// Returns the zero time for non-repeating alarms.
func (a AlarmProperties) NextExecution(executionTime time.Time) (t time.Time) {
	// If there's no interval, does not repeat
	if a.Interval == "" {
		return t
	}

	// Parse the interval, as a ISO8601-formatted duration string
	d, err := timeutils.ParseISO8601Duration(a.Interval)
	if err != nil || d.IsZero() {
		// There shouldn't be errors here, so we just return zero
		return t
	}

	// Compute the next execution time
	t = executionTime.Add(d.Time).AddDate(d.Years, d.Months, d.Days)

	// Check if there's a TTL and if we're going beyond that
	if a.TTL != nil && t.After(*a.TTL) {
		t = time.Time{}
	}

	return t
}
