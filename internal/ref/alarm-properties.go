package ref

import (
	"time"
)

// Properties for an alarm
type AlarmProperties struct {
	// Due time.
	DueTime time.Time
	// Alarm repetition interval.
	// This can be an ISO-formatted duration or a Go duration string.
	Interval string
	// Deadline for repeating alarms.
	TTL *time.Time
	// Data associated with the alarm.
	Data []byte
}
