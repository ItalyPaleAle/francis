package ref

import (
	"time"
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
