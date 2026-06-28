package ref

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	timeutils "github.com/italypaleale/francis/internal/time"
)

// AlarmProperties contains properties for an alarm
type AlarmProperties struct {
	// Due time.
	DueTime time.Time
	// Alarm repetition interval, as a ISO8601-formatted duration string.
	Interval string
	// Cron is a standard cron expression for repeating alarms/jobs.
	// Mutually exclusive with Interval: when set it takes precedence in NextExecution.
	Cron string
	// Deadline for repeating alarms.
	TTL *time.Time
	// Data associated with the alarm.
	Data []byte
}

var errAlarmIntervalZero = errors.New("alarm interval is zero")

// NextExecution returns the time the alarm is executed next.
// Returns the zero time (and nil error) for non-repeating alarms (no interval/cron or TTL exceeded).
// Returns a non-nil error when the stored interval or cron expression is corrupt — callers should keep the alarm rather than delete it.
func (a AlarmProperties) NextExecution(executionTime time.Time) (time.Time, error) {
	// A cron schedule takes precedence over the interval, since the two are mutually exclusive when set
	if a.Cron != "" {
		return a.nextCronExecution(executionTime)
	}

	// If there's no interval, does not repeat
	if a.Interval == "" {
		return time.Time{}, nil
	}

	// Parse the interval, as a ISO8601-formatted duration string
	d, err := timeutils.ParseISO8601Duration(a.Interval)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid alarm interval %q: %w", a.Interval, err)
	}
	if d.IsZero() {
		return time.Time{}, fmt.Errorf("invalid alarm interval %q: %w", a.Interval, errAlarmIntervalZero)
	}

	// Compute the next execution time
	t := executionTime.Add(d.Time).AddDate(d.Years, d.Months, d.Days)

	// Check if there's a TTL and if we're going beyond that
	if a.TTL != nil && t.After(*a.TTL) {
		return time.Time{}, nil
	}

	return t, nil
}

// nextCronExecution computes the next execution time from the cron expression, relative to the given time.
func (a AlarmProperties) nextCronExecution(executionTime time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(a.Cron)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid alarm cron expression %q: %w", a.Cron, err)
	}

	// Compute the next time strictly after the execution time
	t := sched.Next(executionTime)

	// Check if there's a TTL and if we're going beyond that
	if a.TTL != nil && t.After(*a.TTL) {
		return time.Time{}, nil
	}

	return t, nil
}
