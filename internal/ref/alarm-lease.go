package ref

import (
	"fmt"
	"time"
)

// AlarmLease indicates an alarm lease
type AlarmLease struct {
	alarmID string
	dueTime time.Time
	leaseID any
}

// NewAlarmLease returns a new AlarmLease object.
func NewAlarmLease(alarmID string, dueTime time.Time, leaseID any) *AlarmLease {
	return &AlarmLease{
		alarmID: alarmID,
		dueTime: dueTime,
		leaseID: leaseID,
	}
}

// Key returns the key for the alarm.
// This is implemented to comply with the queueable interface.
func (r AlarmLease) Key() string {
	return r.alarmID
}

// DueTime returns the due time for the alarm.
// This is implemented to comply with the queueable interface.
func (r AlarmLease) DueTime() time.Time {
	return r.dueTime
}

// LeaseID returns the value of the leaseID property.
func (r AlarmLease) LeaseID() any {
	return r.leaseID
}

// String implements fmt.Stringer and it's used for debugging
func (r AlarmLease) String() string {
	const RFC3339MilliNoTZ = "2006-01-02T15:04:05.999"

	return fmt.Sprintf(
		"AlarmLease:[AlarmID=%q DueTime=%q DueTimeUnix=%d LeaseID=%q]",
		r.alarmID, r.dueTime.Format(RFC3339MilliNoTZ), r.dueTime.UnixMilli(), r.leaseID,
	)
}
