//nolint:recvcheck
package ref

import (
	"fmt"
	"time"
)

// AlarmLease indicates an alarm lease
type AlarmLease struct {
	ref           AlarmRef
	alarmID       string
	dueTime       time.Time
	leaseID       any
	attempts      int
	executionTime time.Time
}

// NewAlarmLease returns a new AlarmLease object.
func NewAlarmLease(ref AlarmRef, alarmID string, dueTime time.Time, leaseID any) *AlarmLease {
	return &AlarmLease{
		ref:     ref,
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

// AlarmRef returns the alarm reference for the alarm.
func (r AlarmLease) AlarmRef() AlarmRef {
	return r.ref
}

// ActorRef returns the actor reference for the alarm.
func (r AlarmLease) ActorRef() ActorRef {
	return r.ref.ActorRef()
}

// Attempts returns the number of attempts for this alarm.
func (r AlarmLease) Attempts() int {
	return r.attempts
}

// IncreaseAttempts increases the attempts counter for the alarm, also delaying the execution to a new due time.
func (r *AlarmLease) IncreaseAttempts(dueTime time.Time) {
	r.attempts++
	r.dueTime = dueTime

	// Reset the execution time too
	r.executionTime = time.Time{}
}

// SetExecutionTime sets the time the alarm was executed at
func (r *AlarmLease) SetExecutionTime(t time.Time) {
	r.executionTime = t
}

// ExecutionTime returns the time the alarm was executed at
func (r AlarmLease) ExecutionTime() time.Time {
	return r.executionTime
}

// String implements fmt.Stringer and it's used for debugging.
func (r AlarmLease) String() string {
	const RFC3339MilliNoTZ = "2006-01-02T15:04:05.999"

	return fmt.Sprintf(
		"AlarmLease:[AlarmID=%q DueTime=%q DueTimeUnix=%d LeaseID=%q]",
		r.alarmID, r.dueTime.Format(RFC3339MilliNoTZ), r.dueTime.UnixMilli(), r.leaseID,
	)
}
