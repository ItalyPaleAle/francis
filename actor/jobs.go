package actor

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	timeutils "github.com/italypaleale/francis/internal/time"
)

// JobStatus is the lifecycle stage of a dispatched job.
type JobStatus int

const (
	// JobStatusPending indicates the job is scheduled and waiting to run
	JobStatusPending JobStatus = iota
	// JobStatusActive indicates the job is currently being executed (it holds a lease)
	JobStatusActive
	// JobStatusDeadLettered indicates the job exhausted its retries (or failed permanently) and was recorded in the dead-letter store
	JobStatusDeadLettered
)

// String implements fmt.Stringer.
func (s JobStatus) String() string {
	switch s {
	case JobStatusPending:
		return "pending"
	case JobStatusActive:
		return "active"
	case JobStatusDeadLettered:
		return "dead-lettered"
	default:
		return "unknown"
	}
}

// JobInfo describes a dispatched job, spanning both live (pending/active) and dead-lettered jobs.
// Attempts and LastError are only populated once the job has been dead-lettered.
// Live retry counters are kept in-memory on the lease and are not persisted.
type JobInfo struct {
	JobID     string
	ActorType string
	ActorID   string
	Method    string
	Status    JobStatus
	DueTime   time.Time
	Interval  string
	Cron      string
	Attempts  int
	LastError string
	CreatedAt time.Time
}

// JobProperties contains the resolved scheduling options for a dispatched job.
// It is built by the JobOption functions and crosses the actor->host boundary, mirroring how AlarmProperties is passed to SetAlarm.
type JobProperties struct {
	// DueTime is the absolute time the job should first run
	// When zero (and Delay is zero) the job runs "immediately"
	DueTime time.Time
	// Delay schedules the job relative to dispatch time
	// It is resolved against the host clock and is mutually exclusive with DueTime
	Delay time.Duration
	// Interval is the repetition interval as an ISO8601-formatted duration string
	// Mutually exclusive with Cron
	Interval string
	// TTL is the deadline after which a repeating job stops
	TTL time.Time
	// Cron is a standard cron expression for repeating jobs
	// Mutually exclusive with Interval
	Cron string
	// IdempotencyKey, when set, dedups re-dispatch within the same actor
	IdempotencyKey string
}

// JobOption configures the properties of a dispatched job.
type JobOption func(p *JobProperties)

// WithJobDueTime schedules the job to first run at an absolute time.
// The default, when neither this nor WithJobDelay is set, is to run "immediately".
func WithJobDueTime(t time.Time) JobOption {
	return func(p *JobProperties) {
		p.DueTime = t
	}
}

// WithJobDelay schedules the job to first run after the given delay, relative to dispatch time.
func WithJobDelay(d time.Duration) JobOption {
	return func(p *JobProperties) {
		p.Delay = d
	}
}

// WithJobInterval makes the job repeat on the given ISO8601-formatted interval.
// It is mutually exclusive with WithJobCron.
func WithJobInterval(iso8601 string) JobOption {
	return func(p *JobProperties) {
		p.Interval = iso8601
	}
}

// WithJobTTL stops a repeating job from running after the given time.
func WithJobTTL(t time.Time) JobOption {
	return func(p *JobProperties) {
		p.TTL = t
	}
}

// WithJobCron makes the job repeat on the given standard cron expression.
// It is mutually exclusive with WithJobInterval.
func WithJobCron(expr string) JobOption {
	return func(p *JobProperties) {
		p.Cron = expr
	}
}

// WithIdempotencyKey dedups re-dispatch of a job within the same actor.
// Two dispatches to the same actor with the same key produce a single job.
// Without a key each dispatch is a distinct job.
func WithIdempotencyKey(key string) JobOption {
	return func(p *JobProperties) {
		p.IdempotencyKey = key
	}
}

// newJobProperties resolves a set of JobOptions into a validated JobProperties.
func newJobProperties(opts ...JobOption) (JobProperties, error) {
	var p JobProperties
	for _, opt := range opts {
		opt(&p)
	}

	err := p.Validate()
	if err != nil {
		return JobProperties{}, err
	}

	return p, nil
}

// Validate checks that the job properties are well-formed.
// It rejects combining DueTime with Delay, combining Interval with Cron, and malformed intervals or cron expressions.
func (p JobProperties) Validate() error {
	// A job is scheduled by an absolute due time or a relative delay, never both
	if p.Delay != 0 && !p.DueTime.IsZero() {
		return errors.New("job due time and delay are mutually exclusive")
	}
	if p.Delay < 0 {
		return errors.New("job delay must not be negative")
	}

	// A repeating job repeats on an interval or a cron schedule, never both
	switch {
	case p.Interval != "" && p.Cron != "":
		return errors.New("job interval and cron are mutually exclusive")
	case p.Interval != "":
		// A non-empty interval must parse and produce a non-zero repeat period
		d, err := timeutils.ParseISO8601Duration(p.Interval)
		if err != nil {
			return fmt.Errorf("invalid job interval: %w", err)
		}
		if d.IsZero() {
			return errors.New("job interval must be greater than zero")
		}
	case p.Cron != "":
		// A non-empty cron expression must parse as a standard cron expression
		_, err := cron.ParseStandard(p.Cron)
		if err != nil {
			return fmt.Errorf("invalid job cron expression: %w", err)
		}
	}

	return nil
}

// EffectiveDueTime resolves the first run time against the given current time.
// A relative delay wins, then an absolute due time, then a cron schedule's own next tick, otherwise the job is due immediately.
func (p JobProperties) EffectiveDueTime(now time.Time) time.Time {
	switch {
	case p.Delay > 0:
		return now.Add(p.Delay)
	case !p.DueTime.IsZero():
		return p.DueTime
	case p.Cron != "":
		// A repeating cron job with no explicit due time schedules its first occurrence at the next tick rather than firing immediately at registration
		// Validate has already confirmed the expression parses by the time this runs, so the parse error here should not occur in practice
		sched, err := cron.ParseStandard(p.Cron)
		if err != nil {
			return now
		}
		return sched.Next(now)
	default:
		return now
	}
}
