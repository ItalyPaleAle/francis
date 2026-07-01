package cronjob

import (
	"context"
	"log/slog"
	"time"

	timeutils "github.com/italypaleale/francis/internal/time"
)

// cronJobOptions accumulates the configuration applied by the Option builders
type cronJobOptions struct {
	// interval is the repetition as an ISO8601 duration string (from WithInterval or WithPeriod)
	interval string
	// cron is a standard cron expression (from WithCron)
	cron string
	// immediate runs the job once right away on first registration
	immediate bool
	// job is the function executed on each occurrence
	job func(ctx context.Context) error
	// logger is the optional slog logger used to report registration and run events
	logger *slog.Logger
	// scheduleSetters counts how many of WithInterval/WithPeriod/WithCron were applied, to enforce "exactly one"
	scheduleSetters int
}

// Option configures a cron job actor
type Option func(*cronJobOptions)

// WithInterval repeats the job on a fixed interval
// Exactly one of WithInterval, WithPeriod, or WithCron is required
func WithInterval(d time.Duration) Option {
	return func(o *cronJobOptions) {
		o.interval = timeutils.Duration{Time: d}.String()
		o.scheduleSetters++
	}
}

// WithPeriod repeats the job on an ISO8601-formatted duration (e.g. "PT5M", "P1D")
// Exactly one of WithInterval, WithPeriod, or WithCron is required
func WithPeriod(iso8601 string) Option {
	return func(o *cronJobOptions) {
		o.interval = iso8601
		o.scheduleSetters++
	}
}

// WithCron repeats the job on a standard cron expression (e.g. "0 9 * * 1-5")
// Exactly one of WithInterval, WithPeriod, or WithCron is required
func WithCron(expr string) Option {
	return func(o *cronJobOptions) {
		o.cron = expr
		o.scheduleSetters++
	}
}

// WithJob sets the function executed on each occurrence
// It is required
func WithJob(fn func(ctx context.Context) error) Option {
	return func(o *cronJobOptions) {
		o.job = fn
	}
}

// WithImmediate also runs the job once right away, but only the first time the actor is registered
func WithImmediate() Option {
	return func(o *cronJobOptions) {
		o.immediate = true
	}
}

// WithLogger sets an optional logger used to report job registration and run events
func WithLogger(logger *slog.Logger) Option {
	return func(o *cronJobOptions) {
		o.logger = logger
	}
}
