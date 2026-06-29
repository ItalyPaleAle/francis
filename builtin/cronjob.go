package builtin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	timeutils "github.com/italypaleale/francis/internal/time"
)

const (
	// cronJobActorTypePrefix namespaces cron job actor types under the reserved built-in prefix.
	cronJobActorTypePrefix = ref.BuiltInActorTypePrefix + "cronjob."

	// runJobIdempotencyKey keys the recurring job so re-dispatch (e.g. on a retried registration) returns the same job rather than creating a duplicate.
	runJobIdempotencyKey = "run"
	// immediateJobIdempotencyKey keys the one-shot immediate occurrence used by WithImmediate with a cron schedule.
	immediateJobIdempotencyKey = "run-immediate"

	// cronJobIdleTimeout keeps the singleton from lingering between occurrences; a job reactivates it when due.
	cronJobIdleTimeout = time.Minute
)

// cronJobOptions accumulates the configuration applied by the CronJobOption builders.
type cronJobOptions struct {
	// interval is the repetition as an ISO8601 duration string (from WithInterval or WithPeriod)
	interval string
	// cron is a standard cron expression (from WithCron)
	cron string
	// immediate runs the job once right away on first registration
	immediate bool
	// job is the function executed on each occurrence
	job func(ctx context.Context) error
	// scheduleSetters counts how many of WithInterval/WithPeriod/WithCron were applied, to enforce "exactly one"
	scheduleSetters int
}

// CronJobOption configures a cron job actor.
type CronJobOption func(*cronJobOptions)

// WithInterval repeats the job on a fixed interval.
// Exactly one of WithInterval, WithPeriod, or WithCron is required.
func WithInterval(d time.Duration) CronJobOption {
	return func(o *cronJobOptions) {
		o.interval = timeutils.Duration{Time: d}.String()
		o.scheduleSetters++
	}
}

// WithPeriod repeats the job on an ISO8601-formatted duration (e.g. "PT5M", "P1D").
// Exactly one of WithInterval, WithPeriod, or WithCron is required.
func WithPeriod(iso8601 string) CronJobOption {
	return func(o *cronJobOptions) {
		o.interval = iso8601
		o.scheduleSetters++
	}
}

// WithCron repeats the job on a standard cron expression (e.g. "0 9 * * 1-5").
// Exactly one of WithInterval, WithPeriod, or WithCron is required.
func WithCron(expr string) CronJobOption {
	return func(o *cronJobOptions) {
		o.cron = expr
		o.scheduleSetters++
	}
}

// WithJob sets the function executed on each occurrence. It is required.
func WithJob(fn func(ctx context.Context) error) CronJobOption {
	return func(o *cronJobOptions) {
		o.job = fn
	}
}

// WithImmediate also runs the job once right away, but only the first time the actor is registered.
func WithImmediate() CronJobOption {
	return func(o *cronJobOptions) {
		o.immediate = true
	}
}

// NewCronJobActor builds a cron job built-in actor identified by name.
//
// It registers a single durable recurring job that runs the function from WithJob across the cluster
// on one node at a time, on the schedule given by exactly one of WithInterval, WithPeriod, or WithCron.
// With WithImmediate the job also runs once right away, but only the first time it is registered.
//
// Pass the returned value to a host via WithBuiltInActor. Names must be unique within a cluster and
// must not contain '/'.
func NewCronJobActor(name string, opts ...CronJobOption) (*BuiltInActor, error) {
	if name == "" {
		return nil, errors.New("cron job name is required")
	}
	if err := ref.ValidateComponents(name); err != nil {
		return nil, fmt.Errorf("invalid cron job name: %w", err)
	}

	var o cronJobOptions
	for _, opt := range opts {
		opt(&o)
	}

	// Exactly one schedule must be configured
	if o.scheduleSetters != 1 {
		return nil, errors.New("exactly one of WithInterval, WithPeriod, or WithCron is required")
	}

	// Validate the configured schedule
	switch {
	case o.interval != "":
		d, err := timeutils.ParseISO8601Duration(o.interval)
		if err != nil {
			return nil, fmt.Errorf("invalid interval/period: %w", err)
		}
		if d.IsZero() {
			return nil, errors.New("interval/period must be greater than zero")
		}
	case o.cron != "":
		if _, err := cron.ParseStandard(o.cron); err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	if o.job == nil {
		return nil, errors.New("WithJob is required")
	}

	actorType := cronJobActorTypePrefix + name

	return &BuiltInActor{
		actorType: actorType,
		factory: func(actorID string, service *actor.Service) actor.Actor {
			return &cronJobActor{
				interval:  o.interval,
				cron:      o.cron,
				immediate: o.immediate,
				job:       o.job,
				client:    actor.NewActorClient[cronJobState](actorType, actorID, service),
			}
		},
		regOpts: actorcore.RegisterActorOptions{
			IdleTimeout: cronJobIdleTimeout,
		},
	}, nil
}

// cronJobState is the persisted state of a cron job actor.
type cronJobState struct {
	// JobID is the ID of the recurring job registered by the actor; empty until registered
	JobID string `json:"jobID"`
}

// cronJobActor is the singleton actor that owns one recurring job for the cluster.
// It implements actor.ActorJob and deliberately does not implement actor.ActorInvoke, so it cannot be invoked.
type cronJobActor struct {
	interval  string
	cron      string
	immediate bool
	job       func(ctx context.Context) error
	client    actor.Client[cronJobState]
}

// Job handles the register/run/unregister occurrences delivered to this actor.
func (a *cronJobActor) Job(ctx context.Context, method string, _ actor.Envelope) error {
	switch method {
	case methodRegister:
		return a.register(ctx)
	case methodRun:
		return a.job(ctx)
	case methodUnregister:
		return a.unregister(ctx)
	default:
		// Only the framework dispatches to this actor; an unknown method is a programming error, so dead-letter it rather than retry forever
		return fmt.Errorf("%w: unknown cron job method %q", actor.ErrJobPermanentFailure, method)
	}
}

// register sets up the recurring job exactly once.
// It is idempotent: once a job ID is stored it returns immediately, and the recurring job carries a stable idempotency key so a retried registration never creates a duplicate.
func (a *cronJobActor) register(ctx context.Context) error {
	state, err := a.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	// Already registered: nothing to do
	if state.JobID != "" {
		return nil
	}

	// On first registration with WithImmediate, run once right away
	// For interval/period this is folded into the recurring job's first due time; cron schedules the next tick, so dispatch a one-shot occurrence now
	if a.immediate && a.cron != "" {
		_, err = a.client.Dispatch(ctx, methodRun, nil, actor.WithIdempotencyKey(immediateJobIdempotencyKey))
		if err != nil {
			return fmt.Errorf("failed to dispatch immediate cron job occurrence: %w", err)
		}
	}

	// Register the recurring job
	jobOpts, err := a.recurringJobOptions()
	if err != nil {
		return err
	}
	jobID, err := a.client.Dispatch(ctx, methodRun, nil, jobOpts...)
	if err != nil {
		return fmt.Errorf("failed to register recurring cron job: %w", err)
	}

	// Persist the job ID so a duplicate register is a no-op and unregister can cancel it
	state.JobID = jobID
	err = a.client.SetState(ctx, state, nil)
	if err != nil {
		return fmt.Errorf("failed to save cron job state: %w", err)
	}

	return nil
}

// recurringJobOptions builds the dispatch options for the recurring job from the configured schedule.
func (a *cronJobActor) recurringJobOptions() ([]actor.JobOption, error) {
	// The stable idempotency key makes registration safe to retry: re-dispatch returns the same job
	opts := []actor.JobOption{actor.WithIdempotencyKey(runJobIdempotencyKey)}

	switch {
	case a.cron != "":
		opts = append(opts, actor.WithJobCron(a.cron))
	case a.interval != "":
		opts = append(opts, actor.WithJobInterval(a.interval))
		// Without WithImmediate, delay the first occurrence by one full period so it does not run at registration time
		// With WithImmediate the first occurrence defaults to now, so the job runs right away and then repeats
		if !a.immediate {
			d, err := timeutils.ParseISO8601Duration(a.interval)
			if err != nil {
				return nil, fmt.Errorf("invalid interval: %w", err)
			}
			firstDue := time.Now().Add(d.Time).AddDate(d.Years, d.Months, d.Days)
			opts = append(opts, actor.WithJobDueTime(firstDue))
		}
	default:
		return nil, errors.New("no schedule configured")
	}

	return opts, nil
}

// unregister cancels the recurring job and clears state so a later register can re-register.
func (a *cronJobActor) unregister(ctx context.Context) error {
	state, err := a.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	if state.JobID != "" {
		err = a.client.CancelJob(ctx, state.JobID)
		// A job that is already gone is fine: the end state (no recurring job) is what we want
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			return fmt.Errorf("failed to cancel recurring cron job: %w", err)
		}
	}

	err = a.client.DeleteState(ctx)
	// Treat missing state as already-clean
	if err != nil && !errors.Is(err, actor.ErrStateNotFound) {
		return fmt.Errorf("failed to clear cron job state: %w", err)
	}

	return nil
}
