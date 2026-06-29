// Package cronjob provides a built-in actor that runs a function on a schedule, cluster-wide on one node at a time
//
// Build one with New and pass the result to a host via the host's WithBuiltInActor option
// The actor registers a single durable recurring job, so the function runs once per occurrence across the whole cluster rather than once per host
package cronjob

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
	timeutils "github.com/italypaleale/francis/internal/time"
)

const (
	// cronJobActorTypePrefix namespaces cron job actor types under the reserved built-in prefix
	cronJobActorTypePrefix = ref.BuiltInActorTypePrefix + "cronjob."

	// methodRun delivers each scheduled occurrence to the user-supplied job
	methodRun = "run"

	// runJobIdempotencyKey keys the recurring job so re-dispatch (e.g. on a retried registration) returns the same job rather than creating a duplicate
	runJobIdempotencyKey = "run"
	// immediateJobIdempotencyKey keys the one-shot immediate occurrence used by WithImmediate with a cron schedule
	immediateJobIdempotencyKey = "run-immediate"

	// cronJobIdleTimeout keeps the singleton from lingering between occurrences
	cronJobIdleTimeout = time.Minute
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

// New builds a cron job built-in actor identified by name
//
// It registers a single durable recurring job that runs the function from WithJob across the cluster on one node at a time, on the schedule given by exactly one of WithInterval, WithPeriod, or WithCron
// When WithImmediate is set, the job also runs once right away, but only the first time it is registered
//
// Pass the returned value to a host via the host's WithBuiltInActor option
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*builtin.BuiltInActor, error) {
	if name == "" {
		return nil, errors.New("cron job name is required")
	}

	err := ref.ValidateComponents(name)
	if err != nil {
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
		_, err = cron.ParseStandard(o.cron)
		if err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	if o.job == nil {
		return nil, errors.New("WithJob is required")
	}

	actorType := cronJobActorTypePrefix + name

	return builtin.NewBuiltInActor(
		actorType,
		func(actorID string, service *actor.Service) actor.Actor {
			return &cronJobActor{
				interval:  o.interval,
				cron:      o.cron,
				immediate: o.immediate,
				job:       o.job,
				// A built-in actor manages itself through the privileged client, which the public client and Service would reject
				client: actor.NewBuiltInActorClient[cronJobState](builtinkey.Key{}, actorType, actorID, service),
			}
		},
		actorcore.RegisterActorOptions{
			IdleTimeout: cronJobIdleTimeout,
		},
	), nil
}

// cronJobState is the persisted state of a cron job actor
type cronJobState struct {
	// JobID is the ID of the recurring job registered by the actor
	// This is empty until registered
	JobID string `json:"jobID"`
}

// cronJobActor is the singleton actor that owns one recurring job for the cluster
// It implements actor.ActorInvoke for the register and unregister lifecycle, and actor.ActorJob for each scheduled occurrence
// Clients cannot invoke it directly because the Service rejects built-in actor types
type cronJobActor struct {
	interval  string
	cron      string
	immediate bool
	job       func(ctx context.Context) error
	client    actor.Client[cronJobState]
}

// Invoke handles the register and unregister lifecycle methods, which the framework drives synchronously
func (a *cronJobActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	switch method {
	case builtin.MethodRegister:
		return nil, a.register(ctx)
	case builtin.MethodUnregister:
		return nil, a.unregister(ctx)
	default:
		// Only the framework invokes this actor, so an unknown method is a programming error
		return nil, fmt.Errorf("unknown cron job lifecycle method %q", method)
	}
}

// Job handles each scheduled occurrence, delivered by the recurring run job
func (a *cronJobActor) Job(ctx context.Context, method string, _ actor.Envelope) error {
	if method != methodRun {
		// Only the recurring job dispatches to this actor
		// An unknown method is a programming error, so dead-letter it rather than retry forever
		return fmt.Errorf("%w: unknown cron job method %q", actor.ErrJobPermanentFailure, method)
	}
	return a.job(ctx)
}

// register sets up the recurring job exactly once
// It is idempotent: once a job ID is stored it returns immediately, and the recurring job carries a stable idempotency key so a retried registration never creates a duplicate
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
	// For interval/period this is folded into the recurring job's first due time
	// Cron schedules the next tick, so dispatch a one-shot occurrence now
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

// recurringJobOptions builds the dispatch options for the recurring job from the configured schedule
func (a *cronJobActor) recurringJobOptions() ([]actor.JobOption, error) {
	opts := make([]actor.JobOption, 0, 3)
	opts = append(opts,
		// The stable idempotency key makes registration safe to retry: re-dispatch returns the same job
		actor.WithIdempotencyKey(runJobIdempotencyKey),
	)

	switch {
	case a.cron != "":
		opts = append(opts, actor.WithJobCron(a.cron))
	case a.interval != "":
		opts = append(opts, actor.WithJobInterval(a.interval))

		// Without WithImmediate, delay the first occurrence by one full period so it does not run at registration time
		// When WithImmediate is set, the first occurrence defaults to now, so the job runs right away and then repeats
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

// unregister cancels the recurring job and clears state so a later register can re-register
func (a *cronJobActor) unregister(ctx context.Context) error {
	state, err := a.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	if state.JobID != "" {
		err = a.client.CancelJob(ctx, state.JobID)
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			// A job that is already gone is fine: the end state (no recurring job) is what we want
			return fmt.Errorf("failed to cancel recurring cron job: %w", err)
		}
	}

	err = a.client.DeleteState(ctx)
	if err != nil && !errors.Is(err, actor.ErrStateNotFound) {
		// Treat missing state as already-clean
		return fmt.Errorf("failed to clear cron job state: %w", err)
	}

	return nil
}
