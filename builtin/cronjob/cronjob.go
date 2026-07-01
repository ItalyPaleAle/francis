// Package cronjob provides a built-in actor that runs a function on a schedule, cluster-wide on one node at a time
//
// Build one with New and pass the result to a host via the host's WithBuiltInActor option
// The actor registers a single durable recurring job, so the function runs once per occurrence across the whole cluster rather than once per host
//
// The work is split across two actor instances of the same reserved type, each with its own turn lock:
//   - a scheduler (the cluster-wide singleton) that owns registration and answers the register, unregister, and trigger lifecycle invocations
//   - a runner that actually executes the user function for each occurrence delivered as a durable job
//
// Keeping them separate means a long-running job, which holds the runner's turn for its whole duration, never blocks the scheduler's lifecycle invocations
// CronJob.Trigger requests a one-shot immediate run, and repeated triggers while a run is still pending collapse into a single run
package cronjob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
	timeutils "github.com/italypaleale/francis/internal/time"
)

const (
	// cronJobActorTypePrefix namespaces cron job actor types within the cron job's own bare type space
	// The reserved built-in prefix is added by the host when registering, so it is not included here
	cronJobActorTypePrefix = "cronjob."

	// runnerActorID is the fixed actor ID of the runner half, distinct from the scheduler singleton so the two have independent turn locks
	// A run holds the runner's turn for its whole duration, so keeping it off the scheduler keeps lifecycle invocations responsive
	runnerActorID = "runner"

	// methodRun delivers each scheduled occurrence to the user-supplied job
	methodRun = "run"
	// methodTrigger asks the scheduler to dispatch a one-shot immediate run, and backs CronJob.Trigger
	methodTrigger = "trigger"

	// runJobIdempotencyKey keys the recurring job so re-dispatch (e.g. on a retried registration) returns the same job rather than creating a duplicate
	runJobIdempotencyKey = "run"
	// immediateJobIdempotencyKey keys the one-shot immediate occurrence used by WithImmediate with a cron schedule
	immediateJobIdempotencyKey = "run-immediate"
	// triggerJobIdempotencyKey keys the one-shot run dispatched by a manual trigger, so repeated triggers while one run is still pending collapse into a single run
	triggerJobIdempotencyKey = "run-trigger"

	// cronJobIdleTimeout keeps the singleton from lingering between occurrences
	cronJobIdleTimeout = time.Minute
)

// New builds a cron job built-in actor identified by name
//
// It registers a single durable recurring job that runs the function from WithJob across the cluster on one node at a time, on the schedule given by exactly one of WithInterval, WithPeriod, or WithCron
// When WithImmediate is set, the job also runs once right away, but only the first time it is registered
//
// Pass the returned value to a host via the host's WithBuiltInActor option
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*CronJob, error) {
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

	log := o.logger
	if log != nil {
		// Set the job name in the logger
		log = log.With(slog.String("cronJob", name))
	}

	actorType := cronJobActorTypePrefix + name

	return &CronJob{
		actorType: actorType,
		factory: func(actorID string, service *actor.Service) actor.Actor {
			// The same reserved type backs two instances: the runner executes the job, every other ID is the scheduler singleton
			if actorID == runnerActorID {
				return &cronJobRunner{
					job: o.job,
					log: log,
				}
			}

			// A built-in actor manages itself through the privileged client, which the public client and Service would reject
			// The client constructors resolve the reserved actor type from the bare one
			return &cronJobScheduler{
				interval:  o.interval,
				cron:      o.cron,
				immediate: o.immediate,
				log:       log,
				// state persists this scheduler's own recurring job ID
				state: builtinactor.NewClient[cronJobState](actorType, actorID, service),
				// runner is bound to the runner instance, so the scheduler dispatches and cancels runs there rather than on itself
				runner: builtinactor.NewClient[struct{}](actorType, runnerActorID, service),
			}
		},
		regOpts: actorcore.RegisterActorOptions{
			IdleTimeout: cronJobIdleTimeout,
		},
	}, nil
}

// CronJob is a built-in cron job actor, returned by New and passed to a host via WithBuiltInActor
// It satisfies the framework's built-in actor contract and exposes a Service method for the on-demand Trigger and Unregister operations
// The actor behavior itself lives in the unexported cronJobScheduler and cronJobRunner instances that Factory builds
type CronJob struct {
	actorType string
	factory   actor.Factory
	regOpts   actorcore.RegisterActorOptions
}

// ActorType returns the reserved actor type registered for this cron job
func (c *CronJob) ActorType() string {
	return c.actorType
}

// Factory returns the actor factory the host registers
func (c *CronJob) Factory() actor.Factory {
	return c.factory
}

// RegisterOptions returns the registration options the host uses to register the actor
func (c *CronJob) RegisterOptions() actorcore.RegisterActorOptions {
	return c.regOpts
}

// Bootstrap sets up the recurring job by invoking the scheduler's one-time registration
// The host calls this once it is ready
// It is idempotent and safe to call from every host
func (c *CronJob) Bootstrap(ctx context.Context, svc *actor.Service) error {
	_, err := builtinactor.Invoke(ctx, svc, c.actorType, builtinactor.MethodRegister, nil)
	return err
}

// Service binds the cron job to an actor.Service, returning a CronJobService that exposes the on-demand Trigger and Unregister operations pre-configured for that service
// Obtain the service from a host with host.Service()
func (c *CronJob) Service(svc *actor.Service) *CronJobService {
	return &CronJobService{
		actorType: c.actorType,
		svc:       svc,
	}
}

// CronJobService exposes the on-demand operations of a cron job (Trigger and Unregister), bound to a specific actor.Service
// Obtain one from CronJob.Service
type CronJobService struct {
	actorType string
	svc       *actor.Service
}

// Unregister cancels the recurring job and clears the actor's state, so a later Bootstrap re-registers it cleanly
func (s *CronJobService) Unregister(ctx context.Context) error {
	_, err := builtinactor.Invoke(ctx, s.svc, s.actorType, builtinactor.MethodUnregister, nil)
	return err
}

// Trigger runs the job once, immediately, regardless of the schedule
// The run happens on the runner, so this returns promptly even while a previous run is still going, and repeated triggers while a run is still pending collapse into a single run
func (s *CronJobService) Trigger(ctx context.Context) error {
	_, err := builtinactor.Invoke(ctx, s.svc, s.actorType, methodTrigger, nil)
	return err
}

// cronJobState is the persisted state of a cron job actor
type cronJobState struct {
	// JobID is the ID of the recurring job registered by the actor
	// This is empty until registered
	JobID string `json:"jobID"`
}

// cronJobScheduler is the cluster-wide singleton that owns one recurring job for the cluster
// It implements actor.ActorInvoke for the register, unregister, and trigger lifecycle methods, dispatching the actual runs to the separate runner instance
// Clients cannot invoke it directly because the Service rejects built-in actor types
type cronJobScheduler struct {
	interval  string
	cron      string
	immediate bool
	// log is an instance of a logger
	log *slog.Logger
	// state persists this scheduler's recurring job ID
	state actor.Client[cronJobState]
	// runner is bound to the runner instance, where runs are dispatched and cancelled
	runner actor.Client[struct{}]
}

// Invoke handles the register, unregister, and trigger lifecycle methods, which the framework drives synchronously
func (a *cronJobScheduler) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	switch method {
	case builtinactor.MethodRegister:
		return nil, a.register(ctx)
	case builtinactor.MethodUnregister:
		return nil, a.unregister(ctx)
	case methodTrigger:
		return nil, a.trigger(ctx)
	default:
		// Only the framework invokes this actor, so an unknown method is a programming error
		return nil, fmt.Errorf("unknown cron job lifecycle method %q", method)
	}
}

// register sets up the recurring job, or, if one is already registered, reconciles it against the configured schedule
// A retried registration is safe: the recurring job carries a stable idempotency key so re-dispatching it never creates a duplicate
func (a *cronJobScheduler) register(ctx context.Context) error {
	state, err := a.state.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	// Already registered: reconcile the existing job's schedule against the configured one rather than assuming it still matches
	if state.JobID != "" {
		return a.reconcileSchedule(ctx, state.JobID)
	}

	return a.registerNew(ctx, time.Time{})
}

// reconcileSchedule loads the already-registered job and compares its schedule to the one currently configured
// A mismatch (e.g. the cron expression or interval was changed in code since the job was first registered) replaces the job: the old one is cancelled and a new one is registered in its place, running immediately too when WithImmediate is set
// A missing job (e.g. cancelled outside the framework) is treated the same as never having registered
func (a *cronJobScheduler) reconcileSchedule(ctx context.Context, jobID string) error {
	job, err := a.runner.GetJob(ctx, jobID)
	if errors.Is(err, actor.ErrJobNotFound) {
		if a.log != nil {
			a.log.Warn("Registered cron job is missing; re-registering", slog.String("jobID", jobID))
		}

		return a.registerNew(ctx, time.Time{})
	} else if err != nil {
		return fmt.Errorf("failed to load registered cron job: %w", err)
	}

	if job.Cron == a.cron && job.Interval == a.interval {
		if a.log != nil {
			a.log.Info("Cron job already registered", slog.String("jobID", jobID))
		}

		return nil
	}

	if a.log != nil {
		a.log.Info("Cron job schedule changed; replacing recurring job", slog.String("oldJobID", jobID))
	}

	err = a.runner.CancelJob(ctx, jobID)
	if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
		// A job that is already gone is fine: we still want to register the new schedule
		return fmt.Errorf("failed to cancel outdated cron job: %w", err)
	}

	// Keep the next occurrence at the time the old schedule already had it due for, rather than resetting the clock from now
	// Only the recurrence going forward picks up the new schedule
	return a.registerNew(ctx, job.DueTime)
}

// registerNew dispatches the recurring job for the configured schedule and persists its ID, replacing whatever was previously stored
// When WithImmediate is set, it also runs once right away: folded into the recurring job's first due time for interval/period, or as a separate one-shot occurrence for cron, which schedules its own next tick
// preserveDueTime, when non-zero, pins the first occurrence to that time instead of letting the configured schedule compute a fresh one - used when replacing a job whose schedule changed, so the replacement does not reset how soon the next run happens
// It is ignored when WithImmediate is set, since immediate execution takes priority
func (a *cronJobScheduler) registerNew(ctx context.Context, preserveDueTime time.Time) error {
	if a.immediate && a.cron != "" {
		_, err := a.runner.Dispatch(ctx, methodRun, nil, actor.WithIdempotencyKey(immediateJobIdempotencyKey))
		if err != nil {
			return fmt.Errorf("failed to dispatch immediate cron job occurrence: %w", err)
		}
	}

	// Register the recurring job on the runner
	jobOpts, err := a.recurringJobOptions()
	if err != nil {
		return err
	}

	if !a.immediate && !preserveDueTime.IsZero() {
		jobOpts = append(jobOpts, actor.WithJobDueTime(preserveDueTime))
	}

	jobID, err := a.runner.Dispatch(ctx, methodRun, nil, jobOpts...)
	if err != nil {
		return fmt.Errorf("failed to register recurring cron job: %w", err)
	}

	// Persist the job ID so a duplicate register is a no-op and unregister can cancel it
	err = a.state.SetState(ctx, cronJobState{JobID: jobID}, nil)
	if err != nil {
		return fmt.Errorf("failed to save cron job state: %w", err)
	}

	if a.log != nil {
		a.log.Info("Cron job registered", slog.String("jobID", jobID))
	}

	return nil
}

// trigger dispatches a one-shot immediate run to the runner
// The fixed idempotency key collapses multiple pending triggers into a single run: while one run is still pending, further triggers return the same job instead of queuing another
func (a *cronJobScheduler) trigger(ctx context.Context) error {
	_, err := a.runner.Dispatch(ctx, methodRun, nil, actor.WithIdempotencyKey(triggerJobIdempotencyKey))
	if err != nil {
		return fmt.Errorf("failed to dispatch triggered cron job run: %w", err)
	}

	return nil
}

// recurringJobOptions builds the dispatch options for the recurring job from the configured schedule
func (a *cronJobScheduler) recurringJobOptions() ([]actor.JobOption, error) {
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

// unregister cancels the recurring job on the runner and clears state so a later register can re-register
func (a *cronJobScheduler) unregister(ctx context.Context) error {
	state, err := a.state.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	if state.JobID != "" {
		err = a.runner.CancelJob(ctx, state.JobID)
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			// A job that is already gone is fine: the end state (no recurring job) is what we want
			return fmt.Errorf("failed to cancel recurring cron job: %w", err)
		}
	}

	err = a.state.DeleteState(ctx)
	if err != nil && !errors.Is(err, actor.ErrStateNotFound) {
		// Treat missing state as already-clean
		return fmt.Errorf("failed to clear cron job state: %w", err)
	}

	return nil
}

// cronJobRunner is the worker half of a cron job: a separate actor instance that runs the user function
// It implements actor.ActorJob, and each occurrence (scheduled, immediate, or triggered) arrives as a durable run job
// Splitting it from the scheduler gives the two independent turn locks, so a long-running job does not block the scheduler's lifecycle invocations
type cronJobRunner struct {
	job func(ctx context.Context) error
	// log reports run start/completion events
	log *slog.Logger
}

// Job runs the user function for each occurrence delivered by the recurring schedule or an explicit trigger
func (a *cronJobRunner) Job(ctx context.Context, method string, _ actor.Envelope) error {
	if method != methodRun {
		// Only run jobs are dispatched to this actor
		// An unknown method is a programming error, so dead-letter it rather than retry forever
		return fmt.Errorf("%w: unknown cron job method %q", actor.ErrJobPermanentFailure, method)
	}

	if a.log != nil {
		a.log.Info("Cron job run started")
	}
	start := time.Now()

	err := a.job(ctx)

	duration := time.Since(start)
	if err != nil {
		if a.log != nil {
			a.log.Warn("Cron job run completed with error", slog.Duration("duration", duration), slog.Any("error", err))
		}
		return fmt.Errorf("error running job: %w", err)
	}

	if a.log != nil {
		a.log.Info("Cron job run completed", slog.Duration("duration", duration))
	}

	return nil
}
