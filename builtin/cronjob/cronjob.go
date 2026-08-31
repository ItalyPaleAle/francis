// Package cronjob provides a built-in actor that runs a function on a schedule, cluster-wide on one node at a time
//
// Build one with New and register the result on a host with the host's RegisterBuiltInActor method
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
	"math/rand/v2"
	"strconv"
	"time"
	"uuid"

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

	// methodRun delivers scheduled occurrences to the user-supplied job
	methodRun = "run"
	// methodScheduledRun delivers one occurrence of a jittered schedule to the runner
	methodScheduledRun = "scheduled-run"
	// methodPlanScheduledRun asks the scheduler to plan the occurrence after the one in the payload
	methodPlanScheduledRun = "plan-scheduled-run"
	// methodTrigger asks the scheduler to dispatch a one-shot immediate run, and backs CronJob.Trigger
	methodTrigger = "trigger"

	// runJobIdempotencyKey keys the recurring job so re-dispatch (e.g. on a retried registration) returns the same job rather than creating a duplicate
	runJobIdempotencyKey = "run"
	// firstScheduledRunKeyPrefix namespaces the occurrence that starts each jittered chain
	firstScheduledRunKeyPrefix = "run-scheduled-"
	// scheduledRunKeyPrefix namespaces every later occurrence of a jittered chain, keyed by the occurrence that planned it so that a re-delivered occurrence plans its successor only once
	scheduledRunKeyPrefix = "run-after-"
	// immediateJobIdempotencyKey keys the one-shot immediate occurrence used by WithImmediate when the recurring job does not itself run at registration time
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
// Register the returned value on a host with the host's RegisterBuiltInActor method
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

	if o.jitter < 0 {
		return nil, errors.New("jitter must not be negative")
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

		// Jitter must be less than half of the period
		if o.jitter > 0 {
			// Calendar-aware intervals (months, years) have no fixed length, so they are measured from now, which is enough for a sanity check on a value this far from the boundary
			now := time.Now()
			period := now.Add(d.Time).AddDate(d.Years, d.Months, d.Days).Sub(now)
			if o.jitter >= period/2 {
				return nil, fmt.Errorf("jitter must be less than half of the interval/period (%v)", period/2)
			}
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
					// Planning on the scheduler serializes each successor with unregister without putting the user callback on the scheduler's turn
					planNext: func(ctx context.Context, payload scheduledRunPayload) error {
						_, rErr := builtinactor.Invoke(ctx, service, actorType, methodPlanScheduledRun, payload)
						return rErr
					},
					log: log,
				}
			}

			// A built-in actor manages itself through the privileged client, which the public client and Service would reject
			// The client constructors resolve the reserved actor type from the bare one
			return &cronJobScheduler{
				interval:  o.interval,
				cron:      o.cron,
				immediate: o.immediate,
				jitter:    o.jitter,
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

// CronJob is a built-in cron job actor, returned by New and registered on a host with RegisterBuiltInActor
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

// Singleton reports that the cron job has a cluster-wide singleton instance (the scheduler) the host bootstraps once it is ready
// The scheduler implements actor.ActorBootstrapper, setting up the recurring job idempotently, which is safe to trigger from every host
func (c *CronJob) Singleton() bool {
	return true
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
	// This is empty until registered, and stays empty for a jittered schedule, which is a chain of one-shot runs rather than one recurring job
	JobID string `json:"jobID"`
	// ChainID identifies one registration of a jittered chain so an occurrence from an older registration cannot extend the current one
	ChainID string `json:"chainID,omitempty"`
	// ChainInterval and ChainCron record the schedule a jittered chain was registered for
	ChainInterval string `json:"chainInterval,omitempty"`
	ChainCron     string `json:"chainCron,omitempty"`
}

// scheduledRunPayload travels with every occurrence of a jittered schedule
type scheduledRunPayload struct {
	// ChainID ties the occurrence to the registration that created it
	ChainID string `msgpack:"chainID"`
	// Nominal is the time this occurrence was scheduled for, before jitter was applied
	// The occurrence after it is computed from this rather than from when the run executes, so the offset drawn for one occurrence does not shift the ones that follow
	Nominal time.Time `msgpack:"nominal"`
}

// cronJobScheduler is the cluster-wide singleton that owns one recurring job for the cluster
// It implements actor.ActorBootstrapper for registration and actor.ActorInvoke for lifecycle methods and jittered-chain planning, dispatching the actual user runs to the separate runner instance
// Clients cannot invoke it directly because the Service rejects built-in actor types
type cronJobScheduler struct {
	interval  string
	cron      string
	immediate bool
	// jitter is the maximum offset applied to each occurrence's due time, and when non-zero the schedule runs as a chain of one-shot runs instead of a single recurring job
	jitter time.Duration
	// log is an instance of a logger
	log *slog.Logger
	// state persists this scheduler's recurring job ID
	state actor.Client[cronJobState]
	// runner is bound to the runner instance, where runs are dispatched and cancelled
	runner actor.Client[struct{}]
}

// Bootstrap sets up the recurring job, which the host drives once it's ready by invoking the reserved bootstrap lifecycle on the scheduler singleton
// It is idempotent and safe to trigger from every host: invocations of the singleton are serialized by its turn lock, and the register logic reconciles an already-registered job rather than duplicating it
func (a *cronJobScheduler) Bootstrap(ctx context.Context, _ actor.Envelope) error {
	return a.register(ctx)
}

// Invoke handles lifecycle requests and serialized successor planning
// Registration is not handled here because the host drives it through Bootstrap
func (a *cronJobScheduler) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case builtinactor.MethodUnregister:
		return nil, a.unregister(ctx)
	case methodPlanScheduledRun:
		return nil, a.planScheduledRun(ctx, data)
	case methodTrigger:
		return nil, a.trigger(ctx)
	default:
		// Only the framework invokes this actor, so an unknown method is a programming error
		return nil, fmt.Errorf("unknown cron job lifecycle method %q", method)
	}
}

// register sets up the schedule, or, if one is already registered, reconciles it against the configured one
// A retried registration is safe: every job it dispatches carries a stable idempotency key, so re-dispatching returns the existing job rather than creating a duplicate
func (a *cronJobScheduler) register(ctx context.Context) error {
	state, err := a.state.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}

	// A jittered schedule is a chain of one-shot runs rather than a recurring job, so it is registered and reconciled differently
	if a.jitter > 0 {
		return a.registerChain(ctx, state)
	}

	// A chain left behind by a configuration that had jitter would keep running alongside the recurring job, so it goes first
	if state.ChainInterval != "" || state.ChainCron != "" {
		if a.log != nil {
			a.log.Info("Jitter was removed; replacing the run chain with a recurring job")
		}

		// The recurring job picks the schedule up at the occurrence the chain had already committed to
		preserveDueTime, err := a.cancelChain(ctx)
		if err != nil {
			return err
		}

		return a.registerNew(ctx, preserveDueTime)
	}

	// Already registered: reconcile the existing job's schedule against the configured one rather than assuming it still matches
	if state.JobID != "" {
		return a.reconcileSchedule(ctx, state.JobID)
	}

	return a.registerNew(ctx, time.Time{})
}

// registerChain reconciles a jittered schedule, which runs as a chain of one-shot jobs: each occurrence plans the one after it, so no recurring job represents the schedule
// The chain is registered exactly as long as one of its runs is on the runner, so that is what registration looks at, and the schedule it was set up for comes from the actor's state, which is where a chain records what a recurring job would carry on its own job row
func (a *cronJobScheduler) registerChain(ctx context.Context, state cronJobState) (err error) {
	// A recurring job left behind by a configuration without jitter is replaced by the chain
	if state.JobID != "" {
		if a.log != nil {
			a.log.Info("Jitter was added; replacing the recurring job with a run chain", slog.String("oldJobID", state.JobID))
		}

		err = a.runner.CancelJob(ctx, state.JobID)
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			// A job that is already gone is fine: we still want to register the chain
			return fmt.Errorf("failed to cancel outdated cron job: %w", err)
		}
	}

	live, err := a.liveScheduledRuns(ctx)
	if err != nil {
		return err
	}

	// A live chain registered for the configured schedule is left alone, which is what makes bootstrapping from every host a no-op after the first
	if len(live) > 0 && state.JobID == "" && state.ChainID != "" && state.ChainInterval == a.interval && state.ChainCron == a.cron {
		if a.log != nil {
			a.log.Info("Cron job already registered")
		}

		return nil
	}

	// Anything still live belongs to a schedule that is no longer configured, or to a chain no state accounts for: it is cancelled so that exactly one chain remains
	// Its next occurrence is where the replacement picks up, so that changing the schedule does not reset how soon the job runs next
	preserveDueTime, err := a.cancelScheduledRuns(ctx, live)
	if err != nil {
		return err
	}

	return a.registerChainNew(ctx, preserveDueTime)
}

// registerChainNew starts a jittered chain by dispatching its first occurrence, and records the schedule it was started for
// preserveDueTime, when non-zero, is the due time the chain being replaced had already committed to: the first occurrence lands there untouched, and only the ones after it follow the new schedule
func (a *cronJobScheduler) registerChainNew(ctx context.Context, preserveDueTime time.Time) (err error) {
	// The chain never runs at registration time, so WithImmediate gets a one-shot run of its own
	if a.immediate {
		_, err = a.runner.Dispatch(ctx, methodRun, nil, actor.WithIdempotencyKey(immediateJobIdempotencyKey))
		if err != nil {
			return fmt.Errorf("failed to dispatch immediate cron job occurrence: %w", err)
		}
	}

	// A preserved due time was already spread by the jitter of the chain that planned it, so it is taken as it stands rather than offset a second time
	nominal := preserveDueTime
	jitter := time.Duration(0)
	if nominal.IsZero() {
		nominal, err = nextOccurrence(a.cron, a.interval, time.Now())
		if err != nil {
			return err
		}

		jitter = a.jitter
	}

	// Every registration gets a new identity so a delayed occurrence from a replaced chain cannot extend the replacement
	chainID := uuid.NewV7().String()
	due, err := planRun(ctx, a.runner, nominal, jitter, chainID, firstScheduledRunKeyPrefix+chainID)
	if err != nil {
		return err
	}

	// Persist the schedule rather than a job ID: the chain outlives any single one of its jobs, since each occurrence is a new job
	err = a.state.SetState(ctx, cronJobState{ChainID: chainID, ChainInterval: a.interval, ChainCron: a.cron}, nil)
	if err != nil {
		return fmt.Errorf("failed to save cron job state: %w", err)
	}

	if a.log != nil {
		a.log.Info("Cron job registered", slog.Time("dueTime", due))
	}

	return nil
}

// planScheduledRun advances a jittered chain while holding the scheduler's turn lock, which makes successor creation mutually exclusive with unregister and registration changes
func (a *cronJobScheduler) planScheduledRun(ctx context.Context, data actor.Envelope) error {
	// Decode the occurrence identity supplied by the runner
	var payload scheduledRunPayload
	if data != nil {
		err := data.Decode(&payload)
		if err != nil {
			return fmt.Errorf("%w: failed to decode scheduled cron job run: %w", actor.ErrJobPermanentFailure, err)
		}
	}
	if payload.ChainID == "" {
		return fmt.Errorf("%w: scheduled cron job run is missing its chain ID", actor.ErrJobPermanentFailure)
	}
	if payload.Nominal.IsZero() {
		return fmt.Errorf("%w: scheduled cron job run is missing the occurrence it belongs to", actor.ErrJobPermanentFailure)
	}

	// Ignore work from a chain that unregister or a later registration has superseded
	state, err := a.state.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read cron job state: %w", err)
	}
	if state.ChainID != payload.ChainID {
		if a.log != nil {
			a.log.Debug("Skipped planning from an inactive cron job chain", slog.String("chainID", payload.ChainID))
		}
		return nil
	}

	// Advance from the nominal occurrence so jitter on one run does not shift the rest of the schedule
	nominal, err := nextOccurrence(a.cron, a.interval, payload.Nominal)
	if err != nil {
		// The schedule is validated in New
		return fmt.Errorf("%w: %w", actor.ErrJobPermanentFailure, err)
	}

	// Skip missed occurrences so a delayed run resumes the schedule instead of chasing an outage
	now := time.Now()
	if !nominal.After(now) {
		nominal, err = nextOccurrence(a.cron, a.interval, now)
		if err != nil {
			return fmt.Errorf("%w: %w", actor.ErrJobPermanentFailure, err)
		}
	}

	// The chain and source occurrence together make retries idempotent without colliding with another registration
	key := scheduledRunKeyPrefix + payload.ChainID + "-" + strconv.FormatInt(payload.Nominal.UnixMilli(), 10)
	due, err := planRun(ctx, a.runner, nominal, a.jitter, payload.ChainID, key)
	if err != nil {
		return err
	}

	if a.log != nil {
		a.log.Debug("Planned next cron job run", slog.Time("dueTime", due))
	}

	return nil
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
			firstDue, err := nextOccurrence("", a.interval, time.Now())
			if err != nil {
				return nil, err
			}

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

	// A jittered schedule has no recurring job to cancel: it lives on for as long as one of its occurrences is on the runner, so those are what stop it
	if a.jitter > 0 {
		_, err = a.cancelChain(ctx)
		if err != nil {
			return err
		}
	}

	err = a.state.DeleteState(ctx)
	if err != nil && !errors.Is(err, actor.ErrStateNotFound) {
		// Treat missing state as already-clean
		return fmt.Errorf("failed to clear cron job state: %w", err)
	}

	return nil
}

// liveScheduledRuns returns the occurrences of a jittered chain that are still on the runner, whether waiting, leased, or executing
// Every occurrence dispatches its successor as soon as it starts, so one of these exists for as long as the chain is alive, which is how registration tells a live chain from one that ended
// A run that is only ever dispatched on its own (WithImmediate, or a trigger) is not part of the chain and is deliberately not counted
func (a *cronJobScheduler) liveScheduledRuns(ctx context.Context) ([]actor.JobInfo, error) {
	jobs, err := a.runner.ListJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list cron job runs: %w", err)
	}

	live := make([]actor.JobInfo, 0, 2)
	for _, job := range jobs {
		if job.Method == methodScheduledRun && job.Status != actor.JobStatusDeadLettered {
			live = append(live, job)
		}
	}

	return live, nil
}

// cancelChain ends a jittered schedule by cancelling the occurrences it still has on the runner, and returns the earliest due time it cancelled
func (a *cronJobScheduler) cancelChain(ctx context.Context) (time.Time, error) {
	live, err := a.liveScheduledRuns(ctx)
	if err != nil {
		return time.Time{}, err
	}

	return a.cancelScheduledRuns(ctx, live)
}

// cancelScheduledRuns cancels the given occurrences of a chain, returning the earliest due time among them so a replacement can pick the schedule up where this one left it
// Occurrences are cancelled whatever their status
// Successor planning holds this scheduler's turn lock, so a concurrent planner either finishes before this snapshot or observes the cleared or replaced chain afterward
func (a *cronJobScheduler) cancelScheduledRuns(ctx context.Context, live []actor.JobInfo) (earliest time.Time, err error) {
	now := time.Now()

	for _, job := range live {
		// Only an occurrence still ahead of us says anything about where the schedule was going: one that is due in the past is being executed right now
		if job.DueTime.After(now) && (earliest.IsZero() || job.DueTime.Before(earliest)) {
			earliest = job.DueTime
		}

		err = a.runner.CancelJob(ctx, job.JobID)
		if err != nil && !errors.Is(err, actor.ErrJobNotFound) {
			// A job that is already gone is fine: the end state (that occurrence not running) is what we want
			return time.Time{}, fmt.Errorf("failed to cancel scheduled cron job run: %w", err)
		}
	}

	return earliest, nil
}

// cronJobRunner is the worker half of a cron job: a separate actor instance that runs the user function
// It implements actor.ActorJob, and each occurrence (scheduled, immediate, or triggered) arrives as a durable run job
// Splitting it from the scheduler gives the two independent turn locks, so a long-running job does not block the scheduler's lifecycle invocations
type cronJobRunner struct {
	job func(ctx context.Context) error
	// planNext asks the scheduler to advance a jittered chain while holding its turn lock
	planNext func(ctx context.Context, payload scheduledRunPayload) error
	// log reports run start/completion events
	log *slog.Logger
}

// Job handles each occurrence delivered to the runner:
func (a *cronJobRunner) Job(ctx context.Context, method string, data actor.Envelope) error {
	switch method {
	case methodRun:
		// Executes the user function
		return a.run(ctx)
	case methodScheduledRun:
		// Executes the user function and carries the schedule forward (when jitter is configured)
		return a.scheduledRun(ctx, data)
	default:
		// An unknown method is a programming error, so dead-letter it rather than retry forever
		return fmt.Errorf("%w: unknown cron job method %q", actor.ErrJobPermanentFailure, method)
	}
}

// scheduledRun executes one occurrence of a jittered schedule and keeps the schedule going by planning the occurrence that follows it
// The successor is planned before the user function runs, so an occurrence that fails, however many times it is retried and even if it ends up dead-lettered, cannot take the rest of the schedule with it
func (a *cronJobRunner) scheduledRun(ctx context.Context, data actor.Envelope) error {
	// Every occurrence carries the time it was scheduled for, which is what the next one is computed from
	var payload scheduledRunPayload
	if data != nil {
		err := data.Decode(&payload)
		if err != nil {
			// Nothing about a payload that cannot be read is going to change on a retry
			return fmt.Errorf("%w: failed to decode scheduled cron job run: %w", actor.ErrJobPermanentFailure, err)
		}
	}
	if payload.Nominal.IsZero() {
		return fmt.Errorf("%w: scheduled cron job run is missing the occurrence it belongs to", actor.ErrJobPermanentFailure)
	}
	if payload.ChainID == "" {
		return fmt.Errorf("%w: scheduled cron job run is missing its chain ID", actor.ErrJobPermanentFailure)
	}

	// Plan first: a run that executed but failed to hand the schedule on would end the schedule there
	if a.planNext == nil {
		return fmt.Errorf("%w: scheduled cron job runner has no planner", actor.ErrJobPermanentFailure)
	}
	err := a.planNext(ctx, payload)
	if err != nil {
		// The user function has not run yet, so the retry executes both steps and nothing runs twice
		return err
	}

	return a.run(ctx)
}

// run executes the user function for a single occurrence (from the schedule, WithImmediate, or an explicit trigger)
func (a *cronJobRunner) run(ctx context.Context) error {
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

// planRun dispatches one occurrence of a jittered schedule, due at its nominal time offset by the given jitter, and returns the due time it landed on
// The occurrence carries its own nominal time so that the run it delivers can compute the one after it, and the caller supplies an idempotency key that identifies the occurrence, so dispatching it twice yields a single run
func planRun(ctx context.Context, runner actor.Client[struct{}], nominal time.Time, jitter time.Duration, chainID string, key string) (time.Time, error) {
	due := jitterDueTime(nominal, jitter)

	_, err := runner.Dispatch(ctx, methodScheduledRun, scheduledRunPayload{ChainID: chainID, Nominal: nominal},
		actor.WithJobDueTime(due),
		actor.WithIdempotencyKey(key),
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to plan cron job run: %w", err)
	}

	return due, nil
}

// jitterDueTime offsets t by a random amount in the range +/- jitter, which is what keeps cron jobs sharing a schedule from all firing at the same instant
func jitterDueTime(t time.Time, jitter time.Duration) time.Time {
	if jitter <= 0 {
		return t
	}

	// #nosec G404 -- not security-sensitive
	return t.Add(rand.N(2*jitter) - jitter)
}

// nextOccurrence returns the time the given schedule is next due after from, before any jitter is applied
// It mirrors how the framework advances a repeating job, so the occurrences planned here line up with the recurring job's own ticks
func nextOccurrence(cronExpr string, interval string, from time.Time) (time.Time, error) {
	switch {
	case cronExpr != "":
		sched, err := cron.ParseStandard(cronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
		}

		return sched.Next(from), nil
	case interval != "":
		d, err := timeutils.ParseISO8601Duration(interval)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid interval: %w", err)
		}

		return from.Add(d.Time).AddDate(d.Years, d.Months, d.Days), nil
	default:
		return time.Time{}, errors.New("no schedule configured")
	}
}
