//go:build integration

// Package cron exercises the built-in cron job actor end to end:
//
//   - it runs the user-supplied job on its schedule, cluster-wide on a single node
//   - it registers exactly one recurring job, even when multiple hosts bootstrap it
//   - clients cannot invoke a built-in actor directly
//   - Unregister cancels the recurring job and stops further executions
package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/cronjob"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

const (
	// pollInterval keeps job polling fast so occurrences fire promptly instead of waiting on the multi-second default
	pollInterval = 250 * time.Millisecond
	// cronInterval is the cron job's repetition period, kept short so the scenario observes several occurrences quickly
	cronInterval = 300 * time.Millisecond
	// cronJitter spreads the jittered job's occurrences
	cronJitter = 100 * time.Millisecond

	// singletonActorID mirrors the fixed actor ID built-in singletons use, needed to query the scheduler half
	singletonActorID = "singleton"
	// runnerActorID mirrors the fixed actor ID of the cron job's runner half, where the recurring run job actually lives
	runnerActorID = "runner"

	eventuallyTimeout = 30 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	// stabilizeWindow is how long the run count must hold steady to count as settled
	stabilizeWindow = 2 * time.Second
)

// matrix runs the scenario across representative topology/provider combinations
// Multi-host entries also prove that concurrent bootstrap from every host still registers the job exactly once
var matrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int
}{
	{cluster.Local, provider.SQLite, 2},
	{cluster.Local, provider.StandaloneMemory, 1},
	{cluster.Remote, provider.Postgres, 2},
}

func init() {
	for _, m := range matrix {
		suite.Register(&builtinCron{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}
}

// builtinCron drives a cluster whose hosts register a built-in cron job actor and asserts its behavior
type builtinCron struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster  *cluster.Cluster
	cron     *cronjob.CronJob
	cronType string
	// runs counts how many times the user job has executed, incremented from the actor's goroutine
	runs atomic.Int64

	// jitterCron is a third cron job configured with jitter, whose schedule is a chain of one-shot runs rather than a recurring job
	jitterCron *cronjob.CronJob
	// jitterType is the reserved actor type of the jittered cron job, used to inspect the jobs it registers
	jitterType string
	// jitterRuns counts executions of the jittered job
	jitterRuns atomic.Int64

	// trigCron is a second cron job that only runs when triggered, used to exercise the trigger API and the scheduler/runner split
	trigCron *cronjob.CronJob
	// trigRuns counts executions of the triggered job
	trigRuns atomic.Int64
	// trigStarted signals that a triggered run has begun and is about to block
	trigStarted chan struct{}
	// trigRelease unblocks the in-flight triggered run so it can complete
	trigRelease chan struct{}
}

func (s *builtinCron) Name() string {
	return "builtincron/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinCron) Setup(t *testing.T) []framework.Option {
	cronActor, err := cronjob.New(
		"e2e",
		cronjob.WithInterval(cronInterval),
		cronjob.WithImmediate(),
		cronjob.WithJob(func(context.Context) error {
			s.runs.Add(1)
			return nil
		}),
	)
	require.NoError(t, err)

	s.cron = cronActor
	// The host registers the actor under the reserved prefix, so jobs and the guard use the full type
	s.cronType = builtinactor.FullActorType(cronActor.ActorType())

	// A second cron job whose interval is far in the future, so it never fires on its own and only runs when triggered
	// Its job blocks until released, so the test can hold a run in flight on the runner while it exercises the scheduler
	s.trigStarted = make(chan struct{}, 1)
	s.trigRelease = make(chan struct{})
	trigCron, err := cronjob.New(
		"e2e-trigger",
		cronjob.WithInterval(time.Hour),
		cronjob.WithJob(func(ctx context.Context) error {
			s.trigRuns.Add(1)
			select {
			case s.trigStarted <- struct{}{}:
			default:
			}
			select {
			case <-s.trigRelease:
			case <-ctx.Done():
			}
			return nil
		}),
	)
	require.NoError(t, err)
	s.trigCron = trigCron

	// A third cron job on the same schedule, but jittered: it has no recurring job, and each occurrence dispatches the next one around its scheduled time
	jitterCron, err := cronjob.New(
		"e2e-jitter",
		cronjob.WithInterval(cronInterval),
		cronjob.WithJitter(cronJitter),
		cronjob.WithJob(func(context.Context) error {
			s.jitterRuns.Add(1)
			return nil
		}),
	)
	require.NoError(t, err)
	s.jitterCron = jitterCron
	s.jitterType = builtinactor.FullActorType(jitterCron.ActorType())

	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              s.hosts,
		BuiltInActors:      []builtinactor.BuiltInActor{cronActor, trigCron, jitterCron},
		AlarmsPollInterval: pollInterval,
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *builtinCron) Run(t *testing.T) {
	ctx := t.Context()
	svc := s.cluster.Service(0)
	cronSvc := s.cron.Service(svc)
	trigSvc := s.trigCron.Service(svc)
	jitterSvc := s.jitterCron.Service(svc)

	// The job runs repeatedly on its schedule (WithImmediate means the first occurrence is right away)
	t.Run("runs on schedule", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return s.runs.Load() >= 3
		}, eventuallyTimeout, eventuallyTick, "the cron job should run repeatedly on its schedule")
	})

	// Exactly one recurring job is registered, even though every host bootstrapped it
	t.Run("registers exactly once", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return s.liveRunJobs(t) == 1
		}, eventuallyTimeout, eventuallyTick, "exactly one recurring job should be registered")
	})

	// Clients cannot target a built-in actor through the public Service, on any host
	t.Run("cannot be targeted directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			s.assertClientRejected(t, s.cluster.Service(i), i)
		}
	})

	// An explicit trigger runs the job immediately, and the long-running run does not block the scheduler's lifecycle invocations
	t.Run("trigger runs immediately without blocking lifecycle", func(t *testing.T) {
		// Release the blocked run however the subtest exits, so the host can shut down cleanly
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(s.trigRelease) }) }
		defer release()

		// The triggered cron is otherwise idle (its schedule is an hour out), so it has not run yet
		require.Zero(t, s.trigRuns.Load(), "the triggered cron must not run before it is triggered")

		err := trigSvc.Trigger(ctx)
		require.NoError(t, err)

		// The triggered run starts on the runner instance
		select {
		case <-s.trigStarted:
		case <-time.After(eventuallyTimeout):
			t.Fatal("the triggered run did not start")
		}
		assert.GreaterOrEqual(t, s.trigRuns.Load(), int64(1), "the trigger should have run the job")

		// The run is now blocked, holding the runner's turn
		// Unregister targets the separate scheduler actor, so it must return without waiting for the in-flight run
		done := make(chan error, 1)
		go func() {
			done <- trigSvc.Unregister(ctx)
		}()
		select {
		case err := <-done:
			require.NoError(t, err, "unregister must not block on the long-running run")
		case <-time.After(10 * time.Second):
			t.Fatal("unregister blocked behind the long-running job on the runner")
		}

		// Let the blocked run finish
		release()
	})

	// A jittered job runs on its schedule as a chain of one-shot jobs, each occurrence dispatching the one after it
	t.Run("jittered schedule runs on time", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return s.jitterRuns.Load() >= 3
		}, eventuallyTimeout, eventuallyTick, "the jittered cron job should run repeatedly on its schedule")

		// There is no recurring job to find: the schedule only ever exists as the occurrences that are still live, and a chain that forked would show up here as more of them
		assert.Zero(t, s.liveJobs(t, s.jitterType, "run"), "a jittered schedule registers no recurring job")
		assert.LessOrEqual(t, s.liveJobs(t, s.jitterType, "scheduled-run"), 2, "at most the occurrence running and the one it planned should be live")
	})

	// Unregistering a jittered job cancels the occurrence the chain is waiting on, which is what ends it
	t.Run("unregister stops a jittered schedule", func(t *testing.T) {
		err := jitterSvc.Unregister(ctx)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return s.liveJobs(t, s.jitterType, "scheduled-run") == 0
		}, eventuallyTimeout, eventuallyTick, "unregister should leave no occurrence of the chain behind")

		// With nothing left to carry the schedule forward, executions stop
		settled := s.settleRuns(t, &s.jitterRuns)
		time.Sleep(stabilizeWindow)
		assert.Equal(t, settled, s.jitterRuns.Load(), "no further executions after unregister")
	})

	// Unregister cancels the recurring job and stops further executions
	t.Run("unregister stops execution", func(t *testing.T) {
		err := cronSvc.Unregister(ctx)
		require.NoError(t, err)

		// The recurring job is removed
		require.Eventually(t, func() bool {
			return s.liveRunJobs(t) == 0
		}, eventuallyTimeout, eventuallyTick, "unregister should cancel the recurring job")

		// And the run count stops growing
		settled := s.settleRuns(t, &s.runs)
		time.Sleep(stabilizeWindow)
		assert.Equal(t, settled, s.runs.Load(), "no further executions after unregister")
	})
}

// liveRunJobs returns how many live (non-dead-lettered) recurring "run" jobs the runner has
// A duplicate registration would surface here as more than one
func (s *builtinCron) liveRunJobs(t *testing.T) int {
	t.Helper()
	return s.liveJobs(t, s.cronType, "run")
}

// liveJobs returns how many live (non-dead-lettered) jobs with the given method a cron job's runner has
// The scheduler dispatches every job to the runner instance, so that is where they live: one recurring "run" job for a plain schedule, or the "scheduled-run" occurrences a jittered one is made of
// It inspects through the host because the public Service rejects built-in actor types
func (s *builtinCron) liveJobs(t *testing.T, actorType string, method string) int {
	t.Helper()
	jobs, err := s.cluster.Host(0).ListJobs(t.Context(), actorType, runnerActorID)
	require.NoError(t, err)

	var n int
	for _, j := range jobs {
		if j.Status != actor.JobStatusDeadLettered && j.Method == method {
			n++
		}
	}
	return n
}

// assertClientRejected checks that every Service method targeting an actor by type rejects the built-in cron type with ErrActorTypeReserved
func (s *builtinCron) assertClientRejected(t *testing.T, svc *actor.Service, host int) {
	t.Helper()
	ctx := t.Context()

	_, invErr := svc.Invoke(ctx, s.cronType, singletonActorID, "run", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, _, streamErr := svc.InvokeStream(ctx, s.cronType, singletonActorID, "run", "", nil)
	require.ErrorIs(t, streamErr, actor.ErrActorTypeReserved, "host %d InvokeStream", host)

	setStateErr := svc.SetState(ctx, s.cronType, singletonActorID, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	var dest map[string]any
	getStateErr := svc.GetState(ctx, s.cronType, singletonActorID, &dest)
	require.ErrorIs(t, getStateErr, actor.ErrActorTypeReserved, "host %d GetState", host)

	deleteStateErr := svc.DeleteState(ctx, s.cronType, singletonActorID)
	require.ErrorIs(t, deleteStateErr, actor.ErrActorTypeReserved, "host %d DeleteState", host)

	setAlarmErr := svc.SetAlarm(ctx, s.cronType, singletonActorID, "a", actor.AlarmProperties{})
	require.ErrorIs(t, setAlarmErr, actor.ErrActorTypeReserved, "host %d SetAlarm", host)

	deleteAlarmErr := svc.DeleteAlarm(ctx, s.cronType, singletonActorID, "a")
	require.ErrorIs(t, deleteAlarmErr, actor.ErrActorTypeReserved, "host %d DeleteAlarm", host)

	_, dispatchErr := svc.Dispatch(ctx, s.cronType, singletonActorID, "run", nil)
	require.ErrorIs(t, dispatchErr, actor.ErrActorTypeReserved, "host %d Dispatch", host)

	_, listErr := svc.ListJobs(ctx, s.cronType, singletonActorID)
	require.ErrorIs(t, listErr, actor.ErrActorTypeReserved, "host %d ListJobs", host)

	cancelErr := svc.CancelJob(ctx, s.cronType, singletonActorID, "job")
	require.ErrorIs(t, cancelErr, actor.ErrActorTypeReserved, "host %d CancelJob", host)

	haltErr := svc.Halt(s.cronType, singletonActorID)
	require.ErrorIs(t, haltErr, actor.ErrActorTypeReserved, "host %d Halt", host)
}

// settleRuns waits until the given run count stops changing for a full stabilize window and returns it
func (s *builtinCron) settleRuns(t *testing.T, counter *atomic.Int64) int64 {
	t.Helper()

	last := counter.Load()
	stableSince := time.Now()
	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(eventuallyTick)
		c := counter.Load()
		if c != last {
			last = c
			stableSince = time.Now()
			continue
		}

		if time.Since(stableSince) >= stabilizeWindow {
			return last
		}
	}

	t.Fatalf("run count did not settle within %s (last %d)", eventuallyTimeout, last)

	return last
}
