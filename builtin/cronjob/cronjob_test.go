package cronjob

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

func TestNew(t *testing.T) {
	noop := func(context.Context) error { return nil }

	t.Run("valid interval", func(t *testing.T) {
		b, err := New("nightly", WithInterval(time.Minute), WithJob(noop))
		require.NoError(t, err)
		assert.Equal(t, cronJobActorTypePrefix+"nightly", b.ActorType())
		assert.NotNil(t, b.Factory())
		assert.Equal(t, cronJobIdleTimeout, b.RegisterOptions().IdleTimeout)
	})

	t.Run("valid period", func(t *testing.T) {
		b, err := New("p", WithPeriod("PT5M"), WithJob(noop))
		require.NoError(t, err)
		assert.Equal(t, cronJobActorTypePrefix+"p", b.ActorType())
	})

	t.Run("valid cron", func(t *testing.T) {
		_, err := New("c", WithCron("0 9 * * 1-5"), WithJob(noop))
		require.NoError(t, err)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := New("", WithInterval(time.Minute), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects name with slash", func(t *testing.T) {
		_, err := New("a/b", WithInterval(time.Minute), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects no schedule", func(t *testing.T) {
		_, err := New("x", WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects two schedules", func(t *testing.T) {
		_, err := New("x", WithInterval(time.Minute), WithCron("* * * * *"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects interval and period together", func(t *testing.T) {
		_, err := New("x", WithInterval(time.Minute), WithPeriod("PT5M"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects zero interval", func(t *testing.T) {
		_, err := New("x", WithInterval(0), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects invalid period", func(t *testing.T) {
		_, err := New("x", WithPeriod("not-a-duration"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects invalid cron", func(t *testing.T) {
		_, err := New("x", WithCron("not a cron"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects missing job", func(t *testing.T) {
		_, err := New("x", WithInterval(time.Minute))
		require.Error(t, err)
	})

	t.Run("valid jitter", func(t *testing.T) {
		_, err := New("j", WithInterval(time.Minute), WithJitter(10*time.Second), WithJob(noop))
		require.NoError(t, err)
	})

	t.Run("valid jitter with a cron schedule", func(t *testing.T) {
		// The gaps between cron ticks are not fixed, so the jitter is not checked against them
		_, err := New("j", WithCron("0 9 * * *"), WithJitter(time.Hour), WithJob(noop))
		require.NoError(t, err)
	})

	t.Run("rejects negative jitter", func(t *testing.T) {
		_, err := New("x", WithInterval(time.Minute), WithJitter(-time.Second), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects jitter of half the interval or more", func(t *testing.T) {
		// Windows of half the interval on either side of an occurrence would touch the neighboring ones, letting runs swap order
		_, err := New("x", WithInterval(time.Minute), WithJitter(30*time.Second), WithJob(noop))
		require.Error(t, err)

		_, err = New("x", WithPeriod("PT1M"), WithJitter(90*time.Second), WithJob(noop))
		require.Error(t, err)
	})
}

// TestFactoryRoles verifies the factory builds a scheduler for the singleton and a runner for the runner ID
func TestFactoryRoles(t *testing.T) {
	b, err := New("roles", WithInterval(time.Minute), WithJob(func(context.Context) error { return nil }))
	require.NoError(t, err)

	scheduler := b.Factory()("singleton", nil)
	_, ok := scheduler.(*cronJobScheduler)
	assert.True(t, ok, "a non-runner ID should build the scheduler")

	runner := b.Factory()(runnerActorID, nil)
	_, ok = runner.(*cronJobRunner)
	assert.True(t, ok, "the runner ID should build the runner")
}

func TestCronJobRecurringJobOptions(t *testing.T) {
	t.Run("interval, not immediate, delays first occurrence", func(t *testing.T) {
		a := &cronJobScheduler{interval: "PT1M"}
		before := time.Now()
		p := resolveJobProps(t, a)
		assert.Equal(t, "PT1M", p.Interval)
		assert.Empty(t, p.Cron)
		assert.Equal(t, runJobIdempotencyKey, p.IdempotencyKey)
		// The first occurrence is pushed out by one full period so it does not run at registration time
		assert.False(t, p.DueTime.IsZero(), "non-immediate interval should set a future due time")
		assert.WithinDuration(t, before.Add(time.Minute), p.DueTime, 5*time.Second)
	})

	t.Run("interval, immediate, first occurrence is now", func(t *testing.T) {
		a := &cronJobScheduler{interval: "PT1M", immediate: true}
		p := resolveJobProps(t, a)
		assert.Equal(t, "PT1M", p.Interval)
		assert.Equal(t, runJobIdempotencyKey, p.IdempotencyKey)
		// No explicit due time means the recurring job's first occurrence defaults to now
		assert.True(t, p.DueTime.IsZero(), "immediate interval should leave the due time unset (runs now)")
	})

	t.Run("cron schedule", func(t *testing.T) {
		a := &cronJobScheduler{cron: "0 9 * * *"}
		p := resolveJobProps(t, a)
		assert.Equal(t, "0 9 * * *", p.Cron)
		assert.Empty(t, p.Interval)
		assert.Equal(t, runJobIdempotencyKey, p.IdempotencyKey)
		assert.True(t, p.DueTime.IsZero(), "cron schedules its own next tick")
	})
}

func TestCronJobRegister(t *testing.T) {
	ctx := t.Context()

	t.Run("registers recurring job on the runner on empty state", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-1"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		// The recurring job is dispatched to the runner, not stored on the scheduler
		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, methodRun, runner.dispatches[0].method)
		assert.Equal(t, "PT1M", runner.dispatches[0].props.Interval)
		assert.Empty(t, state.dispatches, "the scheduler must not dispatch runs to itself")
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, "job-1", state.setStateCalls[0].JobID)
	})

	t.Run("is a no-op when already registered with a matching schedule", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "existing"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-2",
			getJobInfo: actor.JobInfo{Interval: "PT1M", Method: methodRun},
		}
		a := &cronJobScheduler{
			interval: "PT1M",
			state:    state,
			runner:   runner,
		}

		err := a.register(ctx)
		require.NoError(t, err)

		assert.Empty(t, runner.dispatches, "a matching schedule must not dispatch again")
		assert.Empty(t, runner.cancelled)
		assert.Empty(t, state.setStateCalls)
	})

	t.Run("replaces the job when the configured schedule changed, preserving the next due time", func(t *testing.T) {
		oldDue := time.Now().Add(3 * time.Hour)
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "existing"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-new",
			getJobInfo: actor.JobInfo{
				Interval: "PT5M",
				DueTime:  oldDue,
			},
		}
		a := &cronJobScheduler{
			interval: "PT1M",
			state:    state,
			runner:   runner,
		}

		err := a.register(ctx)
		require.NoError(t, err)

		// The outdated job is cancelled and a fresh one is dispatched with the currently configured schedule
		assert.Equal(t, []string{"existing"}, runner.cancelled)
		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, "PT1M", runner.dispatches[0].props.Interval)
		// The next occurrence stays where the old schedule already had it due, rather than resetting the clock from now
		assert.True(t, oldDue.Equal(runner.dispatches[0].props.DueTime), "expected due time %s, got %s", oldDue, runner.dispatches[0].props.DueTime)
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, "job-new", state.setStateCalls[0].JobID)
	})

	t.Run("replaces the job and runs immediately when the schedule changed and WithImmediate is set", func(t *testing.T) {
		oldDue := time.Now().Add(3 * time.Hour)
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "existing"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-new",
			getJobInfo: actor.JobInfo{
				Cron:    "0 9 * * *",
				DueTime: oldDue,
			},
		}
		a := &cronJobScheduler{
			cron:      "0 10 * * *",
			immediate: true,
			state:     state,
			runner:    runner,
		}

		err := a.register(ctx)
		require.NoError(t, err)

		assert.Equal(t, []string{"existing"}, runner.cancelled)
		require.Len(t, runner.dispatches, 2, "a replaced schedule with WithImmediate also dispatches the one-shot occurrence")
		assert.Equal(t, immediateJobIdempotencyKey, runner.dispatches[0].props.IdempotencyKey)
		assert.Equal(t, "0 10 * * *", runner.dispatches[1].props.Cron)
		// WithImmediate takes priority over preserving the old due time
		assert.True(t, runner.dispatches[1].props.DueTime.IsZero(), "immediate registration must not pin the due time to the old schedule")
	})

	t.Run("re-registers when the stored job ID no longer exists", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "gone"}}
		runner := &fakeClient[struct{}]{dispatchID: "job-new", getJobErr: actor.ErrJobNotFound}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		assert.Empty(t, runner.cancelled, "nothing to cancel when the job is already gone")
		require.Len(t, runner.dispatches, 1)
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, "job-new", state.setStateCalls[0].JobID)
	})

	t.Run("immediate cron also dispatches a one-shot occurrence to the runner", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-3"}
		a := &cronJobScheduler{cron: "0 9 * * *", immediate: true, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 2)
		// The immediate one-shot is dispatched first, with its own idempotency key and no schedule
		assert.Equal(t, immediateJobIdempotencyKey, runner.dispatches[0].props.IdempotencyKey)
		assert.Empty(t, runner.dispatches[0].props.Cron)
		assert.Empty(t, runner.dispatches[0].props.Interval)
		// The recurring cron job follows
		assert.Equal(t, "0 9 * * *", runner.dispatches[1].props.Cron)
		assert.Equal(t, runJobIdempotencyKey, runner.dispatches[1].props.IdempotencyKey)
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, "job-3", state.setStateCalls[0].JobID)
	})

	t.Run("immediate interval does not dispatch a separate one-shot", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-4"}
		a := &cronJobScheduler{interval: "PT1M", immediate: true, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 1, "interval immediacy is folded into the recurring job's first due time")
		assert.Equal(t, "PT1M", runner.dispatches[0].props.Interval)
	})
}

func TestCronJobRegisterWithJitter(t *testing.T) {
	ctx := t.Context()
	const jitter = 10 * time.Second

	t.Run("starts the chain at the first occurrence", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-1"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		before := time.Now()
		err := a.register(ctx)
		require.NoError(t, err)

		// A jittered schedule has no recurring job: registration dispatches the first occurrence, one interval out and spread by the jitter
		require.Len(t, runner.dispatches, 1)
		first := runner.dispatches[0]
		assert.Equal(t, methodScheduledRun, first.method)
		assert.Empty(t, first.props.Interval, "an occurrence of a chain is a one-shot")
		assert.Empty(t, first.props.Cron)
		firstPayload := scheduledPayload(t, first)
		assert.NotEmpty(t, firstPayload.ChainID)
		assert.Equal(t, firstScheduledRunKeyPrefix+firstPayload.ChainID, first.props.IdempotencyKey, "the first occurrence must be scoped to its chain")
		assertWithinJitter(t, before.Add(time.Minute), first.props.DueTime, jitter+time.Second)

		// The occurrence carries the time it was scheduled for, which is what the run it delivers advances the schedule from
		assert.WithinDuration(t, before.Add(time.Minute), payloadNominal(t, first), time.Second)

		// The schedule is persisted, since the chain's jobs do not carry it the way a recurring job does
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, cronJobState{ChainID: firstPayload.ChainID, ChainInterval: "PT1M"}, state.setStateCalls[0])
	})

	t.Run("starts the chain at the next cron tick", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-2"}
		a := &cronJobScheduler{cron: "0 9 * * *", jitter: jitter, state: state, runner: runner}

		before := time.Now()
		err := a.register(ctx)
		require.NoError(t, err)

		sched, err := cron.ParseStandard("0 9 * * *")
		require.NoError(t, err)
		require.Len(t, runner.dispatches, 1)
		assertWithinJitter(t, sched.Next(before), runner.dispatches[0].props.DueTime, jitter)
		payload := scheduledPayload(t, runner.dispatches[0])
		assert.Equal(t, cronJobState{ChainID: payload.ChainID, ChainCron: "0 9 * * *"}, state.setStateCalls[0])
	})

	t.Run("immediate runs now and starts the chain one occurrence out", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-3"}
		a := &cronJobScheduler{interval: "PT1M", immediate: true, jitter: jitter, state: state, runner: runner}

		before := time.Now()
		err := a.register(ctx)
		require.NoError(t, err)

		// The chain never runs at registration time, so WithImmediate gets a one-shot of its own, which is not part of the chain and is not jittered
		require.Len(t, runner.dispatches, 2)
		assert.Equal(t, methodRun, runner.dispatches[0].method)
		assert.Equal(t, immediateJobIdempotencyKey, runner.dispatches[0].props.IdempotencyKey)
		assert.True(t, runner.dispatches[0].props.DueTime.IsZero(), "an immediate run is not jittered")

		assert.Equal(t, methodScheduledRun, runner.dispatches[1].method)
		assertWithinJitter(t, before.Add(time.Minute), runner.dispatches[1].props.DueTime, jitter+time.Second)
	})

	t.Run("is a no-op when the chain is already registered for the same schedule", func(t *testing.T) {
		state := &fakeClient[cronJobState]{
			state: cronJobState{ChainID: "chain-1", ChainInterval: "PT1M"},
		}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-4",
			jobs: []actor.JobInfo{
				{JobID: "pending", Method: methodScheduledRun, Status: actor.JobStatusPending},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		// A live occurrence is what proves the chain is still going, so bootstrapping from another host changes nothing
		assert.Empty(t, runner.dispatches)
		assert.Empty(t, runner.cancelled)
		assert.Empty(t, state.setStateCalls)
	})

	t.Run("replaces a legacy chain without a generation", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-migrated",
			jobs: []actor.JobInfo{
				{JobID: "legacy", Method: methodScheduledRun, Status: actor.JobStatusPending},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		// Legacy occurrences cannot coordinate with unregister, so bootstrap replaces them with a generation-aware chain
		assert.Equal(t, []string{"legacy"}, runner.cancelled)
		require.Len(t, runner.dispatches, 1)
		payload := scheduledPayload(t, runner.dispatches[0])
		assert.NotEmpty(t, payload.ChainID)
		assert.Equal(t, payload.ChainID, state.setStateCalls[0].ChainID)
	})

	t.Run("restarts the chain when nothing is live", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-old", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-5",
			// The last occurrence was dead-lettered without planning a successor, so the chain ended
			jobs: []actor.JobInfo{
				{JobID: "dead", Method: methodScheduledRun, Status: actor.JobStatusDeadLettered},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, methodScheduledRun, runner.dispatches[0].method)
		assert.Empty(t, runner.cancelled, "a dead-lettered occurrence is not cancellable")
	})

	t.Run("replaces the chain when the schedule changed, preserving the next occurrence", func(t *testing.T) {
		oldDue := time.Now().Add(3 * time.Hour)
		state := &fakeClient[cronJobState]{
			state: cronJobState{ChainID: "chain-old", ChainInterval: "PT5M"},
		}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-6",
			jobs: []actor.JobInfo{
				{JobID: "pending", Method: methodScheduledRun, Status: actor.JobStatusPending, DueTime: oldDue},
				// An occurrence that is executing right now came due in the past, so it says nothing about where the schedule was going next
				{JobID: "running", Method: methodScheduledRun, Status: actor.JobStatusActive, DueTime: time.Now().Add(-time.Second)},
				{JobID: "triggered", Method: methodRun, Status: actor.JobStatusPending},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		// Every occurrence of the outdated chain is cancelled, whether it is waiting or already leased, while a triggered run is not part of the chain and is left alone
		assert.Equal(t, []string{"pending", "running"}, runner.cancelled)

		// The replacement picks the schedule up where the old chain left it, and that due time was already spread once so it is taken as it stands
		require.Len(t, runner.dispatches, 1)
		assert.True(t, oldDue.Equal(runner.dispatches[0].props.DueTime), "expected due time %s, got %s", oldDue, runner.dispatches[0].props.DueTime)
		assert.True(t, oldDue.Equal(payloadNominal(t, runner.dispatches[0])))
		payload := scheduledPayload(t, runner.dispatches[0])
		assert.Equal(t, cronJobState{ChainID: payload.ChainID, ChainInterval: "PT1M"}, state.setStateCalls[0])
	})

	t.Run("replaces a recurring job when jitter is turned on", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "existing"}}
		runner := &fakeClient[struct{}]{dispatchID: "job-7"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		// The recurring job the previous configuration registered would keep running unjittered alongside the chain
		assert.Equal(t, []string{"existing"}, runner.cancelled)
		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, methodScheduledRun, runner.dispatches[0].method)
		payload := scheduledPayload(t, runner.dispatches[0])
		assert.Equal(t, cronJobState{ChainID: payload.ChainID, ChainInterval: "PT1M"}, state.setStateCalls[0], "the replaced job ID must not be left behind in state")
	})

	t.Run("replaces the chain with a recurring job when jitter is turned off", func(t *testing.T) {
		oldDue := time.Now().Add(3 * time.Hour)
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-old", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{
			dispatchID: "job-8",
			jobs: []actor.JobInfo{
				{JobID: "pending", Method: methodScheduledRun, Status: actor.JobStatusPending, DueTime: oldDue},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.register(ctx)
		require.NoError(t, err)

		assert.Equal(t, []string{"pending"}, runner.cancelled)
		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, methodRun, runner.dispatches[0].method)
		assert.Equal(t, "PT1M", runner.dispatches[0].props.Interval, "the replacement is a recurring job again")
		// The recurring job picks up at the occurrence the chain had already committed to
		assert.True(t, oldDue.Equal(runner.dispatches[0].props.DueTime))
		assert.Equal(t, cronJobState{JobID: "job-8"}, state.setStateCalls[0])
	})
}

func TestCronJobScheduledRun(t *testing.T) {
	ctx := t.Context()
	const jitter = 10 * time.Second

	t.Run("plans the next occurrence before running the job", func(t *testing.T) {
		nominal := time.Now().Truncate(time.Second)
		payload := scheduledRunPayload{ChainID: "chain-1", Nominal: nominal}

		var planned scheduledRunPayload
		var ranAfterPlanning bool
		a := &cronJobRunner{
			planNext: func(_ context.Context, got scheduledRunPayload) error {
				planned = got
				return nil
			},
			job: func(context.Context) error {
				ranAfterPlanning = !planned.Nominal.IsZero()
				return nil
			},
		}

		err := a.Job(ctx, methodScheduledRun, envelopeFor(t, payload))
		require.NoError(t, err)

		assert.Equal(t, payload, planned)
		assert.True(t, ranAfterPlanning, "the successor must be planned before the user function runs, so a failing run cannot end the schedule")
	})

	t.Run("plans the next cron occurrence", func(t *testing.T) {
		nominal := time.Now().Truncate(time.Second)
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-2", ChainCron: "0 9 * * *"}}
		runner := &fakeClient[struct{}]{dispatchID: "next-2"}
		a := &cronJobScheduler{cron: "0 9 * * *", jitter: jitter, state: state, runner: runner}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-2", Nominal: nominal}))
		require.NoError(t, err)

		sched, err := cron.ParseStandard("0 9 * * *")
		require.NoError(t, err)
		require.Len(t, runner.dispatches, 1)
		assertWithinJitter(t, sched.Next(nominal), runner.dispatches[0].props.DueTime, jitter)
	})

	t.Run("resumes one interval out when the schedule fell behind", func(t *testing.T) {
		// An hour-long outage leaves this run standing in for every occurrence that came due meanwhile
		// It is the one catch-up run: the schedule resumes an interval from here rather than working through the ones it missed
		nominal := time.Now().Add(-time.Hour)
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-3", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{dispatchID: "next-3"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		before := time.Now()
		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-3", Nominal: nominal}))
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 1)
		assertWithinJitter(t, before.Add(time.Minute), runner.dispatches[0].props.DueTime, jitter+time.Second)
		assert.WithinDuration(t, before.Add(time.Minute), payloadNominal(t, runner.dispatches[0]), time.Second, "the schedule re-anchors, so the occurrences after it do not chase the outage either")
	})

	t.Run("keeps the schedule's own grid when the next occurrence is still ahead", func(t *testing.T) {
		// A run delivered a little late is not behind: the occurrence it plans has not come due yet, so it stays where the schedule puts it
		nominal := time.Now().Add(-30 * time.Second)
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-4", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{dispatchID: "next-4"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-4", Nominal: nominal}))
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 1)
		assertWithinJitter(t, nominal.Add(time.Minute), runner.dispatches[0].props.DueTime, jitter)
		assert.Equal(t, nominal.Add(time.Minute).UnixMilli(), payloadNominal(t, runner.dispatches[0]).UnixMilli())
		assert.Equal(t, "chain-4", scheduledPayload(t, runner.dispatches[0]).ChainID)

		// The chain and source occurrence scope the idempotency key to one generation and one handoff
		expectedKey := scheduledRunKeyPrefix + "chain-4-" + strconv.FormatInt(nominal.UnixMilli(), 10)
		assert.Equal(t, expectedKey, runner.dispatches[0].props.IdempotencyKey)
	})

	t.Run("does not run the job when planning fails", func(t *testing.T) {
		var ran bool
		a := &cronJobRunner{
			planNext: func(context.Context, scheduledRunPayload) error { return errors.New("boom") },
			job:      func(context.Context) error { ran = true; return nil },
		}

		err := a.Job(ctx, methodScheduledRun, envelopeFor(t, scheduledRunPayload{ChainID: "chain-5", Nominal: time.Now()}))
		require.Error(t, err)
		assert.False(t, ran, "the retry runs both steps, so the job must not have run already")
	})

	t.Run("returns a retryable error when dispatch fails", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-6", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{dispatchErr: errors.New("boom")}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-6", Nominal: time.Now()}))
		require.Error(t, err)
		require.NotErrorIs(t, err, actor.ErrJobPermanentFailure, "a transient dispatch failure should be retried")
	})

	t.Run("dead-letters an occurrence with no nominal time", func(t *testing.T) {
		a := &cronJobRunner{planNext: func(context.Context, scheduledRunPayload) error { return nil }, job: noopJob}

		err := a.Job(ctx, methodScheduledRun, envelopeFor(t, scheduledRunPayload{ChainID: "chain-7"}))
		require.Error(t, err)
		assert.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	})

	t.Run("dead-letters an occurrence with no chain ID", func(t *testing.T) {
		a := &cronJobRunner{planNext: func(context.Context, scheduledRunPayload) error { return nil }, job: noopJob}

		err := a.Job(ctx, methodScheduledRun, envelopeFor(t, scheduledRunPayload{Nominal: time.Now()}))
		require.Error(t, err)
		assert.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	})

	t.Run("dead-letters an occurrence with no schedule to advance", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-8"}}
		a := &cronJobScheduler{jitter: jitter, state: state, runner: &fakeClient[struct{}]{}}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-8", Nominal: time.Now()}))
		require.Error(t, err)
		assert.ErrorIs(t, err, actor.ErrJobPermanentFailure, "a schedule that cannot be advanced is not fixed by retrying")
	})

	t.Run("does not extend an unregistered chain", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "next-9"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-old", Nominal: time.Now()}))
		require.NoError(t, err)
		assert.Empty(t, runner.dispatches, "a late occurrence must not resurrect the chain after unregister")
	})

	t.Run("does not extend a replaced chain", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-new", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{dispatchID: "next-10"}
		a := &cronJobScheduler{interval: "PT1M", jitter: jitter, state: state, runner: runner}

		err := a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-old", Nominal: time.Now()}))
		require.NoError(t, err)
		assert.Empty(t, runner.dispatches, "an occurrence from the old generation must not join the replacement chain")
	})
}

func TestJitterDueTime(t *testing.T) {
	nominal := time.Now()

	t.Run("stays within the jitter window", func(t *testing.T) {
		const jitter = time.Minute

		// The offset is random, so sample it enough times to catch a window that is off
		var sawEarlier, sawLater bool
		for range 200 {
			got := jitterDueTime(nominal, jitter)
			assertWithinJitter(t, nominal, got, jitter)

			switch {
			case got.Before(nominal):
				sawEarlier = true
			case got.After(nominal):
				sawLater = true
			}
		}

		// Spreading in both directions is the point of planning an occurrence ahead of time
		assert.True(t, sawEarlier, "jitter should be able to move a run earlier")
		assert.True(t, sawLater, "jitter should be able to move a run later")
	})

	t.Run("leaves the time untouched without jitter", func(t *testing.T) {
		assert.True(t, nominal.Equal(jitterDueTime(nominal, 0)))
		assert.True(t, nominal.Equal(jitterDueTime(nominal, -time.Second)))
	})
}

func TestNextOccurrence(t *testing.T) {
	from := time.Date(2026, 8, 13, 10, 30, 0, 0, time.UTC)

	t.Run("cron", func(t *testing.T) {
		got, err := nextOccurrence("0 9 * * *", "", from)
		require.NoError(t, err)
		assert.Equal(t, time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC), got)
	})

	t.Run("interval", func(t *testing.T) {
		got, err := nextOccurrence("", "PT5M", from)
		require.NoError(t, err)
		assert.Equal(t, from.Add(5*time.Minute), got)
	})

	t.Run("calendar interval", func(t *testing.T) {
		got, err := nextOccurrence("", "P1M", from)
		require.NoError(t, err)
		assert.Equal(t, from.AddDate(0, 1, 0), got)
	})

	t.Run("no schedule", func(t *testing.T) {
		_, err := nextOccurrence("", "", from)
		require.Error(t, err)
	})
}

func TestCronJobTrigger(t *testing.T) {
	ctx := t.Context()

	t.Run("dispatches a one-shot run to the runner with the collapsing key", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "trigger-1"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.trigger(ctx)
		require.NoError(t, err)

		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, methodRun, runner.dispatches[0].method)
		// The fixed idempotency key is what collapses multiple pending triggers into one run
		assert.Equal(t, triggerJobIdempotencyKey, runner.dispatches[0].props.IdempotencyKey)
		// A trigger is a one-shot run, with no recurring schedule
		assert.Empty(t, runner.dispatches[0].props.Interval)
		assert.Empty(t, runner.dispatches[0].props.Cron)
		// Triggering does not touch the scheduler's persisted state
		assert.Empty(t, state.setStateCalls)
	})

	t.Run("routes the trigger message to trigger", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "trigger-2"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		_, err := a.Invoke(ctx, methodTrigger, nil)
		require.NoError(t, err)
		require.Len(t, runner.dispatches, 1)
		assert.Equal(t, triggerJobIdempotencyKey, runner.dispatches[0].props.IdempotencyKey)
	})
}

func TestCronJobRun(t *testing.T) {
	ctx := t.Context()
	var called bool
	a := &cronJobRunner{job: func(context.Context) error {
		called = true
		return nil
	}}

	err := a.Job(ctx, methodRun, nil)
	require.NoError(t, err)
	assert.True(t, called, "the run method should invoke the job function")
}

func TestCronJobUnknownMethod(t *testing.T) {
	a := &cronJobRunner{}

	// The runner only services run jobs, so lifecycle methods, the trigger message, and bogus names are all unknown
	for _, method := range []string{"bogus", ref.MethodBootstrap, builtinactor.MethodUnregister, methodPlanScheduledRun, methodTrigger} {
		err := a.Job(t.Context(), method, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, actor.ErrJobPermanentFailure, "an unknown job method should dead-letter, not retry forever")
	}
}

func TestCronJobInvoke(t *testing.T) {
	ctx := t.Context()

	t.Run("bootstrap registers the recurring job", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-1"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.Bootstrap(ctx, nil)
		require.NoError(t, err)
		require.Len(t, runner.dispatches, 1)
		require.Len(t, state.setStateCalls, 1)
		assert.Equal(t, "job-1", state.setStateCalls[0].JobID)
	})

	t.Run("unregister routes to unregister", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		_, err := a.Invoke(ctx, builtinactor.MethodUnregister, nil)
		require.NoError(t, err)
		// The recurring job is cancelled on the runner, where it was dispatched
		assert.Equal(t, []string{"job-1"}, runner.cancelled)
		assert.True(t, state.deleted)
	})

	t.Run("scheduled planning routes through the scheduler invocation", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-1", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{dispatchID: "next"}
		a := &cronJobScheduler{interval: "PT1M", jitter: time.Second, state: state, runner: runner}

		_, err := a.Invoke(ctx, methodPlanScheduledRun, envelopeFor(t, scheduledRunPayload{ChainID: "chain-1", Nominal: time.Now()}))
		require.NoError(t, err)
		require.Len(t, runner.dispatches, 1)
	})

	t.Run("rejects an unknown lifecycle method", func(t *testing.T) {
		a := &cronJobScheduler{}
		_, err := a.Invoke(ctx, methodRun, nil)
		require.Error(t, err)
	})
}

func TestCronJobUnregister(t *testing.T) {
	ctx := t.Context()

	t.Run("cancels the recurring job on the runner and clears state", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.unregister(ctx)
		require.NoError(t, err)

		assert.Equal(t, []string{"job-1"}, runner.cancelled)
		assert.True(t, state.deleted)
	})

	t.Run("tolerates an already-cancelled job", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{cancelErr: actor.ErrJobNotFound}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.unregister(ctx)
		require.NoError(t, err)
		assert.True(t, state.deleted)
	})

	t.Run("clears state even with no registered job", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.unregister(ctx)
		require.NoError(t, err)
		assert.Empty(t, runner.cancelled, "nothing to cancel when no job is registered")
		assert.True(t, state.deleted)
	})

	t.Run("cancels the chain when jitter is configured", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{ChainID: "chain-old", ChainInterval: "PT1M"}}
		runner := &fakeClient[struct{}]{
			jobs: []actor.JobInfo{
				{JobID: "pending", Method: methodScheduledRun, Status: actor.JobStatusPending},
				{JobID: "running", Method: methodScheduledRun, Status: actor.JobStatusActive},
				{JobID: "dead", Method: methodScheduledRun, Status: actor.JobStatusDeadLettered},
				{JobID: "triggered", Method: methodRun, Status: actor.JobStatusPending},
			},
		}
		a := &cronJobScheduler{interval: "PT1M", jitter: 10 * time.Second, state: state, runner: runner}

		err := a.unregister(ctx)
		require.NoError(t, err)

		// A jittered schedule has no recurring job: cancelling the occurrences it still has is what ends it
		// A leased occurrence counts as active before it has run, so it is cancelled too, while a dead-lettered one is already gone and a triggered run was never part of the schedule
		assert.Equal(t, []string{"pending", "running"}, runner.cancelled)
		assert.True(t, state.deleted)

		// An occurrence already fetched by the runner may ask to plan after cancellation, but the cleared generation keeps it from reviving the chain
		err = a.planScheduledRun(ctx, envelopeFor(t, scheduledRunPayload{ChainID: "chain-old", Nominal: time.Now()}))
		require.NoError(t, err)
		assert.Empty(t, runner.dispatches)
	})

	t.Run("does not list runs without jitter", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{
			jobs: []actor.JobInfo{{JobID: "triggered", Method: methodRun, Status: actor.JobStatusPending}},
		}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		err := a.unregister(ctx)
		require.NoError(t, err)

		// Without jitter the recurring job is the whole schedule, and a pending run is a triggered one that unregistering leaves alone
		assert.Equal(t, []string{"job-1"}, runner.cancelled)
	})
}

// noopJob is a user function that does nothing, for the cases where only the scheduling around it is under test
func noopJob(context.Context) error {
	return nil
}

// envelopeFor wraps a job payload the way the framework delivers it to the actor
func envelopeFor(t *testing.T, payload scheduledRunPayload) actor.Envelope {
	t.Helper()
	return actorcore.NewObjectEnvelope(payload)
}

// payloadNominal returns the occurrence a recorded dispatch was scheduled for
func payloadNominal(t *testing.T, call dispatchCall) time.Time {
	t.Helper()
	return scheduledPayload(t, call).Nominal
}

// scheduledPayload returns the typed payload carried by a recorded jittered occurrence
func scheduledPayload(t *testing.T, call dispatchCall) scheduledRunPayload {
	t.Helper()

	payload, ok := call.input.(scheduledRunPayload)
	require.True(t, ok, "expected a scheduled run payload, got %T", call.input)
	return payload
}

// assertWithinJitter asserts that a planned due time landed inside the window the jitter is allowed to move it to
func assertWithinJitter(t *testing.T, nominal time.Time, due time.Time, jitter time.Duration) {
	t.Helper()

	assert.False(t, due.Before(nominal.Add(-jitter)), "due time %s is more than %s before the occurrence at %s", due, jitter, nominal)
	assert.False(t, due.After(nominal.Add(jitter)), "due time %s is more than %s after the occurrence at %s", due, jitter, nominal)
}

// resolveJobProps applies the scheduler's recurring job options onto a JobProperties for inspection
func resolveJobProps(t *testing.T, a *cronJobScheduler) actor.JobProperties {
	t.Helper()
	opts, err := a.recurringJobOptions()
	require.NoError(t, err)

	var p actor.JobProperties
	for _, o := range opts {
		o(&p)
	}
	return p
}

// fakeClient is a hand-rolled actor.Client[T] that records the calls the cron job scheduler makes
type fakeClient[T any] struct {
	state       T
	getStateErr error

	dispatchID  string
	dispatchErr error
	dispatches  []dispatchCall

	setStateErr   error
	setStateCalls []T

	cancelErr error
	cancelled []string

	deleteErr error
	deleted   bool

	getJobInfo actor.JobInfo
	getJobErr  error

	// jobs is what ListJobs returns, standing in for the runner's live and dead-lettered jobs
	jobs []actor.JobInfo
}

type dispatchCall struct {
	method string
	input  any
	props  actor.JobProperties
}

func (f *fakeClient[T]) GetState(context.Context) (T, error) {
	return f.state, f.getStateErr
}

func (f *fakeClient[T]) SetState(_ context.Context, state T, _ *actor.SetStateOpts) error {
	if f.setStateErr != nil {
		return f.setStateErr
	}
	f.setStateCalls = append(f.setStateCalls, state)
	f.state = state
	return nil
}

func (f *fakeClient[T]) DeleteState(context.Context) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	var zero T
	f.state = zero
	f.deleted = true
	return nil
}

func (f *fakeClient[T]) ListStates(context.Context, *actor.ListStatesOpts) (actor.TypedStateList[T], error) {
	return actor.TypedStateList[T]{}, nil
}

func (f *fakeClient[T]) Dispatch(_ context.Context, method string, input any, opts ...actor.JobOption) (string, error) {
	if f.dispatchErr != nil {
		return "", f.dispatchErr
	}

	var p actor.JobProperties
	for _, o := range opts {
		o(&p)
	}
	f.dispatches = append(f.dispatches, dispatchCall{method: method, input: input, props: p})
	return f.dispatchID, nil
}

func (f *fakeClient[T]) CancelJob(_ context.Context, jobID string) error {
	if f.cancelErr != nil {
		return f.cancelErr
	}
	f.cancelled = append(f.cancelled, jobID)
	return nil
}

// The remaining methods are part of the actor.Client interface but unused by the cron job scheduler
func (f *fakeClient[T]) Invoke(context.Context, string, string, string, any, ...actor.InvokeOption) (actor.Envelope, error) {
	return nil, nil
}

func (f *fakeClient[T]) Peek(context.Context, string, string, string, any, ...actor.InvokeOption) (actor.Envelope, error) {
	return nil, nil
}

func (f *fakeClient[T]) SetAlarm(context.Context, string, actor.AlarmProperties) error {
	return nil
}

func (f *fakeClient[T]) DeleteAlarm(context.Context, string) error {
	return nil
}

func (f *fakeClient[T]) GetJob(context.Context, string) (actor.JobInfo, error) {
	return f.getJobInfo, f.getJobErr
}

func (f *fakeClient[T]) ListJobs(context.Context) ([]actor.JobInfo, error) {
	return f.jobs, nil
}

func (f *fakeClient[T]) RetryJob(context.Context, string) (string, error) {
	return "", nil
}

func (f *fakeClient[T]) Halt() {
	// Nop
}

var (
	_ actor.Client[cronJobState] = (*fakeClient[cronJobState])(nil)
	_ actor.Client[struct{}]     = (*fakeClient[struct{}])(nil)

	_ builtinactor.BuiltInActor = (*CronJob)(nil)
	_ actor.ActorBootstrapper   = (*cronJobScheduler)(nil)
)
