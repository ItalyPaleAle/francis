package cronjob

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
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
	ctx := context.Background()

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
			getJobInfo: actor.JobInfo{Interval: "PT1M"},
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

		require.NoError(t, a.register(ctx))

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

		require.NoError(t, a.register(ctx))

		require.Len(t, runner.dispatches, 1, "interval immediacy is folded into the recurring job's first due time")
		assert.Equal(t, "PT1M", runner.dispatches[0].props.Interval)
	})
}

func TestCronJobTrigger(t *testing.T) {
	ctx := context.Background()

	t.Run("dispatches a one-shot run to the runner with the collapsing key", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "trigger-1"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		require.NoError(t, a.trigger(ctx))

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
	ctx := context.Background()
	var called bool
	a := &cronJobRunner{job: func(context.Context) error {
		called = true
		return nil
	}}

	require.NoError(t, a.Job(ctx, methodRun, nil))
	assert.True(t, called, "the run method should invoke the job function")
}

func TestCronJobUnknownMethod(t *testing.T) {
	a := &cronJobRunner{}

	// The runner only services run jobs, so lifecycle methods, the trigger message, and bogus names are all unknown
	for _, method := range []string{"bogus", builtinactor.MethodRegister, builtinactor.MethodUnregister, methodTrigger} {
		err := a.Job(context.Background(), method, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, actor.ErrJobPermanentFailure, "an unknown job method should dead-letter, not retry forever")
	}
}

func TestCronJobInvoke(t *testing.T) {
	ctx := context.Background()

	t.Run("register routes to register", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{dispatchID: "job-1"}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		_, err := a.Invoke(ctx, builtinactor.MethodRegister, nil)
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

	t.Run("rejects an unknown lifecycle method", func(t *testing.T) {
		a := &cronJobScheduler{}
		_, err := a.Invoke(ctx, methodRun, nil)
		require.Error(t, err)
	})
}

func TestCronJobUnregister(t *testing.T) {
	ctx := context.Background()

	t.Run("cancels the recurring job on the runner and clears state", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		require.NoError(t, a.unregister(ctx))

		assert.Equal(t, []string{"job-1"}, runner.cancelled)
		assert.True(t, state.deleted)
	})

	t.Run("tolerates an already-cancelled job", func(t *testing.T) {
		state := &fakeClient[cronJobState]{state: cronJobState{JobID: "job-1"}}
		runner := &fakeClient[struct{}]{cancelErr: actor.ErrJobNotFound}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		require.NoError(t, a.unregister(ctx))
		assert.True(t, state.deleted)
	})

	t.Run("clears state even with no registered job", func(t *testing.T) {
		state := &fakeClient[cronJobState]{}
		runner := &fakeClient[struct{}]{}
		a := &cronJobScheduler{interval: "PT1M", state: state, runner: runner}

		require.NoError(t, a.unregister(ctx))
		assert.Empty(t, runner.cancelled, "nothing to cancel when no job is registered")
		assert.True(t, state.deleted)
	})
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
}

type dispatchCall struct {
	method string
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
	f.deleted = true
	return nil
}

func (f *fakeClient[T]) Dispatch(_ context.Context, method string, _ any, opts ...actor.JobOption) (string, error) {
	var p actor.JobProperties
	for _, o := range opts {
		o(&p)
	}
	f.dispatches = append(f.dispatches, dispatchCall{method: method, props: p})
	return f.dispatchID, f.dispatchErr
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

func (f *fakeClient[T]) SetAlarm(context.Context, string, actor.AlarmProperties) error { return nil }

func (f *fakeClient[T]) DeleteAlarm(context.Context, string) error {
	return nil
}

func (f *fakeClient[T]) GetJob(context.Context, string) (actor.JobInfo, error) {
	return f.getJobInfo, f.getJobErr
}
func (f *fakeClient[T]) ListJobs(context.Context) ([]actor.JobInfo, error) { return nil, nil }
func (f *fakeClient[T]) RetryJob(context.Context, string) (string, error)  { return "", nil }
func (f *fakeClient[T]) Halt()                                             {}

var (
	_ actor.Client[cronJobState] = (*fakeClient[cronJobState])(nil)
	_ actor.Client[struct{}]     = (*fakeClient[struct{}])(nil)
)

var _ builtinactor.BuiltInActor = (*CronJob)(nil)
