package builtin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

func TestNewCronJobActor(t *testing.T) {
	noop := func(context.Context) error { return nil }

	t.Run("valid interval", func(t *testing.T) {
		b, err := NewCronJobActor("nightly", WithInterval(time.Minute), WithJob(noop))
		require.NoError(t, err)
		assert.Equal(t, cronJobActorTypePrefix+"nightly", b.ActorType())
		assert.NotNil(t, b.Factory())
		assert.Equal(t, cronJobIdleTimeout, b.RegisterOptions().IdleTimeout)
	})

	t.Run("valid period", func(t *testing.T) {
		b, err := NewCronJobActor("p", WithPeriod("PT5M"), WithJob(noop))
		require.NoError(t, err)
		assert.Equal(t, cronJobActorTypePrefix+"p", b.ActorType())
	})

	t.Run("valid cron", func(t *testing.T) {
		_, err := NewCronJobActor("c", WithCron("0 9 * * 1-5"), WithJob(noop))
		require.NoError(t, err)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := NewCronJobActor("", WithInterval(time.Minute), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects name with slash", func(t *testing.T) {
		_, err := NewCronJobActor("a/b", WithInterval(time.Minute), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects no schedule", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects two schedules", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithInterval(time.Minute), WithCron("* * * * *"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects interval and period together", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithInterval(time.Minute), WithPeriod("PT5M"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects zero interval", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithInterval(0), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects invalid period", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithPeriod("not-a-duration"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects invalid cron", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithCron("not a cron"), WithJob(noop))
		require.Error(t, err)
	})

	t.Run("rejects missing job", func(t *testing.T) {
		_, err := NewCronJobActor("x", WithInterval(time.Minute))
		require.Error(t, err)
	})
}

func TestCronJobRecurringJobOptions(t *testing.T) {
	t.Run("interval, not immediate, delays first occurrence", func(t *testing.T) {
		a := &cronJobActor{interval: "PT1M"}
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
		a := &cronJobActor{interval: "PT1M", immediate: true}
		p := resolveJobProps(t, a)
		assert.Equal(t, "PT1M", p.Interval)
		assert.Equal(t, runJobIdempotencyKey, p.IdempotencyKey)
		// No explicit due time means the recurring job's first occurrence defaults to now
		assert.True(t, p.DueTime.IsZero(), "immediate interval should leave the due time unset (runs now)")
	})

	t.Run("cron schedule", func(t *testing.T) {
		a := &cronJobActor{cron: "0 9 * * *"}
		p := resolveJobProps(t, a)
		assert.Equal(t, "0 9 * * *", p.Cron)
		assert.Empty(t, p.Interval)
		assert.Equal(t, runJobIdempotencyKey, p.IdempotencyKey)
		assert.True(t, p.DueTime.IsZero(), "cron schedules its own next tick")
	})
}

func TestCronJobRegister(t *testing.T) {
	ctx := context.Background()

	t.Run("registers recurring job on empty state", func(t *testing.T) {
		fc := &fakeClient{dispatchID: "job-1"}
		a := &cronJobActor{interval: "PT1M", client: fc}

		require.NoError(t, a.register(ctx))

		require.Len(t, fc.dispatches, 1)
		assert.Equal(t, methodRun, fc.dispatches[0].method)
		assert.Equal(t, "PT1M", fc.dispatches[0].props.Interval)
		require.Len(t, fc.setStateCalls, 1)
		assert.Equal(t, "job-1", fc.setStateCalls[0].JobID)
	})

	t.Run("is a no-op when already registered", func(t *testing.T) {
		fc := &fakeClient{state: cronJobState{JobID: "existing"}, dispatchID: "job-2"}
		a := &cronJobActor{interval: "PT1M", client: fc}

		require.NoError(t, a.register(ctx))

		assert.Empty(t, fc.dispatches, "an already-registered actor must not dispatch again")
		assert.Empty(t, fc.setStateCalls)
	})

	t.Run("immediate cron also dispatches a one-shot occurrence", func(t *testing.T) {
		fc := &fakeClient{dispatchID: "job-3"}
		a := &cronJobActor{cron: "0 9 * * *", immediate: true, client: fc}

		require.NoError(t, a.register(ctx))

		require.Len(t, fc.dispatches, 2)
		// The immediate one-shot is dispatched first, with its own idempotency key and no schedule
		assert.Equal(t, immediateJobIdempotencyKey, fc.dispatches[0].props.IdempotencyKey)
		assert.Empty(t, fc.dispatches[0].props.Cron)
		assert.Empty(t, fc.dispatches[0].props.Interval)
		// The recurring cron job follows
		assert.Equal(t, "0 9 * * *", fc.dispatches[1].props.Cron)
		assert.Equal(t, runJobIdempotencyKey, fc.dispatches[1].props.IdempotencyKey)
		require.Len(t, fc.setStateCalls, 1)
		assert.Equal(t, "job-3", fc.setStateCalls[0].JobID)
	})

	t.Run("immediate interval does not dispatch a separate one-shot", func(t *testing.T) {
		fc := &fakeClient{dispatchID: "job-4"}
		a := &cronJobActor{interval: "PT1M", immediate: true, client: fc}

		require.NoError(t, a.register(ctx))

		require.Len(t, fc.dispatches, 1, "interval immediacy is folded into the recurring job's first due time")
		assert.Equal(t, "PT1M", fc.dispatches[0].props.Interval)
	})
}

func TestCronJobRun(t *testing.T) {
	ctx := context.Background()
	var called bool
	a := &cronJobActor{job: func(context.Context) error {
		called = true
		return nil
	}}

	require.NoError(t, a.Job(ctx, methodRun, nil))
	assert.True(t, called, "the run method should invoke the job function")
}

func TestCronJobUnknownMethod(t *testing.T) {
	a := &cronJobActor{}
	err := a.Job(context.Background(), "bogus", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, actor.ErrJobPermanentFailure, "an unknown method should dead-letter, not retry forever")
}

func TestCronJobUnregister(t *testing.T) {
	ctx := context.Background()

	t.Run("cancels the recurring job and clears state", func(t *testing.T) {
		fc := &fakeClient{state: cronJobState{JobID: "job-1"}}
		a := &cronJobActor{interval: "PT1M", client: fc}

		require.NoError(t, a.unregister(ctx))

		assert.Equal(t, []string{"job-1"}, fc.cancelled)
		assert.True(t, fc.deleted)
	})

	t.Run("tolerates an already-cancelled job", func(t *testing.T) {
		fc := &fakeClient{state: cronJobState{JobID: "job-1"}, cancelErr: actor.ErrJobNotFound}
		a := &cronJobActor{interval: "PT1M", client: fc}

		require.NoError(t, a.unregister(ctx))
		assert.True(t, fc.deleted)
	})

	t.Run("clears state even with no registered job", func(t *testing.T) {
		fc := &fakeClient{}
		a := &cronJobActor{interval: "PT1M", client: fc}

		require.NoError(t, a.unregister(ctx))
		assert.Empty(t, fc.cancelled, "nothing to cancel when no job is registered")
		assert.True(t, fc.deleted)
	})
}

// resolveJobProps applies the actor's recurring job options onto a JobProperties for inspection.
func resolveJobProps(t *testing.T, a *cronJobActor) actor.JobProperties {
	t.Helper()
	opts, err := a.recurringJobOptions()
	require.NoError(t, err)

	var p actor.JobProperties
	for _, o := range opts {
		o(&p)
	}
	return p
}

// fakeClient is a hand-rolled actor.Client[cronJobState] that records the calls the cron job actor makes.
type fakeClient struct {
	state       cronJobState
	getStateErr error

	dispatchID  string
	dispatchErr error
	dispatches  []dispatchCall

	setStateErr   error
	setStateCalls []cronJobState

	cancelErr error
	cancelled []string

	deleteErr error
	deleted   bool
}

type dispatchCall struct {
	method string
	props  actor.JobProperties
}

func (f *fakeClient) GetState(context.Context) (cronJobState, error) {
	return f.state, f.getStateErr
}

func (f *fakeClient) SetState(_ context.Context, state cronJobState, _ *actor.SetStateOpts) error {
	if f.setStateErr != nil {
		return f.setStateErr
	}
	f.setStateCalls = append(f.setStateCalls, state)
	f.state = state
	return nil
}

func (f *fakeClient) DeleteState(context.Context) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = true
	return nil
}

func (f *fakeClient) Dispatch(_ context.Context, method string, _ any, opts ...actor.JobOption) (string, error) {
	var p actor.JobProperties
	for _, o := range opts {
		o(&p)
	}
	f.dispatches = append(f.dispatches, dispatchCall{method: method, props: p})
	return f.dispatchID, f.dispatchErr
}

func (f *fakeClient) CancelJob(_ context.Context, jobID string) error {
	if f.cancelErr != nil {
		return f.cancelErr
	}
	f.cancelled = append(f.cancelled, jobID)
	return nil
}

// The remaining methods are part of the actor.Client interface but unused by the cron job actor.
func (f *fakeClient) SetAlarm(context.Context, string, actor.AlarmProperties) error { return nil }
func (f *fakeClient) DeleteAlarm(context.Context, string) error                     { return nil }
func (f *fakeClient) GetJob(context.Context, string) (actor.JobInfo, error) {
	return actor.JobInfo{}, nil
}
func (f *fakeClient) ListJobs(context.Context) ([]actor.JobInfo, error) { return nil, nil }
func (f *fakeClient) RetryJob(context.Context, string) (string, error)  { return "", nil }
func (f *fakeClient) Halt()                                             {}

var _ actor.Client[cronJobState] = (*fakeClient)(nil)
