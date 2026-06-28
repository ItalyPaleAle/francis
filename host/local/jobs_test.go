package local

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/testutil"
)

func newJobsTestHost(t *testing.T, clock *clocktesting.FakeClock) (*Host, *components_mocks.MockActorProvider) {
	provider := components_mocks.NewMockActorProvider(t)
	host := &Host{
		actorProvider:          provider,
		log:                    slog.New(slog.DiscardHandler),
		clock:                  clock,
		providerRequestTimeout: 30 * time.Second,
	}
	return host, provider
}

func TestHostDispatch(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	t.Run("dispatch with idempotency key maps to the alarm name", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)

		provider.
			On("DispatchJob",
				mock.MatchedBy(testutil.MatchContextInterface),
				ref.NewAlarmRef("T", "a1", "key-1"),
				mock.MatchedBy(func(req components.SetAlarmReq) bool {
					return req.Kind == components.AlarmKindJob && req.JobMethod == "send" && req.DueTime.Equal(clock.Now())
				}),
			).
			Return("job-id-1", nil).
			Once()

		jobID, err := host.Dispatch(t.Context(), "T", "a1", "send", nil, actor.JobProperties{IdempotencyKey: "key-1"})
		require.NoError(t, err)
		assert.Equal(t, "job-id-1", jobID)
		provider.AssertExpectations(t)
	})

	t.Run("dispatch without key uses a non-empty random name", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)

		provider.
			On("DispatchJob",
				mock.MatchedBy(testutil.MatchContextInterface),
				mock.MatchedBy(func(r ref.AlarmRef) bool {
					return r.ActorType == "T" && r.ActorID == "a1" && r.Name != ""
				}),
				mock.MatchedBy(func(req components.SetAlarmReq) bool {
					return req.Kind == components.AlarmKindJob && req.JobMethod == "send"
				}),
			).
			Return("job-id-2", nil).
			Once()

		jobID, err := host.Dispatch(t.Context(), "T", "a1", "send", nil, actor.JobProperties{})
		require.NoError(t, err)
		assert.Equal(t, "job-id-2", jobID)
		provider.AssertExpectations(t)
	})

	t.Run("invalid properties are rejected before reaching the provider", func(t *testing.T) {
		host, _ := newJobsTestHost(t, clock)

		_, err := host.Dispatch(t.Context(), "T", "a1", "send", nil, actor.JobProperties{Interval: "PT1H", Cron: "* * * * *"})
		require.ErrorContains(t, err, "mutually exclusive")
	})
}

func TestHostGetJob(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	t.Run("found", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)

		provider.
			On("GetJob", mock.MatchedBy(testutil.MatchContextInterface), "job-1").
			Return(components.JobInfo{
				JobID:     "job-1",
				ActorType: "T",
				ActorID:   "a1",
				Method:    "send",
				Status:    components.JobStatusDeadLettered,
				Attempts:  3,
				LastError: "boom",
			}, nil).
			Once()

		info, err := host.GetJob(t.Context(), "job-1")
		require.NoError(t, err)
		assert.Equal(t, "job-1", info.JobID)
		assert.Equal(t, actor.JobStatusDeadLettered, info.Status)
		assert.Equal(t, 3, info.Attempts)
		assert.Equal(t, "boom", info.LastError)
		provider.AssertExpectations(t)
	})

	t.Run("not found maps to ErrJobNotFound", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)

		provider.
			On("GetJob", mock.MatchedBy(testutil.MatchContextInterface), "missing").
			Return(components.JobInfo{}, components.ErrNoJob).
			Once()

		_, err := host.GetJob(t.Context(), "missing")
		require.ErrorIs(t, err, actor.ErrJobNotFound)
		provider.AssertExpectations(t)
	})
}

func TestHostListJobs(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	host, provider := newJobsTestHost(t, clock)

	provider.
		On("ListJobs", mock.MatchedBy(testutil.MatchContextInterface), "T", "a1").
		Return([]components.JobInfo{
			{JobID: "j1", Status: components.JobStatusPending},
			{JobID: "j2", Status: components.JobStatusDeadLettered},
		}, nil).
		Once()

	jobs, err := host.ListJobs(t.Context(), "T", "a1")
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	assert.Equal(t, actor.JobStatusPending, jobs[0].Status)
	assert.Equal(t, actor.JobStatusDeadLettered, jobs[1].Status)
	provider.AssertExpectations(t)
}

func TestHostCancelJob(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	t.Run("success", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)
		provider.
			On("CancelJob", mock.MatchedBy(testutil.MatchContextInterface), "T", "a1", "j1").
			Return(nil).
			Once()

		err := host.CancelJob(t.Context(), "T", "a1", "j1")
		require.NoError(t, err)
		provider.AssertExpectations(t)
	})

	t.Run("not found maps to ErrJobNotFound", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)
		provider.
			On("CancelJob", mock.MatchedBy(testutil.MatchContextInterface), "T", "a1", "missing").
			Return(components.ErrNoJob).
			Once()

		err := host.CancelJob(t.Context(), "T", "a1", "missing")
		require.ErrorIs(t, err, actor.ErrJobNotFound)
		provider.AssertExpectations(t)
	})
}

func TestHostRetryJob(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	t.Run("re-dispatches atomically via the provider", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)

		provider.
			On("RetryDeadJob", mock.MatchedBy(testutil.MatchContextInterface), "dead-1").
			Return("new-job", nil).
			Once()

		newID, err := host.RetryJob(t.Context(), "dead-1")
		require.NoError(t, err)
		assert.Equal(t, "new-job", newID)
		provider.AssertExpectations(t)
	})

	t.Run("not found maps to ErrJobNotFound", func(t *testing.T) {
		host, provider := newJobsTestHost(t, clock)
		provider.
			On("RetryDeadJob", mock.MatchedBy(testutil.MatchContextInterface), "missing").
			Return("", components.ErrNoJob).
			Once()

		_, err := host.RetryJob(t.Context(), "missing")
		require.ErrorIs(t, err, actor.ErrJobNotFound)
		provider.AssertExpectations(t)
	})
}

func TestJobPropertiesToSetAlarmReq(t *testing.T) {
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)

	t.Run("encodes data and resolves delay", func(t *testing.T) {
		req, err := jobPropertiesToSetAlarmReq(actor.JobProperties{Delay: 10 * time.Minute, Interval: "PT1H"}, "send", map[string]string{"k": "v"}, now)
		require.NoError(t, err)
		assert.Equal(t, components.AlarmKindJob, req.Kind)
		assert.Equal(t, "send", req.JobMethod)
		assert.Equal(t, now.Add(10*time.Minute), req.DueTime)
		assert.Equal(t, "PT1H", req.Interval)
		assert.NotEmpty(t, req.Data)
	})

	t.Run("nil input leaves data empty", func(t *testing.T) {
		req, err := jobPropertiesToSetAlarmReq(actor.JobProperties{}, "send", nil, now)
		require.NoError(t, err)
		assert.Nil(t, req.Data)
		assert.Equal(t, now, req.DueTime)
	})

	t.Run("unserializable input errors", func(t *testing.T) {
		_, err := jobPropertiesToSetAlarmReq(actor.JobProperties{}, "send", make(chan int), now)
		require.ErrorContains(t, err, "msgpack")
	})
}
