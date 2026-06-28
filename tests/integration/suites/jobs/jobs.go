//go:build integration

// Package jobs exercises job dispatch and execution end to end:
//
// - immediate, delayed, interval, and cron jobs
// - idempotency-key dedup
// - dead-lettering on exhausted retries and on permanent failure with the JobFailed hook
// - GetJob/ListJobs/CancelJob/RetryJob
// - a repeating job whose one failing occurrence dead-letters while the recurrence continues
package jobs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// pollInterval keeps job polling fast so scenarios fire promptly instead of waiting on the multi-second component default
	pollInterval = 250 * time.Millisecond
	// maxAttempts bounds how many times a failing job is retried before it is dead-lettered
	maxAttempts = 3
	// initialRetryDelay keeps the retry backoff short so failure scenarios converge quickly
	initialRetryDelay = 150 * time.Millisecond

	// eventuallyTimeout is generous so the assertions tolerate scheduling and provider latency
	eventuallyTimeout = 20 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	// stabilizeWindow is how long a count must hold steady to count as settled
	stabilizeWindow = pollInterval * 4
)

// matrix is the representative set of topology/provider combinations the job scenarios run against
// Job execution shares the alarm drivers — per-host polling on the local topology and runtime-owned polling on the remote topology — across the SQLite, in-memory standalone, and Postgres provider implementations
var matrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
}{
	{cluster.Local, provider.SQLite},
	{cluster.Local, provider.StandaloneMemory},
	{cluster.Remote, provider.Postgres},
}

// Register the job scenario across the representative matrix
func init() {
	for _, m := range matrix {
		suite.Register(&jobs{kind: m.kind, variant: m.variant})
	}
}

// jobs runs one host with fast polling and drives the job lifecycle through the service
type jobs struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *jobs) Name() string {
	return "jobs/" + string(s.kind) + "/" + string(s.variant)
}

func (s *jobs) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors: []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{
			IdleTimeout:       time.Minute,
			MaxAttempts:       maxAttempts,
			InitialRetryDelay: initialRetryDelay,
		})},
		AlarmsPollInterval: pollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *jobs) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// An immediate job runs once and then quiesces
	t.Run("immediate job runs once", func(t *testing.T) {
		actorID := "immediate-1-" + string(s.kind) + "-" + string(s.variant)
		_, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)

		got := settleJob(t, actorID)
		assert.GreaterOrEqual(t, got, 1, "an immediate job should run")
	})

	// A job delivers its input and method to the actor
	t.Run("carries method and data", func(t *testing.T) {
		actorID := "data-1-" + string(s.kind) + "-" + string(s.variant)
		_, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "send", "hello")
		require.NoError(t, err)

		settleJob(t, actorID)
		fires := shared.ProbeObserver.JobFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, "send", fires[0].Method)
		assert.Equal(t, "hello", fires[0].Data)
	})

	// A delayed job does not run before its delay elapses, then runs
	t.Run("delayed job runs after the delay", func(t *testing.T) {
		actorID := "delayed-1-" + string(s.kind) + "-" + string(s.variant)
		_, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobDelay(800*time.Millisecond))
		require.NoError(t, err)

		got := settleJob(t, actorID)
		assert.GreaterOrEqual(t, got, 1, "a delayed job should eventually run")
	})

	// An interval job runs repeatedly until it is cancelled
	t.Run("interval job repeats", func(t *testing.T) {
		actorID := "interval-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobInterval(shared.ISOInterval(300*time.Millisecond)))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) >= 3
		}, eventuallyTimeout, eventuallyTick, "an interval job should run multiple times")

		// Stop further executions so the count cannot grow into later subtests
		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.NoError(t, err)
	})

	// A cron job is stored with its schedule and reported by GetJob
	// Standard cron has minute granularity, so delivery timing is covered by the interval scenario rather than waited on here
	t.Run("cron job is scheduled", func(t *testing.T) {
		actorID := "cron-1-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobCron("*/5 * * * *"))
		require.NoError(t, err)

		info, err := svc.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, "*/5 * * * *", info.Cron)
		assert.NotEqual(t, actor.JobStatusDeadLettered, info.Status)

		// Clean up so it does not fire during the run
		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.NoError(t, err)
	})

	// Dispatching twice with the same idempotency key yields a single execution
	t.Run("idempotency key dedups", func(t *testing.T) {
		actorID := "idem-1-" + string(s.kind) + "-" + string(s.variant)
		id1, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithIdempotencyKey("k"))
		require.NoError(t, err)
		id2, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithIdempotencyKey("k"))
		require.NoError(t, err)
		assert.Equal(t, id1, id2, "re-dispatching with the same key returns the same job")

		got := settleJob(t, actorID)
		assert.Equal(t, 1, got, "an idempotency key must coalesce to a single execution")
	})

	// Without a key, N dispatches yield N executions
	t.Run("without a key each dispatch runs", func(t *testing.T) {
		actorID := "nokey-1-" + string(s.kind) + "-" + string(s.variant)
		id1, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)
		id2, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)
		assert.NotEqual(t, id1, id2, "each dispatch without a key is a distinct job")

		got := settleJob(t, actorID)
		assert.GreaterOrEqual(t, got, 2, "two keyless dispatches should run twice")
	})

	// A persistently failing job is retried up to the attempt budget and then dead-lettered
	t.Run("dead-letters after exhausting retries", func(t *testing.T) {
		actorID := "deadretry-1-" + string(s.kind) + "-" + string(s.variant)
		shared.ProbeObserver.SetJobFault(actorID, -1)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)

		got := settleJob(t, actorID)
		assert.GreaterOrEqual(t, got, maxAttempts, "a persistently failing job should be retried up to its attempt budget")

		// The job lands in the dead-letter store and the JobFailed hook fires
		require.Eventually(t, func() bool {
			info, gErr := svc.GetJob(ctx, jobID)
			return gErr == nil && info.Status == actor.JobStatusDeadLettered
		}, eventuallyTimeout, eventuallyTick, "an exhausted job should be dead-lettered")
		assert.GreaterOrEqual(t, shared.ProbeObserver.JobFailedCount(actorID), 1, "the JobFailed hook should fire")
	})

	// A job returning ErrJobPermanentFailure is dead-lettered immediately, without retries
	t.Run("dead-letters on permanent failure", func(t *testing.T) {
		actorID := "deadperm-1-" + string(s.kind) + "-" + string(s.variant)
		shared.ProbeObserver.SetJobPermanentFailure(actorID)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)

		got := settleJob(t, actorID)
		assert.Equal(t, 1, got, "a permanent failure should not be retried")

		require.Eventually(t, func() bool {
			info, gErr := svc.GetJob(ctx, jobID)
			return gErr == nil && info.Status == actor.JobStatusDeadLettered
		}, eventuallyTimeout, eventuallyTick, "a permanently failing job should be dead-lettered")
		assert.GreaterOrEqual(t, shared.ProbeObserver.JobFailedCount(actorID), 1, "the JobFailed hook should fire")
	})

	// Cancelling a not-yet-due job prevents it from ever running
	t.Run("cancel before due prevents running", func(t *testing.T) {
		actorID := "cancel-1-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobDelay(time.Hour))
		require.NoError(t, err)
		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.NoError(t, err)

		// Cancelling a job that no longer exists reports the public not-found error
		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.ErrorIs(t, err, actor.ErrJobNotFound)

		got := settleJob(t, actorID)
		assert.Equal(t, 0, got, "a cancelled job must not run")
	})

	// A dead-lettered job can be replayed, which runs it again
	t.Run("retry re-dispatches a dead job", func(t *testing.T) {
		actorID := "retry-1-" + string(s.kind) + "-" + string(s.variant)
		shared.ProbeObserver.SetJobPermanentFailure(actorID)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
		require.NoError(t, err)

		// Wait until the job is dead-lettered
		require.Eventually(t, func() bool {
			info, gErr := svc.GetJob(ctx, jobID)
			return gErr == nil && info.Status == actor.JobStatusDeadLettered
		}, eventuallyTimeout, eventuallyTick, "the job should be dead-lettered before replay")

		before := shared.ProbeObserver.JobCount(actorID)

		// Replay it
		// With the permanent fault already consumed, the retry should succeed and run once more
		newID, err := svc.RetryJob(ctx, jobID)
		require.NoError(t, err)
		assert.NotEqual(t, jobID, newID, "replay should mint a new job ID")

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > before
		}, eventuallyTimeout, eventuallyTick, "the replayed job should run again")
	})

	// A repeating job whose one occurrence fails permanently dead-letters that occurrence while the recurrence keeps running
	t.Run("repeating job dead-letters one occurrence and continues", func(t *testing.T) {
		actorID := "repeat-dead-" + string(s.kind) + "-" + string(s.variant)
		shared.ProbeObserver.SetJobPermanentFailure(actorID)
		_, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobInterval(shared.ISOInterval(300*time.Millisecond)))
		require.NoError(t, err)

		// The first occurrence is dead-lettered
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobFailedCount(actorID) >= 1
		}, eventuallyTimeout, eventuallyTick, "the failing occurrence should be dead-lettered")

		// The recurrence continues, so later occurrences still run successfully
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) >= 3
		}, eventuallyTimeout, eventuallyTick, "the recurrence should keep running after one occurrence dead-letters")

		// The actor now has both a dead job and a live recurrence, so cancel the live one to stop further executions
		list, err := svc.ListJobs(ctx, shared.ProbeActorType, actorID)
		require.NoError(t, err)
		var live, dead int
		for _, j := range list {
			if j.Status == actor.JobStatusDeadLettered {
				dead++
				continue
			}
			live++
			err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, j.JobID)
			require.NoError(t, err)
		}
		assert.GreaterOrEqual(t, dead, 1, "the failed occurrence should be in the dead-letter store")
		assert.GreaterOrEqual(t, live, 1, "the recurrence should still have a live job")
	})

	// GetJob reports the public not-found error for an unknown job
	t.Run("get missing returns not found", func(t *testing.T) {
		_, err := svc.GetJob(ctx, "11111111-1111-7111-8111-111111111111")
		require.ErrorIs(t, err, actor.ErrJobNotFound)
	})
}

// settleJob waits until an actor's job count stops changing for a full stabilize window and returns the settled count
// Waiting for the count to quiesce tolerates the at-least-once nature of job delivery and any retry still in flight
func settleJob(t *testing.T, actorID string) int {
	t.Helper()

	last := shared.ProbeObserver.JobCount(actorID)
	stableSince := time.Now()
	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(eventuallyTick)

		c := shared.ProbeObserver.JobCount(actorID)
		if c != last {
			last = c
			stableSince = time.Now()
			continue
		}

		if time.Since(stableSince) >= stabilizeWindow {
			return last
		}
	}

	t.Fatalf("job for %q did not settle within %s (last count %d)", actorID, eventuallyTimeout, last)
	return last
}
