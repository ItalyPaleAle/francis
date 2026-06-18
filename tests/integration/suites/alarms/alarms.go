//go:build integration

// Package alarms exercises alarm scheduling and execution end to end: one-shot and repeating alarms, editing and deleting, transient and persistent execution failures with their retry and removal behavior, and fetching many alarms at once
package alarms

import (
	"strconv"
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
	// pollInterval keeps alarm polling fast so scenarios fire promptly instead of waiting on the multi-second component default
	pollInterval = 250 * time.Millisecond
	// maxAttempts bounds how many times a failing alarm is retried before it is treated as fatal and removed
	maxAttempts = 3
	// initialRetryDelay keeps the retry backoff short so failure scenarios converge quickly
	initialRetryDelay = 150 * time.Millisecond

	// eventuallyTimeout is generous so the assertions tolerate scheduling and provider latency
	eventuallyTimeout = 20 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	// stabilizeWindow is long enough to span several poll cycles, so a "stays at N" assertion would catch an unexpected re-fire
	stabilizeWindow = 2 * time.Second
)

// variants is the representative set the alarm scenarios run against
// Alarm scheduling has three distinct provider implementations: the SQLite path, the Postgres path, and the in-memory standalone path that StandaloneMemory stands in for, so one variant each is enough
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the alarm scenario across the representative variants on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&alarms{kind: k, variant: v})
		}
	}
}

// alarms runs one host with fast alarm polling and drives the alarm lifecycle through the service
type alarms struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *alarms) Name() string {
	return "alarms/" + string(s.kind) + "/" + string(s.variant)
}

func (s *alarms) Setup(t *testing.T) []framework.Option {
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

func (s *alarms) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// A non-repeating alarm fires and then stops, so its execution count settles
	t.Run("one-shot fires once", func(t *testing.T) {
		const actorID = "oneshot-1"
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now(),
		})
		require.NoError(t, err)

		// Alarm delivery is at-least-once, so assert that it fires and then quiesces rather than asserting an exact count
		got := settleAlarm(t, actorID)
		assert.GreaterOrEqual(t, got, 1, "one-shot alarm should fire")
	})

	// An alarm delivers its associated data to the actor
	t.Run("carries data", func(t *testing.T) {
		const actorID = "data-1"
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now(),
			Data:    "hello",
		})
		require.NoError(t, err)

		settleAlarm(t, actorID)
		fires := shared.ProbeObserver.AlarmFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, "hello", fires[0].Data)
	})

	// A repeating alarm fires on its interval until it is removed
	t.Run("repeating fires repeatedly", func(t *testing.T) {
		const actorID = "repeat-1"
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime:  time.Now(),
			Interval: shared.ISOInterval(400 * time.Millisecond),
		})
		require.NoError(t, err)

		// Several executions over time confirm the alarm reschedules itself
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) >= 3
		}, eventuallyTimeout, eventuallyTick, "repeating alarm should fire multiple times")

		// Stop further executions so the count cannot keep growing into later subtests
		err = svc.DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a")
		require.NoError(t, err)
	})

	// A repeating alarm whose interval is shorter than the poll interval must keep firing on its interval
	// This exercises the kept-lease fast path: when the next occurrence is within one poll interval the lease is preserved and the alarm is re-enqueued in memory, rather than waiting for the next fetch
	// A broken kept-lease path falls back to lease renewal (every AlarmsLeaseDuration-10s, i.e. ~10s here), so it could not reach this many fires within the window below
	t.Run("sub-poll interval keeps firing on the kept lease", func(t *testing.T) {
		// The probe observer's fire counts are process-global, so a per-scenario actor ID keeps this count isolated from the same scenario on other variants
		actorID := "repeat-fast-" + string(s.kind) + "-" + string(s.variant)

		// 100ms interval under a 250ms poll keeps the lease, so the alarm must re-arm from memory each time
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime:  time.Now(),
			Interval: shared.ISOInterval(100 * time.Millisecond),
		})
		require.NoError(t, err)

		// Reaching this many fires in well under the ~10s renewal cadence is only possible if the kept lease re-fires the alarm in memory
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) >= 8
		}, 7*time.Second, eventuallyTick, "a sub-poll repeating alarm should fire rapidly on its kept lease")

		// Stop further executions so the count cannot keep growing into later subtests
		err = svc.DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a")
		require.NoError(t, err)
	})

	// A repeating alarm with a TTL stops repeating once the deadline passes, rather than firing forever
	t.Run("repeating alarm stops at its TTL deadline", func(t *testing.T) {
		const actorID = "ttl-1"

		// The fire count is process-global, so measure this run's executions against a baseline
		before := shared.ProbeObserver.AlarmCount(actorID)
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime:  time.Now(),
			Interval: shared.ISOInterval(400 * time.Millisecond),
			TTL:      time.Now().Add(2 * time.Second),
		})
		require.NoError(t, err)

		// settleAlarm returns only once the count stops changing, which a TTL-bounded alarm does but an unbounded repeating one would not
		got := settleAlarm(t, actorID)
		assert.GreaterOrEqual(t, got-before, 2, "the alarm should fire several times before its TTL deadline")
	})

	// Re-setting an alarm replaces its schedule and data, so an edit can bring a far-future alarm forward
	t.Run("edit reschedules and updates data", func(t *testing.T) {
		const actorID = "edit-1"

		// The initial alarm is far in the future, so it will not fire on its own
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now().Add(time.Hour),
			Data:    "old",
		})
		require.NoError(t, err)

		// Editing it to fire now with new data should produce an execution carrying the new data, never the old one
		err = svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now(),
			Data:    "new",
		})
		require.NoError(t, err)

		got := settleAlarm(t, actorID)
		assert.GreaterOrEqual(t, got, 1, "edited alarm should fire")
		fires := shared.ProbeObserver.AlarmFires(actorID)
		require.NotEmpty(t, fires)
		for _, f := range fires {
			assert.Equal(t, "new", f.Data, "the edited schedule and data should replace the original")
		}
	})

	// Deleting an alarm before it comes due prevents it from ever executing
	t.Run("delete before due prevents firing", func(t *testing.T) {
		const actorID = "delete-1"
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now().Add(800 * time.Millisecond),
		})
		require.NoError(t, err)
		err = svc.DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a")
		require.NoError(t, err)

		// The count settling at zero, well past the original due time, confirms the alarm never ran
		got := settleAlarm(t, actorID)
		assert.Equal(t, 0, got, "a deleted alarm must not fire")
	})

	// Deleting an alarm that does not exist reports the public not-found error
	t.Run("delete missing returns not found", func(t *testing.T) {
		err := svc.DeleteAlarm(ctx, shared.ProbeActorType, "no-such-actor", "no-such-alarm")
		require.ErrorIs(t, err, actor.ErrAlarmNotFound)
	})

	// A transient failure is retried and then succeeds, leaving the alarm completed
	t.Run("transient failure retries then succeeds", func(t *testing.T) {
		const actorID = "transient-1"

		// Fail the first execution only, so a retry within the attempt budget succeeds
		shared.ProbeObserver.SetAlarmFault(actorID, 1)
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now(),
		})
		require.NoError(t, err)

		// At least two executions are expected: one failed attempt followed by a successful retry that completes the alarm
		got := settleAlarm(t, actorID)
		require.GreaterOrEqual(t, got, 2, "a transient failure should be retried")

		fires := shared.ProbeObserver.AlarmFires(actorID)
		require.GreaterOrEqual(t, len(fires), 2)
		assert.True(t, fires[0].Failed, "first attempt should fail")
		assert.False(t, fires[len(fires)-1].Failed, "the alarm should ultimately succeed")
	})

	// A persistent failure is retried up to the attempt budget and then removed, so executions stop
	t.Run("persistent failure stops after retries", func(t *testing.T) {
		const actorID = "persistent-1"

		// Fail every execution so the alarm exhausts its attempts and is treated as fatal
		shared.ProbeObserver.SetAlarmFault(actorID, -1)
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime: time.Now(),
		})
		require.NoError(t, err)

		// The count settling proves the alarm was removed once fatal, and it must have retried at least up to the attempt budget first
		// The exact final count differs slightly between runtimes, so assert the lower bound
		got := settleAlarm(t, actorID)
		assert.GreaterOrEqual(t, got, maxAttempts, "a persistently failing alarm should be retried up to its attempt budget before removal")

		fires := shared.ProbeObserver.AlarmFires(actorID)
		for _, f := range fires {
			assert.True(t, f.Failed, "every execution of a persistently failing alarm should fail")
		}
	})

	// Many alarms coming due at once are all fetched and executed, across more than one fetch batch
	t.Run("batch of many alarms all fire", func(t *testing.T) {
		// The default fetch-ahead batch size is 25, so 30 alarms span more than one batch
		const count = 30
		ids := make([]string, count)
		for i := range count {
			ids[i] = "batch-" + strconv.Itoa(i)
			err := svc.SetAlarm(ctx, shared.ProbeActorType, ids[i], "a", actor.AlarmProperties{
				DueTime: time.Now(),
			})
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			for _, id := range ids {
				if shared.ProbeObserver.AlarmCount(id) < 1 {
					return false
				}
			}
			return true
		}, eventuallyTimeout, eventuallyTick, "every alarm in the batch should fire")
	})
}

// settleAlarm waits until an actor's alarm count stops changing for a full stabilize window and returns the settled count
// Waiting for the count to quiesce, rather than checking a fixed window, tolerates the at-least-once nature of alarm delivery and any retry still in flight
// A repeating alarm never settles, so this also distinguishes a one-shot or exhausted alarm from one that keeps firing
func settleAlarm(t *testing.T, actorID string) int {
	t.Helper()

	last := shared.ProbeObserver.AlarmCount(actorID)
	stableSince := time.Now()
	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(eventuallyTick)

		c := shared.ProbeObserver.AlarmCount(actorID)
		if c != last {
			// The count moved, so restart the stabilize window
			last = c
			stableSince = time.Now()
			continue
		}

		// The count has held steady for long enough to consider the alarm quiesced
		if time.Since(stableSince) >= stabilizeWindow {
			return last
		}
	}

	t.Fatalf("alarm for %q did not settle within %s (last count %d)", actorID, eventuallyTimeout, last)
	return last
}
