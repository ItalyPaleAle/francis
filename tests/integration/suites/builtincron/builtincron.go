//go:build integration

// Package builtincron exercises the built-in cron job actor end to end:
//
//   - it runs the user-supplied job on its schedule, cluster-wide on a single node
//   - it registers exactly one recurring job, even when multiple hosts bootstrap it
//   - clients cannot invoke a built-in actor directly
//   - Unregister cancels the recurring job and stops further executions
package builtincron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin"
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

	// singletonActorID mirrors the fixed actor ID built-in singletons use, needed to query the actor's jobs
	singletonActorID = "singleton"

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
	cron     *builtin.BuiltInActor
	cronType string
	// runs counts how many times the user job has executed, incremented from the actor's goroutine
	runs atomic.Int64
}

func (s *builtinCron) Name() string {
	return "builtincron/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinCron) Setup(t *testing.T) []framework.Option {
	cronActor, err := builtin.NewCronJobActor(
		"e2e",
		builtin.WithInterval(cronInterval),
		builtin.WithImmediate(),
		builtin.WithJob(func(context.Context) error {
			s.runs.Add(1)
			return nil
		}),
	)
	require.NoError(t, err)
	s.cron = cronActor
	s.cronType = cronActor.ActorType()

	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              s.hosts,
		BuiltInActors:      []*builtin.BuiltInActor{cronActor},
		AlarmsPollInterval: pollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *builtinCron) Run(t *testing.T) {
	ctx := t.Context()
	svc := s.cluster.Service(0)

	// The job runs repeatedly on its schedule (WithImmediate means the first occurrence is right away)
	t.Run("runs on schedule", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return s.runs.Load() >= 3
		}, eventuallyTimeout, eventuallyTick, "the cron job should run repeatedly on its schedule")
	})

	// Exactly one recurring job is registered, even though every host bootstrapped it
	t.Run("registers exactly once", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return s.liveRunJobs(t, svc) == 1
		}, eventuallyTimeout, eventuallyTick, "exactly one recurring job should be registered")
	})

	// Clients cannot invoke a built-in actor directly, on any host
	t.Run("cannot be invoked directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			_, err := s.cluster.Service(i).Invoke(ctx, s.cronType, singletonActorID, "run", nil)
			require.ErrorIs(t, err, actor.ErrActorTypeReserved, "host %d should reject direct invocation", i)
		}
	})

	// Unregister cancels the recurring job and stops further executions
	t.Run("unregister stops execution", func(t *testing.T) {
		require.NoError(t, s.cron.Unregister(ctx, svc))

		// The recurring job is removed
		require.Eventually(t, func() bool {
			return s.liveRunJobs(t, svc) == 0
		}, eventuallyTimeout, eventuallyTick, "unregister should cancel the recurring job")

		// And the run count stops growing
		settled := s.settleRuns(t)
		time.Sleep(stabilizeWindow)
		assert.Equal(t, settled, s.runs.Load(), "no further executions after unregister")
	})
}

// liveRunJobs returns how many live (non-dead-lettered) recurring "run" jobs the singleton has
// A duplicate registration would surface here as more than one
func (s *builtinCron) liveRunJobs(t *testing.T, svc *actor.Service) int {
	t.Helper()
	jobs, err := svc.ListJobs(t.Context(), s.cronType, singletonActorID)
	require.NoError(t, err)

	var n int
	for _, j := range jobs {
		if j.Status != actor.JobStatusDeadLettered && j.Method == "run" {
			n++
		}
	}
	return n
}

// settleRuns waits until the run count stops changing for a full stabilize window and returns it
func (s *builtinCron) settleRuns(t *testing.T) int64 {
	t.Helper()
	last := s.runs.Load()
	stableSince := time.Now()
	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(eventuallyTick)
		c := s.runs.Load()
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
