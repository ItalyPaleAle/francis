//go:build integration

// Package taskpool exercises the built-in task pool actor end to end:
//
//   - every submitted task runs and is drained from the shared queue by whichever hosts have capacity
//   - on a multi-host cluster the pool scales out, running tasks in parallel across hosts up to each host's strict per-host limit
//   - an idempotent submission (a task key) yields a single task even when submitted twice
//   - a task requiring a capability no host advertises stays pending, while base tasks still run
//   - a permanently failing task is dead-lettered and can be retried
//   - clients cannot invoke a built-in actor directly
package taskpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/taskpool"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

const (
	// pollInterval keeps job polling fast so submitted tasks start promptly instead of waiting on the multi-second default
	pollInterval = 250 * time.Millisecond

	eventuallyTimeout = 30 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	// settleWindow is how long a pending task is watched to confirm it does not run
	settleWindow = 3 * time.Second
)

// Task input modes, carried as the task payload so the single shared handler can select a behavior per task
const (
	// modeOK completes immediately
	modeOK = "ok"
	// modeBlock holds the host's slot until the test releases it, used to observe cross-host parallelism
	modeBlock = "block"
	// modeFailOnce fails permanently on its first execution and succeeds afterwards, used to exercise dead-letter and retry
	modeFailOnce = "fail-once"
)

// taskInput is the payload submitted with every task, selecting the handler's behavior
type taskInput struct {
	Mode string `json:"mode"`
}

// matrix runs the scenario across representative topology/provider combinations
// Multi-host entries also prove the pool distributes tasks and runs them in parallel across hosts
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
		suite.Register(&builtinTaskPool{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}
}

// builtinTaskPool drives a cluster whose hosts register a built-in task pool actor and asserts its behavior
type builtinTaskPool struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster  *cluster.Cluster
	pool     *taskpool.TaskPool
	poolType string

	// runs counts successful completions per task ID, so at-least-once redeliveries are absorbed
	// failing marks task IDs whose executions must fail permanently, so the flag is honored by every host and delivery rather than consumed once
	mu      sync.Mutex
	runs    map[string]int
	failing map[string]bool

	// running and maxRunning track live and peak concurrent executions, for the cross-host distribution assertion
	running    atomic.Int32
	maxRunning atomic.Int32
	// release gates the blocking tasks so they hold their host slot until the test lets them finish
	release chan struct{}
}

func (s *builtinTaskPool) Name() string {
	return "builtintaskpool/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinTaskPool) Setup(t *testing.T) []framework.Option {
	s.runs = map[string]int{}
	s.failing = map[string]bool{}
	s.release = make(chan struct{})

	// One pool, registered on every host, with the default strict per-host limit of one so a multi-host cluster runs exactly one task per host at a time
	pool, err := taskpool.New("e2e", taskpool.WithHandler(s.handle))
	require.NoError(t, err)
	s.pool = pool

	// The host registers the actor under the reserved prefix, so the guard test uses the full type
	s.poolType = builtinactor.FullActorType(pool.ActorType())

	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              s.hosts,
		BuiltInActors:      []builtinactor.BuiltInActor{pool},
		AlarmsPollInterval: pollInterval,
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

// handle is the pool's task handler, shared by every host, selecting its behavior from the task payload
func (s *builtinTaskPool) handle(ctx context.Context, task taskpool.Task) error {
	var in taskInput
	// A task with no payload decodes to the zero value, which falls through to the default (immediate completion) branch
	_ = task.Decode(&in)

	switch in.Mode {
	case modeBlock:
		// Take a slot and record the peak, then hold it until released so overlapping runs are observable across hosts
		now := s.running.Add(1)
		s.recordPeak(now)
		defer s.running.Add(-1)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
		s.recordRun(task.ID())
		return nil

	case modeFailOnce:
		// While the task is marked failing, every execution fails permanently, so the job dead-letters no matter which host runs it
		if s.isFailing(task.ID()) {
			return fmt.Errorf("induced permanent task failure: %w", actor.ErrJobPermanentFailure)
		}
		s.recordRun(task.ID())
		return nil

	default:
		s.recordRun(task.ID())
		return nil
	}
}

func (s *builtinTaskPool) Run(t *testing.T) {
	ctx := t.Context()
	svc := s.pool.Service(s.cluster.Service(0))

	// Every submitted task runs, drained from the shared queue by whichever hosts have capacity
	// The handler records a completion under the task's ID, which is the worker's actor ID, so tasks are submitted with a task key to make that ID predictable
	t.Run("runs every submitted task", func(t *testing.T) {
		const numTasks = 8
		keys := make([]string, numTasks)
		for i := range numTasks {
			keys[i] = fmt.Sprintf("run-%d", i)
			_, err := svc.Submit(ctx, taskInput{Mode: modeOK}, taskpool.WithTaskKey(keys[i]))
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			for _, key := range keys {
				if s.runCount(key) == 0 {
					return false
				}
			}
			return true
		}, eventuallyTimeout, eventuallyTick, "every submitted task should run")
	})

	// On a multi-host cluster the pool runs tasks in parallel across hosts, up to one per host
	t.Run("distributes across hosts", func(t *testing.T) {
		s.maxRunning.Store(0)

		// More blocking tasks than hosts guarantees each host has work to pull, so the peak concurrency reveals how many hosts ran at once
		numTasks := s.hosts * 2
		for range numTasks {
			_, err := svc.Submit(ctx, taskInput{Mode: modeBlock})
			require.NoError(t, err)
		}

		// The peak number of simultaneously-running tasks reaches the host count, since each host runs one at a time
		require.Eventually(t, func() bool {
			return int(s.maxRunning.Load()) >= s.hosts
		}, eventuallyTimeout, eventuallyTick, "the pool should run one task per host in parallel")

		// Release the held tasks so their slots free up before the next subtest needs capacity
		close(s.release)
		require.Eventually(t, func() bool {
			return s.running.Load() == 0
		}, eventuallyTimeout, eventuallyTick, "the blocking tasks should drain once released")
	})

	// Submitting twice with the same task key yields a single task
	t.Run("idempotent submit yields one task", func(t *testing.T) {
		const key = "idem-1"
		id1, err := svc.Submit(ctx, taskInput{Mode: modeOK}, taskpool.WithTaskKey(key))
		require.NoError(t, err)
		id2, err := svc.Submit(ctx, taskInput{Mode: modeOK}, taskpool.WithTaskKey(key))
		require.NoError(t, err)

		// The idempotent dispatch dedups, so both submissions resolve to the same task
		assert.Equal(t, id1, id2)

		// And that task runs
		require.Eventually(t, func() bool {
			return s.runCount(key) >= 1
		}, eventuallyTimeout, eventuallyTick, "the idempotent task should run")
	})

	// A task requiring a capability no host advertises stays pending, while base tasks keep running
	t.Run("task with an unavailable capability stays pending", func(t *testing.T) {
		// A base task must run, proving the pool is live
		_, err := svc.Submit(ctx, taskInput{Mode: modeOK}, taskpool.WithTaskKey("base-1"))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return s.runCount("base-1") >= 1
		}, eventuallyTimeout, eventuallyTick, "a base task should run")

		// No host advertises "gpu", so a task requiring it has nowhere to run
		gpuID, err := svc.Submit(ctx, taskInput{Mode: modeOK}, taskpool.WithTaskKey("gpu-1"), taskpool.WithRequiredCapability("gpu"))
		require.NoError(t, err)

		// It never runs, and stays reported as a live pending task
		require.Never(t, func() bool {
			return s.runCount("gpu-1") > 0
		}, settleWindow, eventuallyTick, "a task with no capable host must not run")

		info, err := svc.GetTask(ctx, gpuID)
		require.NoError(t, err)
		assert.Equal(t, taskpool.TaskStatusPending, info.Status)
		assert.Equal(t, "gpu", info.Capability)

		// Cancel it so it does not linger past the scenario
		require.NoError(t, svc.CancelTask(ctx, gpuID))
	})

	// A permanently failing task is dead-lettered, and retrying it runs the task again to success
	t.Run("dead-letters and retries", func(t *testing.T) {
		const key = "dl-1"
		// Mark the task failing so every execution fails permanently until the test clears it
		s.setFailing(key, true)

		id, err := svc.Submit(ctx, taskInput{Mode: modeFailOnce}, taskpool.WithTaskKey(key))
		require.NoError(t, err)

		// The failing task lands in the dead-letter store
		require.Eventually(t, func() bool {
			info, gErr := svc.GetTask(ctx, id)
			return gErr == nil && info.Status == taskpool.TaskStatusDeadLettered
		}, eventuallyTimeout, eventuallyTick, "the permanently failing task should be dead-lettered")

		// Clear the fault, then retry: the re-submitted task runs against the same worker identity and now succeeds
		s.setFailing(key, false)
		_, err = svc.RetryTask(ctx, id)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return s.runCount(key) >= 1
		}, eventuallyTimeout, eventuallyTick, "the retried task should run to success")
	})

	// Clients cannot target a built-in actor through the public Service, on any host
	t.Run("cannot be targeted directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			s.assertClientRejected(t, s.cluster.Service(i), i)
		}
	})
}

// assertClientRejected checks that the public Service rejects the reserved task pool type with ErrActorTypeReserved on every method that targets an actor by type
func (s *builtinTaskPool) assertClientRejected(t *testing.T, svc *actor.Service, host int) {
	t.Helper()
	ctx := t.Context()
	const actorID = "task"

	_, invErr := svc.Invoke(ctx, s.poolType, actorID, "run", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, dispatchErr := svc.Dispatch(ctx, s.poolType, actorID, "run", nil)
	require.ErrorIs(t, dispatchErr, actor.ErrActorTypeReserved, "host %d Dispatch", host)

	setStateErr := svc.SetState(ctx, s.poolType, actorID, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	var dest map[string]any
	getStateErr := svc.GetState(ctx, s.poolType, actorID, &dest)
	require.ErrorIs(t, getStateErr, actor.ErrActorTypeReserved, "host %d GetState", host)

	_, listErr := svc.ListJobs(ctx, s.poolType, actorID)
	require.ErrorIs(t, listErr, actor.ErrActorTypeReserved, "host %d ListJobs", host)

	haltErr := svc.Halt(s.poolType, actorID)
	require.ErrorIs(t, haltErr, actor.ErrActorTypeReserved, "host %d Halt", host)
}

// recordRun records a successful completion for a task ID
func (s *builtinTaskPool) recordRun(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[id]++
}

// runCount returns how many times a task ID has completed successfully
func (s *builtinTaskPool) runCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs[id]
}

// setFailing marks or clears whether executions of a task ID must fail permanently
func (s *builtinTaskPool) setFailing(id string, failing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing[id] = failing
}

// isFailing reports whether executions of a task ID must currently fail permanently
func (s *builtinTaskPool) isFailing(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failing[id]
}

// recordPeak raises the observed peak concurrency to at least now
func (s *builtinTaskPool) recordPeak(now int32) {
	for {
		prev := s.maxRunning.Load()
		if now <= prev || s.maxRunning.CompareAndSwap(prev, now) {
			return
		}
	}
}
