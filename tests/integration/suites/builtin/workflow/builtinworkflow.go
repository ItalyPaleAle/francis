//go:build integration

// Package workflow exercises the built-in workflow actor end to end, on a real cluster:
//
//   - a sequence of steps runs in order, each reading the previous step's output, and the instance reports the last one
//   - a fan-out is sized from an upstream step's output and its tasks are spread across hosts, running in parallel
//   - a retryable failure is retried per the step's own policy, and the attempts are counted in the journal
//   - a step that fails terminally unwinds the work that already succeeded, in reverse order
//   - a wait step parks the instance until an event arrives, and suspend stops it starting anything new
//   - a child workflow keeps its own journal, and only its result enters the parent's, and a child that fails fails the parent's task
//   - cancelling an instance in flight unwinds it and terminates it as cancelled
//   - the service refuses a missing instance, an unknown event, an ambiguous instance ID, a version still in use, and a purge of an instance that has not terminated
//   - a purge removes a terminated instance's children before its own journal
//   - listing filters on the workflow labels the orchestrator writes with every journal write
//   - clients cannot invoke a built-in actor directly
package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

const (
	// pollInterval keeps job polling fast so a dispatched task starts promptly instead of waiting on the multi-second default
	pollInterval = 250 * time.Millisecond

	eventuallyTimeout = 60 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	// settleWindow is how long a parked instance is watched to confirm it does not advance
	settleWindow = 3 * time.Second
	// fanOutHold is how long a fan-out task keeps its host's slot, so tasks on different hosts overlap long enough to be seen
	// It stays short because the step's handler holds the host's only concurrency slot while it runs, so a long hold starves the host
	fanOutHold = 300 * time.Millisecond
	// fanOutAttempts is how many instances the distribution assertion will run before giving up on observing the overlap
	fanOutAttempts = 3
	// sweepRetention is how long the sweeper workflow keeps a terminated instance, which the purge test then waits out
	// A terminated journal is written with a TTL of twice its retention, so the window in which the test can still read an instance is twice this, and the wait below has to leave room inside it
	sweepRetention = 5 * time.Second
)

// matrix runs the scenario across representative topology and provider combinations
// The multi-host entries also prove the engine spreads a fan-out's tasks across hosts and runs them in parallel
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
		suite.Register(&builtinWorkflow{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}
}

// builtinWorkflow drives a cluster whose hosts register built-in workflow actors and asserts the engine's behavior
type builtinWorkflow struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster *cluster.Cluster

	// pipeline is the main workflow under test, and shipment is the child definition it composes
	pipeline     *workflow.Workflow
	shipment     *workflow.Workflow
	pipelineType string
	// sweeper is a trivial workflow with a retention short enough to exercise the purge sweep, kept separate so the pipeline's journals survive the whole scenario
	sweeper *workflow.Workflow

	// mu guards every recorded observation, since handlers run concurrently on every host
	mu sync.Mutex
	// undo records the compensations that ran, in the order they ran, so the reverse ordering is observable
	undo []string
	// attempts counts the attempts made per instance, so the engine-owned retry policy is observable
	attempts map[string]int
	// failing marks the instances whose "flaky" step must fail, so the flag is honored by every host and delivery rather than consumed once
	failing map[string]bool
	// started is closed by the gate step of the instance holding it, so a test can act while work is genuinely in flight
	gates map[string]chan struct{}
	// release gates the blocking step so it holds its host slot until the test lets it finish
	release chan struct{}

	// running and maxRunning track live and peak concurrent fan-out tasks, for the cross-host distribution assertion
	running    atomic.Int32
	maxRunning atomic.Int32
}

func (s *builtinWorkflow) Name() string {
	return "builtinworkflow/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinWorkflow) Setup(t *testing.T) []framework.Option {
	s.attempts = map[string]int{}
	s.failing = map[string]bool{}
	s.gates = map[string]chan struct{}{}
	s.release = make(chan struct{})

	// The child is a workflow in its own right, with its own compensation, so unwinding it exercises the parent asking it to undo itself
	shipment, err := workflow.New("e2e-shipment",
		workflow.WithOutput("book"),
		workflow.WithSteps(
			workflow.Step("book",
				workflow.WithRun(s.bookShipment),
				workflow.WithCompensate(s.cancelShipment),
			),
		),
	)
	require.NoError(t, err)
	s.shipment = shipment

	pipeline, err := workflow.New("e2e-pipeline",
		workflow.WithTimeout(5*time.Minute),
		// One task at a time per host, so a fan-out's peak concurrency reveals how many hosts ran at once
		workflow.WithConcurrency(1),
		// A long retention keeps every instance readable for the whole scenario, since the journal is written with a TTL of twice this
		workflow.WithRetention(workflow.RetentionPolicy{Completed: time.Hour, Failed: time.Hour, Cancelled: time.Hour}),
		workflow.WithSteps(
			// Decides what the run does, from the input, and supplies the fan-out's items
			workflow.Step("plan", workflow.WithRun(s.plan)),

			// Held open on request, which is what lets a test suspend or cancel while work is in flight
			workflow.Step("gate", workflow.WithRun(s.gate), workflow.WithCompensate(s.undoGate)),

			// Retried per its own policy, so the attempt count is observable
			workflow.Step("flaky",
				workflow.WithRun(s.flaky),
				workflow.WithCompensate(s.undoFlaky),
				workflow.WithMaxAttempts(5),
				workflow.WithRetryBackoff(200*time.Millisecond, time.Second),
			),

			// One task per planned item, spread across hosts
			workflow.ForEach("fanout",
				workflow.WithItemsFrom("plan"),
				workflow.WithRun(s.fanOutItem),
				workflow.WithFailurePolicy(workflow.TolerateFailures),
			),

			// Parks the instance until the test raises the event
			workflow.WaitForEvent("proceed", workflow.WithEventTimeout(2*time.Minute)),

			// A whole child instance, whose result alone enters this journal
			workflow.Child("ship", workflow.WithDefinition(shipment)),

			// Fails on request, which is what opens the unwind
			workflow.Step("finish", workflow.WithRun(s.finish)),
		),
	)
	require.NoError(t, err)
	s.pipeline = pipeline

	// A retention this short means an instance is past it almost at once, which is what gives the sweep something to remove
	// It is not shorter still because a terminated journal is written with a TTL of twice its retention, and the test has to be able to observe the instance as completed before that expires
	sweeper, err := workflow.New("e2e-sweeper",
		workflow.WithRetention(workflow.RetentionPolicy{Completed: sweepRetention}),
		workflow.WithSteps(
			workflow.Step("noop", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return "done", nil
			})),
		),
	)
	require.NoError(t, err)
	s.sweeper = sweeper

	// The host registers the actor under the reserved prefix, so the guard test uses the full type
	s.pipelineType = builtinactor.FullActorType(pipeline.ActorType())

	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              s.hosts,
		BuiltInActors:      []builtinactor.BuiltInActor{shipment, pipeline, sweeper},
		AlarmsPollInterval: pollInterval,
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

// runInput is the workflow input, selecting per-instance behavior so one definition serves every subtest
type runInput struct {
	// Items is how many fan-out tasks the plan produces
	Items int `json:"items"`
	// Gated holds the gate step open until the test releases it
	Gated bool `json:"gated"`
	// FailFinish makes the last step fail terminally, which opens the unwind
	FailFinish bool `json:"failFinish"`
}

// plan turns the input into the list the fan-out iterates
func (s *builtinWorkflow) plan(ctx context.Context, t workflow.Task) (any, error) {
	var in runInput
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	items := make([]int, in.Items)
	for i := range items {
		items[i] = i
	}
	return items, nil
}

// gate signals that the instance is in flight and, when asked, holds open until the test releases it
func (s *builtinWorkflow) gate(ctx context.Context, t workflow.Task) (any, error) {
	var in runInput
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	s.openGate(t.InstanceID())
	if !in.Gated {
		return "open", nil
	}

	select {
	case <-s.release:
	case <-ctx.Done():
	case <-time.After(eventuallyTimeout):
	}
	return "released", nil
}

// undoGate records that the gate step was compensated
func (s *builtinWorkflow) undoGate(ctx context.Context, c workflow.Compensation) error {
	s.recordUndo(c.InstanceID() + ":gate")
	return nil
}

// flaky fails while the test has marked this instance failing, so the engine's retry policy is observable
func (s *builtinWorkflow) flaky(ctx context.Context, t workflow.Task) (any, error) {
	s.recordAttempt(t.InstanceID())
	if s.isFailing(t.InstanceID()) {
		return nil, errors.New("induced retryable failure")
	}
	return "ok", nil
}

// undoFlaky records that the flaky step was compensated
func (s *builtinWorkflow) undoFlaky(ctx context.Context, c workflow.Compensation) error {
	s.recordUndo(c.InstanceID() + ":flaky")
	return nil
}

// fanOutItem takes a host slot and records the peak, so overlapping runs are observable across hosts
func (s *builtinWorkflow) fanOutItem(ctx context.Context, t workflow.Task) (any, error) {
	now := s.running.Add(1)
	s.recordPeak(now)
	defer s.running.Add(-1)

	var item int
	err := t.DecodeItem(&item)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// Holding the slot briefly makes concurrent tasks overlap long enough to observe
	select {
	case <-time.After(fanOutHold):
	case <-ctx.Done():
	}

	return item * item, nil
}

// shipmentInput is what a child instance receives, which for this graph is the payload of the event preceding its step
type shipmentInput struct {
	// FailBooking makes the child's only step fail terminally, so the child's failure becomes the parent's
	FailBooking bool `json:"failBooking"`
}

// bookShipment is the child's only step, and returns what its compensation needs to undo it
func (s *builtinWorkflow) bookShipment(ctx context.Context, t workflow.Task) (any, error) {
	var in shipmentInput
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}
	if in.FailBooking {
		return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("induced booking failure"))
	}

	return map[string]string{"booking": t.InstanceID()}, nil
}

// cancelShipment records that the child undid itself
func (s *builtinWorkflow) cancelShipment(ctx context.Context, c workflow.Compensation) error {
	var res map[string]string
	err := c.DecodeResult(&res)
	if err != nil {
		return errors.Join(actor.ErrJobPermanentFailure, err)
	}

	s.recordUndo(res["booking"] + ":ship")
	return nil
}

// finish is the last step, and fails terminally when the input asked it to
func (s *builtinWorkflow) finish(ctx context.Context, t workflow.Task) (any, error) {
	var in runInput
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}
	if in.FailFinish {
		return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("induced terminal failure"))
	}
	return "finished", nil
}

func (s *builtinWorkflow) Run(t *testing.T) {
	ctx := t.Context()
	svc := s.pipeline.Service(s.cluster.Service(0))

	// A sequence runs in order, a fan-out is sized from an upstream output, a wait parks the instance, and a child's result enters the parent's journal
	t.Run("runs an instance end to end", func(t *testing.T) {
		id, created, err := svc.Start(ctx, runInput{Items: 3}, workflow.WithInstanceID("happy-1"))
		require.NoError(t, err)
		require.True(t, created)

		// The instance parks on the wait step, which is where it stays until the event arrives
		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the instance should park on its wait step")

		// It really is parked: nothing after the wait starts on its own
		require.Never(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.Status.IsTerminal()
		}, settleWindow, eventuallyTick, "a parked instance must not advance")

		err = svc.RaiseEvent(ctx, id, "proceed", map[string]string{"by": "test"})
		require.NoError(t, err)

		status := s.awaitStatus(t, svc, id, workflow.StatusCompleted)
		assert.Equal(t, workflow.CompensationNone, status.Compensation)

		// The fan-out was sized from the plan's output, and every task reported
		fanout := s.stepView(t, status, "fanout")
		assert.Equal(t, 3, fanout.Tasks)
		assert.Equal(t, 3, fanout.Completed)

		// The child kept its own journal, and the parent's records only its ID
		ship := s.stepView(t, status, "ship")
		require.Len(t, ship.ChildIDs, 1)

		childSvc := s.shipment.Service(s.cluster.Service(0))
		childStatus, err := childSvc.GetStatus(ctx, ship.ChildIDs[0])
		require.NoError(t, err)
		assert.Equal(t, workflow.StatusCompleted, childStatus.Status)
		require.NotNil(t, childStatus.Parent)
		assert.Equal(t, id, childStatus.Parent.InstanceID)
	})

	// A fan-out's tasks are placed independently, so on a multi-host cluster they run in parallel
	t.Run("distributes a fan-out across hosts", func(t *testing.T) {
		// The peak is a sample of how much overlapped, and one run can miss it: if the hosts pick their first tasks up further apart than a task takes to run, every task runs alone
		// A slow or loaded runner makes that likely, so the observation is retried on a fresh instance rather than decided by a single run
		// Retrying the observation rather than lengthening the hold matters because the handler occupies the host's only concurrency slot while it runs, so a longer hold would starve the host and stall everything queued behind it
		var peak int
		for attempt := range fanOutAttempts {
			s.maxRunning.Store(0)

			// More items than hosts guarantees each host has work to pull, so the peak reveals how many ran at once
			id, _, err := svc.Start(ctx, runInput{Items: s.hosts * 3}, workflow.WithInstanceID(fmt.Sprintf("fanout-%d", attempt)))
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				status, sErr := svc.GetStatus(ctx, id)
				return sErr == nil && status.CurrentStep == "proceed"
			}, eventuallyTimeout, eventuallyTick, "the fan-out should complete and the instance park")

			peak = int(s.maxRunning.Load())

			err = svc.RaiseEvent(ctx, id, "proceed", nil)
			require.NoError(t, err)
			s.awaitStatus(t, svc, id, workflow.StatusCompleted)

			if peak >= s.hosts {
				break
			}
		}

		// Each host runs one task at a time, so the peak reaching the host count means they ran in parallel
		assert.GreaterOrEqual(t, peak, s.hosts, "the fan-out should run one task per host in parallel")
	})

	// A retryable failure is retried per the step's own policy, and the attempts are counted in the journal
	t.Run("retries per the step's policy", func(t *testing.T) {
		const id = "retry-1"
		s.setFailing(id, true)

		_, _, err := svc.Start(ctx, runInput{Items: 1}, workflow.WithInstanceID(id))
		require.NoError(t, err)

		// The step keeps failing while the flag is set, and the journal counts the attempts
		require.Eventually(t, func() bool {
			return s.attemptCount(id) >= 2
		}, eventuallyTimeout, eventuallyTick, "the flaky step should be retried")

		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && s.stepView(t, status, "flaky").Attempts >= 2
		}, eventuallyTimeout, eventuallyTick, "the journal should count the attempts")

		// Clearing the fault lets the next attempt succeed and the run carry on
		s.setFailing(id, false)
		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the run should carry on once the step succeeds")

		err = svc.RaiseEvent(ctx, id, "proceed", nil)
		require.NoError(t, err)
		s.awaitStatus(t, svc, id, workflow.StatusCompleted)
	})

	// A step that fails terminally unwinds the work that already succeeded, in reverse order, including the child
	t.Run("unwinds in reverse order", func(t *testing.T) {
		const id = "unwind-1"
		s.resetUndo()

		_, _, err := svc.Start(ctx, runInput{Items: 1, FailFinish: true}, workflow.WithInstanceID(id))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the instance should reach its wait step")
		err = svc.RaiseEvent(ctx, id, "proceed", nil)
		require.NoError(t, err)

		status := s.awaitStatus(t, svc, id, workflow.StatusFailed)
		assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
		assert.Contains(t, status.Cause, "induced terminal failure")

		// A step that ran after another is compensated before it, and the child undid itself as one of those frames
		undone := s.undoneNames()
		require.Len(t, undone, 3)
		assert.Contains(t, undone[0], ":ship")
		assert.Equal(t, id+":flaky", undone[1])
		assert.Equal(t, id+":gate", undone[2])
	})

	// Suspend stops an instance starting anything new, and resume continues it from where the journal says it was
	t.Run("suspends and resumes", func(t *testing.T) {
		const id = "suspend-1"
		gate := s.registerGate(id)

		_, _, err := svc.Start(ctx, runInput{Items: 1, Gated: true}, workflow.WithInstanceID(id))
		require.NoError(t, err)

		// Suspending while the gate step is in flight makes the pause deterministic
		select {
		case <-gate:
		case <-time.After(eventuallyTimeout):
			t.Fatal("the gate step never ran")
		}

		require.NoError(t, svc.Suspend(ctx, id, "integration test"))

		status := s.awaitStatus(t, svc, id, workflow.StatusSuspended)
		require.NotNil(t, status.Suspended)
		assert.Equal(t, "integration test", status.Suspended.Reason)

		// The work already dispatched finishes and is recorded, but nothing after it starts
		close(s.release)
		require.Eventually(t, func() bool {
			st, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && s.stepView(t, st, "gate").Status == workflow.StepCompleted
		}, eventuallyTimeout, eventuallyTick, "the in-flight step should finish and be recorded")

		require.Never(t, func() bool {
			st, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && s.stepView(t, st, "flaky").Status != workflow.StepPending
		}, settleWindow, eventuallyTick, "a suspended instance must not start the next step")

		err = svc.Resume(ctx, id)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			st, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && st.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the resumed instance should carry on")

		err = svc.RaiseEvent(ctx, id, "proceed", nil)
		require.NoError(t, err)
		s.awaitStatus(t, svc, id, workflow.StatusCompleted)
	})

	// A child that terminates failed is a failure of the parent's task, which the parent's own step policy then decides what to make of
	t.Run("fails the parent when its child fails", func(t *testing.T) {
		const id = "child-fail-1"
		s.resetUndo()

		_, _, err := svc.Start(ctx, runInput{Items: 1}, workflow.WithInstanceID(id))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the instance should reach its wait step")

		// The event's payload is what the child step receives as its input, so this is how the child is told to fail
		err = svc.RaiseEvent(ctx, id, "proceed", shipmentInput{FailBooking: true})
		require.NoError(t, err)

		status := s.awaitStatus(t, svc, id, workflow.StatusFailed)
		assert.Equal(t, workflow.StepFailed, s.stepView(t, status, "ship").Status)
		assert.Equal(t, workflow.StepSkipped, s.stepView(t, status, "finish").Status)

		// The child did not complete, so nothing of it is undone, and the unwind starts from the frame below it
		assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
		assert.Equal(t, []string{id + ":flaky", id + ":gate"}, s.undoneNames())

		// The child's own journal records the failure, so an operator can see which instance failed and why
		ship := s.stepView(t, status, "ship")
		require.Len(t, ship.ChildIDs, 1)
		childStatus, err := s.shipment.Service(s.cluster.Service(0)).GetStatus(ctx, ship.ChildIDs[0])
		require.NoError(t, err)
		assert.Equal(t, workflow.StatusFailed, childStatus.Status)
		assert.Contains(t, childStatus.Cause, "induced booking failure")
	})

	// Cancel moves an instance in flight into an unwind and terminates it as cancelled, recording the reason every compensation receives
	t.Run("cancels an instance in flight", func(t *testing.T) {
		const id = "cancel-1"
		s.resetUndo()

		_, _, err := svc.Start(ctx, runInput{Items: 1}, workflow.WithInstanceID(id))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			status, sErr := svc.GetStatus(ctx, id)
			return sErr == nil && status.CurrentStep == "proceed"
		}, eventuallyTimeout, eventuallyTick, "the instance should reach its wait step")

		require.NoError(t, svc.Cancel(ctx, id, "operator changed their mind"))

		status := s.awaitStatus(t, svc, id, workflow.StatusCancelled)
		assert.Equal(t, "operator changed their mind", status.Cause)
		assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
		assert.Equal(t, []string{id + ":flaky", id + ":gate"}, s.undoneNames())

		// The wait step never got its event, so it is closed out rather than recorded as completed
		assert.NotEqual(t, workflow.StepCompleted, s.stepView(t, status, "proceed").Status)
	})

	// The service refuses what it cannot do before anything durable happens, which is where a caller's mistake surfaces
	t.Run("refuses what it cannot do", func(t *testing.T) {
		t.Run("an instance that does not exist", func(t *testing.T) {
			_, err := svc.GetStatus(ctx, "never-started")
			require.ErrorIs(t, err, workflow.ErrInstanceNotFound)

			err = svc.Purge(ctx, "never-started")
			require.ErrorIs(t, err, workflow.ErrInstanceNotFound)
		})

		t.Run("an event nothing waits for", func(t *testing.T) {
			err := svc.RaiseEvent(ctx, "happy-1", "not-in-the-graph", nil)
			require.ErrorIs(t, err, workflow.ErrNoSuchEvent)
		})

		t.Run("an instance ID that would make an actor ID ambiguous", func(t *testing.T) {
			for _, id := range []string{"orders/1", "orders|1"} {
				_, _, err := svc.Start(ctx, nil, workflow.WithInstanceID(id))
				require.Error(t, err, "instance ID %q should be refused", id)
			}
		})

		t.Run("a version that still has instances", func(t *testing.T) {
			// Forgetting a version under a running instance would let a different graph claim its number
			err := svc.ForgetVersion(ctx, s.pipeline.Version())
			require.ErrorIs(t, err, workflow.ErrVersionInUse)
		})

		t.Run("an instance that has not terminated", func(t *testing.T) {
			const id = "active-purge-1"
			_, _, err := svc.Start(ctx, runInput{Items: 1}, workflow.WithInstanceID(id))
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				status, sErr := svc.GetStatus(ctx, id)
				return sErr == nil && status.CurrentStep == "proceed"
			}, eventuallyTimeout, eventuallyTick, "the instance should reach its wait step")

			// Purging a running instance would delete the journal that accounts for the work still in flight
			err = svc.Purge(ctx, id)
			require.ErrorIs(t, err, workflow.ErrInstanceActive)

			// Leave the instance terminated, so it does not outlive the subtest that started it
			require.NoError(t, svc.Cancel(ctx, id, "done with it"))
			s.awaitStatus(t, svc, id, workflow.StatusCancelled)
		})
	})

	// A purge removes a terminated instance's children first, recursively, then its own journal
	t.Run("purges an instance and its child", func(t *testing.T) {
		status, err := svc.GetStatus(ctx, "happy-1")
		require.NoError(t, err)
		ship := s.stepView(t, status, "ship")
		require.Len(t, ship.ChildIDs, 1)
		childID := ship.ChildIDs[0]

		childSvc := s.shipment.Service(s.cluster.Service(0))
		require.NoError(t, svc.Purge(ctx, "happy-1"))

		_, err = svc.GetStatus(ctx, "happy-1")
		require.ErrorIs(t, err, workflow.ErrInstanceNotFound)

		// The child is removed with its parent, so a purge never leaves a journal nothing points at
		require.Eventually(t, func() bool {
			_, cErr := childSvc.GetStatus(ctx, childID)
			return errors.Is(cErr, workflow.ErrInstanceNotFound)
		}, eventuallyTimeout, eventuallyTick, "child %s should be gone with its parent", childID)

		// Repeating an interrupted purge has to be safe, so a second call reports the instance as gone rather than failing
		err = svc.Purge(ctx, "happy-1")
		require.ErrorIs(t, err, workflow.ErrInstanceNotFound)
	})

	// Listing filters on the workflow labels the orchestrator writes in the same operation as the journal
	t.Run("lists by status", func(t *testing.T) {
		page, err := svc.List(ctx, &workflow.ListOptions{Status: workflow.StatusCompleted, Limit: 100})
		require.NoError(t, err)
		assert.NotEmpty(t, page.Instances, "the completed instances should be listed")
		for _, inst := range page.Instances {
			assert.Equal(t, workflow.StatusCompleted, inst.Status)
		}

		page, err = svc.List(ctx, &workflow.ListOptions{Status: workflow.StatusFailed, Limit: 100})
		require.NoError(t, err)
		assert.NotEmpty(t, page.Instances, "the failed instance should be listed")
		for _, inst := range page.Instances {
			assert.Equal(t, workflow.StatusFailed, inst.Status)
		}
	})

	// The registry records the version this cluster's hosts serve, and no host conflicts with it
	t.Run("registers its definition", func(t *testing.T) {
		defs, err := svc.Definitions(ctx)
		require.NoError(t, err)
		require.Len(t, defs, 1)
		assert.Equal(t, s.pipeline.Version(), defs[0].Version)
		assert.NotEmpty(t, defs[0].Fingerprint)
		assert.False(t, defs[0].Conflicts, "every host serves the same graph")
	})

	// The sweep removes terminated instances past their retention, and leaves nothing behind
	t.Run("purges terminated instances", func(t *testing.T) {
		sweepSvc := s.sweeper.Service(s.cluster.Service(0))

		// Both instances are started before either is awaited, so the first one's journal spends as little of its retention as possible waiting on the second
		ids := []string{"sweep-1", "sweep-2"}
		for _, id := range ids {
			_, _, err := sweepSvc.Start(ctx, nil, workflow.WithInstanceID(id))
			require.NoError(t, err)
		}
		for _, id := range ids {
			s.awaitStatus(t, sweepSvc, id, workflow.StatusCompleted)
		}

		// Both instances are past their retention by now, and still well inside the TTL that keeps their journals readable
		time.Sleep(sweepRetention + 500*time.Millisecond)

		removed, err := sweepSvc.PurgeTerminated(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(ids), removed, "the sweep should remove every terminated instance past its retention")

		for _, id := range ids {
			_, err = sweepSvc.GetStatus(ctx, id)
			require.ErrorIs(t, err, workflow.ErrInstanceNotFound, "instance %s should be gone", id)
		}

		// Sweeping again finds nothing left, so it is safe to run on a schedule
		removed, err = sweepSvc.PurgeTerminated(ctx)
		require.NoError(t, err)
		assert.Zero(t, removed)
	})

	// Clients cannot target a built-in actor through the public Service, on any host
	t.Run("cannot be targeted directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			s.assertClientRejected(t, s.cluster.Service(i), i)
		}
	})
}

// assertClientRejected checks that the public Service rejects the reserved workflow type on every method that targets an actor by type
func (s *builtinWorkflow) assertClientRejected(t *testing.T, svc *actor.Service, host int) {
	t.Helper()
	ctx := t.Context()
	const instanceID = "instance"

	_, invErr := svc.Invoke(ctx, s.pipelineType, instanceID, "status", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, peekErr := svc.Peek(ctx, s.pipelineType, instanceID, "status", nil)
	require.ErrorIs(t, peekErr, actor.ErrActorTypeReserved, "host %d Peek", host)

	_, dispatchErr := svc.Dispatch(ctx, s.pipelineType, instanceID, "start", nil)
	require.ErrorIs(t, dispatchErr, actor.ErrActorTypeReserved, "host %d Dispatch", host)

	setStateErr := svc.SetState(ctx, s.pipelineType, instanceID, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	_, listErr := svc.ListStates(ctx, s.pipelineType, nil)
	require.ErrorIs(t, listErr, actor.ErrActorTypeReserved, "host %d ListStates", host)
}

// awaitStatus polls an instance until it reaches the wanted status, and fails the test if it does not
// It polls in the test's own goroutine rather than through require.Eventually so the last error is still available to report: an instance whose journal passed its retention reads as missing, which is a very different failure from one that is merely slow
func (s *builtinWorkflow) awaitStatus(t *testing.T, svc *workflow.WorkflowService, id string, want workflow.Status) workflow.InstanceStatus {
	t.Helper()

	var (
		last    workflow.InstanceStatus
		lastErr error
	)

	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		status, err := svc.GetStatus(t.Context(), id)
		if err != nil {
			lastErr = err
		} else {
			last = status
			lastErr = nil
			if status.Status == want {
				return last
			}
		}
		time.Sleep(eventuallyTick)
	}

	require.FailNowf(t, "instance did not reach the wanted status",
		"instance %s should reach %q, last status was %q on step %q (last error: %v)", id, want, last.Status, last.CurrentStep, lastErr)
	return last
}

// stepView returns one step of a status by name, so assertions read by name rather than by position
func (s *builtinWorkflow) stepView(t *testing.T, status workflow.InstanceStatus, name string) workflow.StepStatusView {
	t.Helper()

	for _, step := range status.Steps {
		if step.Name == name {
			return step
		}
	}

	require.FailNowf(t, "missing step", "status has no step %q", name)
	return workflow.StepStatusView{}
}

// recordAttempt counts an attempt of the flaky step for an instance
func (s *builtinWorkflow) recordAttempt(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[id]++
}

// attemptCount returns how many attempts of the flaky step an instance has made
func (s *builtinWorkflow) attemptCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[id]
}

// setFailing marks or clears whether the flaky step must fail for an instance
func (s *builtinWorkflow) setFailing(id string, failing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing[id] = failing
}

// isFailing reports whether the flaky step must currently fail for an instance
func (s *builtinWorkflow) isFailing(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failing[id]
}

// recordUndo appends a compensation to the order they ran in
func (s *builtinWorkflow) recordUndo(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.undo = append(s.undo, name)
}

// resetUndo clears the recorded compensations before a subtest that asserts on their order
func (s *builtinWorkflow) resetUndo() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.undo = nil
}

// undoneNames returns the compensations that ran, in order
func (s *builtinWorkflow) undoneNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.undo...)
}

// registerGate creates the channel the gate step closes when an instance reaches it
func (s *builtinWorkflow) registerGate(id string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan struct{})
	s.gates[id] = ch
	return ch
}

// openGate closes an instance's gate channel, at most once, since the step can run more than once
func (s *builtinWorkflow) openGate(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, ok := s.gates[id]
	if !ok {
		return
	}
	delete(s.gates, id)
	close(ch)
}

// recordPeak raises the observed peak concurrency to at least now
func (s *builtinWorkflow) recordPeak(now int32) {
	for {
		prev := s.maxRunning.Load()
		if now <= prev || s.maxRunning.CompareAndSwap(prev, now) {
			return
		}
	}
}
