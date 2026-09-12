package workflow_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
)

// TestPurgeTerminatedSweepsPastRetention verifies the sweep removes terminated instances whose retention has elapsed and leaves the rest alone
func TestPurgeTerminatedSweepsPastRetention(t *testing.T) {
	wf, err := workflow.New("sweepable",
		// A short retention is what lets the sweep have something to do, while staying comfortably inside the journal's own TTL backstop of twice that
		workflow.WithRetention(workflow.RetentionPolicy{Completed: time.Second}),
		workflow.WithSteps(
			workflow.Step("done", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "ok", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	for range 3 {
		id, _, sErr := svc.Start(t.Context(), nil)
		require.NoError(t, sErr)
		awaitStatus(t, svc, id, workflow.StatusCompleted)
	}

	time.Sleep(1100 * time.Millisecond)

	removed, err := svc.PurgeTerminated(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 3, removed)

	page, err := svc.List(t.Context(), &workflow.ListOptions{Limit: 50})
	require.NoError(t, err)
	assert.Empty(t, page.Instances)

	// Sweeping again finds nothing left, which is what makes it safe to run on a schedule
	removed, err = svc.PurgeTerminated(t.Context())
	require.NoError(t, err)
	assert.Zero(t, removed)
}

// TestRequiredCapabilityRunsOnACapableHost verifies a step that requires a capability runs on a host that advertises it
func TestRequiredCapabilityRunsOnACapableHost(t *testing.T) {
	var ran atomic.Bool

	wf, err := workflow.New("capable",
		workflow.WithCapability("gpu"),
		workflow.WithSteps(
			workflow.Step("encode",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					ran.Store(true)
					return "encoded", nil
				}),
				workflow.WithRequiredCapability("gpu"),
			),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	awaitStatus(t, svc, id, workflow.StatusCompleted)
	assert.True(t, ran.Load())
}

// TestCompensationRunsOnTheUndoQueueOfTheSameCapability verifies a step's compensation is routed to the undo queue of the capability the forward task had
func TestCompensationRunsOnTheUndoQueueOfTheSameCapability(t *testing.T) {
	var undone atomic.Bool

	wf, err := workflow.New("capable-undo",
		workflow.WithCapability("gpu"),
		workflow.WithSteps(
			workflow.Step("encode",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return "encoded", nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					undone.Store(true)
					return nil
				}),
				workflow.WithRequiredCapability("gpu"),
			),
			workflow.Step("publish", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("nope"))
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
	assert.True(t, undone.Load())
}

// TestCompensateOnFailure verifies a step whose effect may be partial is compensated even when it failed
func TestCompensateOnFailure(t *testing.T) {
	var undone atomic.Bool

	wf, err := workflow.New("partial-effect",
		workflow.WithSteps(
			workflow.Step("half-done",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					// The side effect landed before the failure, which is exactly the case the option exists for
					return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("failed after writing"))
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					undone.Store(true)
					return nil
				}),
				workflow.WithCompensateOnFailure(),
			),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
	assert.True(t, undone.Load(), "a step that opted in is compensated even though it failed")
}

// TestAFailedStepIsNotCompensatedByDefault verifies the saga convention holds: a step that did not complete is taken not to have taken effect
func TestAFailedStepIsNotCompensatedByDefault(t *testing.T) {
	var undone atomic.Bool

	wf, err := workflow.New("saga-convention",
		workflow.WithSteps(
			workflow.Step("charge",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("declined"))
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					undone.Store(true)
					return nil
				}),
			),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.CompensationNone, status.Compensation)
	assert.False(t, undone.Load(), "nothing was charged, so nothing is refunded")
}

// TestUnwindingACompletedChild verifies compensating a child step asks the child to undo itself, popping its own stack in reverse
func TestUnwindingACompletedChild(t *testing.T) {
	var (
		mu   sync.Mutex
		undo []string
	)

	record := func(name string) {
		mu.Lock()
		undo = append(undo, name)
		mu.Unlock()
	}

	child, err := workflow.New("child-with-undo",
		workflow.WithOutput("creds"),
		workflow.WithSteps(
			workflow.Step("cluster",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return "cluster-1", nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					record("delete-cluster")
					return nil
				}),
			),
			workflow.Step("creds",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return "creds-1", nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					record("revoke-creds")
					return nil
				}),
			),
		),
	)
	require.NoError(t, err)

	parent, err := workflow.New("parent-with-undo",
		workflow.WithSteps(
			workflow.Child("provision", workflow.WithDefinition(child)),
			workflow.Step("verify", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("verification failed"))
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, child, parent)
	parentSvc := parent.Service(host.Service())
	childSvc := child.Service(host.Service())

	id, _, err := parentSvc.Start(t.Context(), nil, workflow.WithInstanceID("tenant-2"))
	require.NoError(t, err)

	status := awaitStatus(t, parentSvc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.CompensationCompleted, status.Compensation)

	// The child popped its own stack in reverse, which is the whole point of it having one
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"revoke-creds", "delete-cluster"}, undo)

	childID := stepView(t, status, "provision").ChildIDs[0]
	childStatus, err := childSvc.GetStatus(t.Context(), childID)
	require.NoError(t, err)
	assert.Equal(t, workflow.StatusCancelled, childStatus.Status)
	assert.Equal(t, workflow.CompensationCompleted, childStatus.Compensation)
}

// TestAFailingChildFailsTheParentTask verifies a child that terminates failed is a failure of the parent's task, which the parent's own step policy then decides what to make of
func TestAFailingChildFailsTheParentTask(t *testing.T) {
	child, err := workflow.New("failing-child",
		workflow.WithSteps(
			workflow.Step("boom", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("child failed"))
			})),
		),
	)
	require.NoError(t, err)

	parent, err := workflow.New("parent-of-failing",
		workflow.WithSteps(
			workflow.Child("sub", workflow.WithDefinition(child)),
			workflow.Step("never", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "should not run", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, child, parent)
	svc := parent.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.StepFailed, stepView(t, status, "sub").Status)
	assert.Equal(t, workflow.StepSkipped, stepView(t, status, "never").Status)
}

// TestFanOutOfChildren verifies one child instance is started per element of the upstream step's output, each getting its own item as its input
func TestFanOutOfChildren(t *testing.T) {
	child, err := workflow.New("shipment",
		workflow.WithSteps(
			workflow.Step("ship", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var box string
				rErr := tk.DecodeInput(&box)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return "shipped-" + box, nil
			})),
		),
	)
	require.NoError(t, err)

	parent, err := workflow.New("order",
		workflow.WithConcurrency(4),
		workflow.WithSteps(
			workflow.Step("plan", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return []string{"a", "b", "c"}, nil
			})),
			workflow.ForEach("ship",
				workflow.WithItemsFrom("plan"),
				workflow.WithChild(child),
				workflow.WithMaxParallel(2),
			),
			workflow.Step("collect", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var results []string
				rErr := tk.DecodeOutput("ship", &results)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return results, nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, child, parent)
	svc := parent.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	ship := stepView(t, status, "ship")
	assert.Equal(t, 3, ship.Tasks)
	assert.Equal(t, 3, ship.Completed)
	assert.Len(t, ship.ChildIDs, 3)

	// Every child is locatable from the parent's journal, which is what makes listing them possible
	page, err := svc.List(t.Context(), &workflow.ListOptions{Limit: 50})
	require.NoError(t, err)
	assert.Len(t, page.Instances, 1, "the parent's own listing does not include its children, which have their own workflow name")
}

// TestDefinitionsAndForgetVersion verifies the registry records what has been deployed and refuses to forget a version that still has instances
func TestDefinitionsAndForgetVersion(t *testing.T) {
	wf, err := workflow.New("registered",
		workflow.WithVersion(7),
		workflow.WithSteps(
			workflow.Step("only", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "ok", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)
	awaitStatus(t, svc, id, workflow.StatusCompleted)

	defs, err := svc.Definitions(t.Context())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, 7, defs[0].Version)
	assert.NotEmpty(t, defs[0].Fingerprint)
	assert.False(t, defs[0].Conflicts)

	// Forgetting a version under a running instance would let a different graph claim its number
	err = svc.ForgetVersion(t.Context(), 7)
	require.ErrorIs(t, err, workflow.ErrVersionInUse)

	require.NoError(t, svc.Purge(t.Context(), id))
	require.NoError(t, svc.ForgetVersion(t.Context(), 7))

	defs, err = svc.Definitions(t.Context())
	require.NoError(t, err)
	assert.Empty(t, defs)
}

// TestOutputSizeCapFailsTheAttemptPermanently verifies the cap is enforced on the worker, before the orchestrator ever has to serialize the value
func TestOutputSizeCapFailsTheAttemptPermanently(t *testing.T) {
	wf, err := workflow.New("bounded-output",
		workflow.WithMaxOutputSize(64),
		workflow.WithSteps(
			workflow.Step("big", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return make([]int, 100), nil
			}), workflow.WithMaxAttempts(5)),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	big := stepView(t, status, "big")
	assert.Contains(t, big.Error, "too large")

	// A value that cannot be stored fails the same way on every attempt, so the attempts are not spent on it
	assert.Equal(t, 1, big.Attempts)
}

// TestInputSizeCapIsEnforcedAtStart verifies the workflow input is capped where it is cheapest to refuse, since it is shipped in every task's payload
func TestInputSizeCapIsEnforcedAtStart(t *testing.T) {
	wf, err := workflow.New("bounded-input",
		workflow.WithMaxInputSize(32),
		workflow.WithSteps(
			workflow.Step("only", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	_, _, err = svc.Start(t.Context(), make([]int, 100))
	require.ErrorIs(t, err, workflow.ErrInputTooLarge)
}
