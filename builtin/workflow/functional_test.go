package workflow_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/testutil"
)

// testRuntimePSK is the shared runtime PSK the functional tests derive their cluster CA from
var testRuntimePSK = []byte("workflow-test-runtime-psk-0123456789")

func testLogger() *slog.Logger {
	if os.Getenv("WF_DEBUG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// startHost builds and runs a local host with the given workflows registered, waiting until it is ready and cleaning it up when the test ends
func startHost(t *testing.T, wfs ...*workflow.Workflow) *local.Host {
	t.Helper()

	host, err := local.NewHost(
		local.WithAddress(testutil.FreeUDPAddr(t)),
		local.WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: filepath.Join(t.TempDir(), "test.db")}),
		local.WithRuntimePSKs(testRuntimePSK),
		// Poll frequently so a dispatched task starts without waiting a full default interval
		local.WithAlarmsPollInterval(100*time.Millisecond),
		local.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	for _, wf := range wfs {
		require.NoError(t, host.RegisterBuiltInActor(wf))
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}

	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("host did not shut down")
		}
	})

	return host
}

// awaitStatus polls an instance until it reaches one of the wanted statuses, and fails the test if it does not
func awaitStatus(t *testing.T, svc *workflow.WorkflowService, id string, want ...workflow.Status) workflow.InstanceStatus {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	var last workflow.InstanceStatus
	for time.Now().Before(deadline) {
		status, err := svc.GetStatus(t.Context(), id)
		if err == nil {
			last = status
			if slices.Contains(want, status.Status) {
				return status
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("instance %s did not reach %v, last status was %q on step %q (cause: %s)", id, want, last.Status, last.CurrentStep, last.Cause)
	return last
}

// stepView returns one step of a status by name, so assertions read by name rather than by position
func stepView(t *testing.T, status workflow.InstanceStatus, name string) workflow.StepStatusView {
	t.Helper()

	for _, s := range status.Steps {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("status has no step %q", name)
	return workflow.StepStatusView{}
}

// TestSequentialWorkflow verifies a plain sequence of steps runs in order, passes each output to the next, and reports the last one as the instance's output
func TestSequentialWorkflow(t *testing.T) {
	var order []string
	var mu sync.Mutex

	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	wf, err := workflow.New("sequential",
		workflow.WithTimeout(time.Minute),
		workflow.WithSteps(
			workflow.Step("first", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				record("first")

				var in map[string]int
				rErr := tk.DecodeInput(&in)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return map[string]int{"doubled": in["n"] * 2}, nil
			})),
			workflow.Step("second", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				record("second")

				var prev map[string]int
				rErr := tk.DecodeOutput("first", &prev)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return map[string]int{"plusOne": prev["doubled"] + 1}, nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, created, err := svc.Start(t.Context(), map[string]int{"n": 20})
	require.NoError(t, err)
	assert.True(t, created)

	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	assert.Equal(t, workflow.CompensationNone, status.Compensation)
	assert.Equal(t, workflow.StepCompleted, stepView(t, status, "first").Status)
	assert.Equal(t, workflow.StepCompleted, stepView(t, status, "second").Status)

	// With no step named, the instance's output is the last step's, and a caller reads it back from the status for as long as the journal is retained
	var out map[string]int
	require.NoError(t, status.DecodeOutput(&out))
	assert.Equal(t, map[string]int{"plusOne": 41}, out)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"first", "second"}, order)
}

// TestStartIsIdempotentForTheSameInstanceID verifies a second Start with the same ID finds the first rather than starting a second run
func TestStartIsIdempotentForTheSameInstanceID(t *testing.T) {
	var runs atomic.Int32

	wf, err := workflow.New("idempotent-start",
		workflow.WithSteps(
			workflow.Step("only", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				runs.Add(1)
				return "done", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, created, err := svc.Start(t.Context(), nil, workflow.WithInstanceID("order-A-91"))
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "order-A-91", id)

	awaitStatus(t, svc, id, workflow.StatusCompleted)

	// The instance already has a journal, so the repeated start is dropped and the second call's input is discarded
	id2, created2, err := svc.Start(t.Context(), "different input", workflow.WithInstanceID("order-A-91"))
	require.NoError(t, err)
	assert.Equal(t, id, id2)
	assert.False(t, created2)
	assert.Equal(t, int32(1), runs.Load())
}

// TestEngineOwnedRetries verifies a retryable failure is retried per the step's own policy, and that the attempts are counted in the journal
func TestEngineOwnedRetries(t *testing.T) {
	var attempts atomic.Int32

	wf, err := workflow.New("retries",
		workflow.WithSteps(
			workflow.Step("flaky",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					if attempts.Add(1) < 3 {
						return nil, errors.New("not yet")
					}
					return "ok", nil
				}),
				workflow.WithMaxAttempts(5),
				workflow.WithRetryBackoff(100*time.Millisecond, time.Second),
			),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	assert.GreaterOrEqual(t, stepView(t, status, "flaky").Attempts, 3)
}

// TestPermanentFailureUnwinds verifies a step that fails permanently under the default policy unwinds the work that already succeeded, in reverse order
func TestPermanentFailureUnwinds(t *testing.T) {
	var (
		mu   sync.Mutex
		undo []string
	)

	wf, err := workflow.New("unwind",
		workflow.WithSteps(
			workflow.Step("reserve",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return map[string]string{"token": "res-1"}, nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					var res map[string]string
					require.NoError(t, c.DecodeResult(&res))

					mu.Lock()
					undo = append(undo, "reserve:"+res["token"]+":"+c.Cause())
					mu.Unlock()
					return nil
				}),
			),
			workflow.Step("charge",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return map[string]string{"chargeId": "ch-1"}, nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					mu.Lock()
					undo = append(undo, "charge")
					mu.Unlock()
					return nil
				}),
			),
			workflow.Step("ship", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("carrier refused"))
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
	assert.Contains(t, status.Cause, "carrier refused")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, undo, 2)

	// A step that ran after another is compensated before it, which is the invariant a saga depends on
	assert.Equal(t, "charge", undo[0])
	assert.Contains(t, undo[1], "reserve:res-1:")
	assert.Contains(t, undo[1], "carrier refused")
}

// TestOptionalAndSkipOnFailure verifies the two ways a step's failure can cost the workflow less than an unwind
func TestOptionalAndSkipOnFailure(t *testing.T) {
	var notified atomic.Bool

	wf, err := workflow.New("failure-policies",
		workflow.WithSteps(
			workflow.Step("manifest",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("store unavailable"))
				}),
				workflow.WithSkipOnFailure("notify"),
			),
			workflow.Step("notify",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					notified.Store(true)
					return nil, nil
				}),
				workflow.WithOptional(),
			),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	// The run is a failure because the manifest is what it was for, but there is nothing to undo so it never unwinds
	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Equal(t, workflow.CompensationNone, status.Compensation)
	assert.Equal(t, workflow.StepFailed, stepView(t, status, "manifest").Status)
	assert.Equal(t, workflow.StepSkipped, stepView(t, status, "notify").Status)
	assert.False(t, notified.Load())
}

// TestParallelGroup verifies a static group runs its members at the same time and hands a later step an object keyed by member name
func TestParallelGroup(t *testing.T) {
	wf, err := workflow.New("parallel",
		workflow.WithConcurrency(4),
		workflow.WithSteps(
			workflow.Parallel("notify",
				workflow.Step("email", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return "email-sent", nil
				})),
				workflow.Step("sms", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					return "sms-sent", nil
				})),
			),
			workflow.Step("check", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var group map[string]string
				rErr := tk.DecodeOutput("notify", &group)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return group, nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	notify := stepView(t, status, "notify")
	assert.Equal(t, 2, notify.Tasks)
	assert.Equal(t, 2, notify.Completed)
}

// TestFanOutTolerateFailures verifies a fan-out is sized from an upstream step's output and that a tolerated failure is recorded without stopping the run
func TestFanOutTolerateFailures(t *testing.T) {
	wf, err := workflow.New("fanout",
		workflow.WithConcurrency(4),
		workflow.WithSteps(
			workflow.Step("plan", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return []int{1, 2, 3, 4}, nil
			})),
			workflow.ForEach("square",
				workflow.WithItemsFrom("plan"),
				workflow.WithMaxParallel(2),
				workflow.WithFailurePolicy(workflow.TolerateFailures),
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					var n int
					rErr := tk.DecodeItem(&n)
					if rErr != nil {
						return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
					}
					if n == 3 {
						return nil, errors.Join(actor.ErrJobPermanentFailure, errors.New("three is unlucky"))
					}
					return n * n, nil
				}),
			),
			workflow.Step("collect", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var results []any
				rErr := tk.DecodeOutput("square", &results)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return len(results), nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	// The step completes despite the failed item, which is recorded in the step's own view
	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	square := stepView(t, status, "square")
	assert.Equal(t, workflow.StepCompleted, square.Status)
	assert.Equal(t, 4, square.Tasks)
	assert.Equal(t, 3, square.Completed)
	assert.Equal(t, 1, square.Failed)
}

// TestWaitForEventAndRaiseEvent verifies an instance parks on a wait step until its event arrives, and that the payload reaches the next step
func TestWaitForEventAndRaiseEvent(t *testing.T) {
	wf, err := workflow.New("approval",
		workflow.WithSteps(
			workflow.Step("open", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "ticket-1", nil
			})),
			workflow.WaitForEvent("approval", workflow.WithEventTimeout(time.Minute)),
			workflow.Step("apply", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var payload map[string]any
				rErr := tk.DecodeOutput("approval", &payload)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return payload["by"], nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	// The instance parks on the wait step, which is where it stays until the event arrives
	require.Eventually(t, func() bool {
		status, sErr := svc.GetStatus(t.Context(), id)
		return sErr == nil && status.CurrentStep == "approval"
	}, 15*time.Second, 50*time.Millisecond)

	require.NoError(t, svc.RaiseEvent(t.Context(), id, "approval", map[string]any{"by": "ops"}))

	status := awaitStatus(t, svc, id, workflow.StatusCompleted)
	assert.Equal(t, workflow.StepCompleted, stepView(t, status, "apply").Status)
}

// TestRaiseEventRejectsAnUnknownEvent verifies an event nothing waits for is refused rather than dispatched as a job nothing reads
func TestRaiseEventRejectsAnUnknownEvent(t *testing.T) {
	wf, err := workflow.New("unknown-event",
		workflow.WithSteps(
			workflow.WaitForEvent("approval"),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	err = svc.RaiseEvent(t.Context(), "some-instance", "nope", nil)
	require.ErrorIs(t, err, workflow.ErrNoSuchEvent)
}

// TestSuspendAndResume verifies a suspended instance records the work already in flight but starts nothing new, and continues from where it left off
func TestSuspendAndResume(t *testing.T) {
	var (
		started   = make(chan struct{})
		release   = make(chan struct{})
		once      sync.Once
		secondRan atomic.Bool
	)

	wf, err := workflow.New("suspendable",
		workflow.WithSteps(
			workflow.Step("first", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				// Holding the first step open makes the suspension land while work is genuinely in flight
				once.Do(func() { close(started) })
				select {
				case <-release:
				case <-time.After(15 * time.Second):
				}
				return "one", nil
			})),
			workflow.Step("second", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				secondRan.Store(true)
				return "two", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the first step never ran")
	}

	require.NoError(t, svc.Suspend(t.Context(), id, "maintenance"))

	status := awaitStatus(t, svc, id, workflow.StatusSuspended)
	require.NotNil(t, status.Suspended)
	assert.Equal(t, "maintenance", status.Suspended.Reason)

	// The work already dispatched runs to completion and its report is recorded, but nothing after it is started
	close(release)
	require.Eventually(t, func() bool {
		s, sErr := svc.GetStatus(t.Context(), id)
		return sErr == nil && stepView(t, s, "first").Status == workflow.StepCompleted
	}, 15*time.Second, 50*time.Millisecond)

	assert.False(t, secondRan.Load(), "a suspended instance must not start the next step")

	status, err = svc.GetStatus(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, workflow.StatusSuspended, status.Status)

	require.NoError(t, svc.Resume(t.Context(), id))

	awaitStatus(t, svc, id, workflow.StatusCompleted)
	assert.True(t, secondRan.Load())
}

// TestCancelUnwinds verifies Cancel moves a running instance into an unwind and terminates it as cancelled
func TestCancelUnwinds(t *testing.T) {
	var (
		started = make(chan struct{})
		once    sync.Once
		undone  atomic.Bool
	)

	wf, err := workflow.New("cancellable",
		workflow.WithSteps(
			workflow.Step("hold",
				workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
					once.Do(func() { close(started) })
					return "held", nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					undone.Store(true)
					return nil
				}),
			),
			workflow.WaitForEvent("never", workflow.WithEventTimeout(time.Hour)),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the first step never ran")
	}

	require.NoError(t, svc.Cancel(t.Context(), id, "customer changed their mind"))

	status := awaitStatus(t, svc, id, workflow.StatusCancelled)
	assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
	assert.Equal(t, "customer changed their mind", status.Cause)
	assert.True(t, undone.Load())
}

// TestListAndPurge verifies instances are listed by their status labels and that a terminated one can be purged while a running one cannot
func TestListAndPurge(t *testing.T) {
	wf, err := workflow.New("listable",
		workflow.WithSteps(
			workflow.Step("done", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "ok", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	ids := make([]string, 3)
	for i := range ids {
		id, _, sErr := svc.Start(t.Context(), i)
		require.NoError(t, sErr)
		ids[i] = id
		awaitStatus(t, svc, id, workflow.StatusCompleted)
	}

	page, err := svc.List(t.Context(), &workflow.ListOptions{Status: workflow.StatusCompleted, Limit: 50})
	require.NoError(t, err)
	assert.Len(t, page.Instances, 3)

	// A status nothing is in returns an empty page rather than everything
	page, err = svc.List(t.Context(), &workflow.ListOptions{Status: workflow.StatusRunning, Limit: 50})
	require.NoError(t, err)
	assert.Empty(t, page.Instances)

	require.NoError(t, svc.Purge(t.Context(), ids[0]))

	_, err = svc.GetStatus(t.Context(), ids[0])
	require.ErrorIs(t, err, workflow.ErrInstanceNotFound)

	// Purging an instance that is already gone is not an error a caller has to guard against, but it does report that there was nothing there
	err = svc.Purge(t.Context(), ids[0])
	require.ErrorIs(t, err, workflow.ErrInstanceNotFound)
}

// TestChildWorkflow verifies a child step runs a whole instance of another definition and that only its result enters the parent's journal
func TestChildWorkflow(t *testing.T) {
	child, err := workflow.New("child-provision",
		workflow.WithOutput("credentials"),
		workflow.WithSteps(
			workflow.Step("cluster", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "cluster-1", nil
			})),
			workflow.Step("credentials", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return map[string]string{"user": "tenant"}, nil
			})),
		),
	)
	require.NoError(t, err)

	parent, err := workflow.New("parent-onboarding",
		workflow.WithSteps(
			workflow.Child("database", workflow.WithDefinition(child)),
			workflow.Step("verify", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				var creds map[string]string
				rErr := tk.DecodeOutput("database", &creds)
				if rErr != nil {
					return nil, errors.Join(actor.ErrJobPermanentFailure, rErr)
				}
				return creds["user"], nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, child, parent)
	parentSvc := parent.Service(host.Service())
	childSvc := child.Service(host.Service())

	id, _, err := parentSvc.Start(t.Context(), nil, workflow.WithInstanceID("tenant-1"))
	require.NoError(t, err)

	status := awaitStatus(t, parentSvc, id, workflow.StatusCompleted)
	dbStep := stepView(t, status, "database")
	require.Len(t, dbStep.ChildIDs, 1)

	// The child kept its own journal, which the parent's only refers to by ID
	childStatus, err := childSvc.GetStatus(t.Context(), dbStep.ChildIDs[0])
	require.NoError(t, err)
	assert.Equal(t, workflow.StatusCompleted, childStatus.Status)
	require.NotNil(t, childStatus.Parent)
	assert.Equal(t, id, childStatus.Parent.InstanceID)

	// WithOutput names which step the instance's output comes from, and the status reports it rather than the last step's
	var creds map[string]string
	require.NoError(t, childStatus.DecodeOutput(&creds))
	assert.Equal(t, map[string]string{"user": "tenant"}, creds)

	// The parent's own output is its last step's, which read the child's
	var verified string
	require.NoError(t, status.DecodeOutput(&verified))
	assert.Equal(t, "tenant", verified)
}
