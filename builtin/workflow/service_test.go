package workflow_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
)

// TestServiceRefusesWhatItCannotDo covers the calls the service rejects before anything durable happens, which is where a caller's mistake should surface
func TestServiceRefusesWhatItCannotDo(t *testing.T) {
	wf, err := workflow.New("service-errors",
		workflow.WithMaxInputSize(32),
		workflow.WithMaxOutputSize(32),
		workflow.WithSteps(
			workflow.WaitForEvent("approval"),
			workflow.Step("finish", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "done", nil
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	t.Run("an instance ID that would make an actor ID ambiguous", func(t *testing.T) {
		for _, id := range []string{"orders/1", "orders|1"} {
			_, _, err := svc.Start(t.Context(), nil, workflow.WithInstanceID(id))
			require.Error(t, err, "instance ID %q should be refused", id)
		}
	})

	t.Run("an input over the cap", func(t *testing.T) {
		// The input is shipped in every task's payload, so an oversized one would cost the whole run rather than one write
		_, _, err := svc.Start(t.Context(), strings.Repeat("x", 100))
		require.ErrorIs(t, err, workflow.ErrInputTooLarge)
	})

	t.Run("an instance that does not exist", func(t *testing.T) {
		_, err := svc.GetStatus(t.Context(), "never-started")
		require.ErrorIs(t, err, workflow.ErrInstanceNotFound)

		err = svc.Purge(t.Context(), "never-started")
		require.ErrorIs(t, err, workflow.ErrInstanceNotFound)
	})

	t.Run("an event nothing waits for", func(t *testing.T) {
		err := svc.RaiseEvent(t.Context(), "never-started", "not-in-the-graph", nil)
		require.ErrorIs(t, err, workflow.ErrNoSuchEvent)
	})

	t.Run("an event payload over the cap", func(t *testing.T) {
		// An event payload becomes a step's output, so it is bounded by the same cap a task's output is
		err := svc.RaiseEvent(t.Context(), "never-started", "approval", strings.Repeat("x", 100))
		require.ErrorIs(t, err, workflow.ErrInputTooLarge)
	})
}

// TestForgetVersionRefusesAVersionThatStillHasInstances verifies the registry keeps a version for as long as anything runs under it, since forgetting one would let a different graph claim its number
func TestForgetVersionRefusesAVersionThatStillHasInstances(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	wf, err := workflow.New("forget-in-use",
		workflow.WithVersion(3),
		workflow.WithSteps(workflow.WaitForEvent("approval")),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, created, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, created)
	awaitStepStatus(t, svc, id, "approval", workflow.StepRunning)

	// The registry records the version the moment a host serves a job of it, so it is there to be refused
	defs, err := svc.Definitions(t.Context())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, 3, defs[0].Version)
	assert.False(t, defs[0].Conflicts)

	err = svc.ForgetVersion(t.Context(), 3)
	require.ErrorIs(t, err, workflow.ErrVersionInUse)

	// Once the instance is gone the version is free, which is the operator's reset for one registered wrongly
	require.NoError(t, svc.RaiseEvent(t.Context(), id, "approval", nil))
	awaitStatus(t, svc, id, workflow.StatusCompleted)
	require.NoError(t, svc.Purge(t.Context(), id))
	require.NoError(t, svc.ForgetVersion(t.Context(), 3))

	defs, err = svc.Definitions(t.Context())
	require.NoError(t, err)
	assert.Empty(t, defs)
}

// TestListPagesThroughInstances verifies a page carries a cursor that visits every instance exactly once, which is what makes a backlog a long call rather than a large one
func TestListPagesThroughInstances(t *testing.T) {
	wf, err := workflow.New("paged", workflow.WithSteps(workflow.WaitForEvent("approval")))
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	const total = 3
	for range total {
		_, _, err := svc.Start(t.Context(), nil)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		page, lErr := svc.List(t.Context(), &workflow.ListOptions{Status: workflow.StatusRunning})
		return lErr == nil && len(page.Instances) == total
	}, 20*time.Second, 100*time.Millisecond, "every instance should have a journal to list")

	// Paging walks the instances in ID order, which for the default UUIDv7 IDs is creation order
	seen := map[string]struct{}{}
	cursor := ""
	pages := 0
	for {
		page, err := svc.List(t.Context(), &workflow.ListOptions{Limit: 2, After: cursor})
		require.NoError(t, err)
		pages++

		for _, inst := range page.Instances {
			_, dup := seen[inst.InstanceID]
			assert.False(t, dup, "instance %s appeared on two pages", inst.InstanceID)
			seen[inst.InstanceID] = struct{}{}
		}

		cursor = page.AfterID()
		if cursor == "" {
			assert.False(t, page.HasMore, "an empty cursor means this page was the last one")
			break
		}
		require.LessOrEqual(t, pages, total, "paging should terminate")
	}

	assert.Len(t, seen, total)
	assert.Equal(t, 2, pages)

	// A filter that matches nothing is an empty page rather than an error
	page, err := svc.List(t.Context(), &workflow.ListOptions{Status: workflow.StatusCancelled})
	require.NoError(t, err)
	assert.Empty(t, page.Instances)
	assert.False(t, page.HasMore)
}

// TestAnEventTimeoutEndsTheRun verifies a wait step that never gets its event ends the instance rather than parking forever
func TestAnEventTimeoutEndsTheRun(t *testing.T) {
	var undone []string
	var mu sync.Mutex

	wf, err := workflow.New("event-timeout", workflow.WithSteps(
		workflow.Step("reserve",
			workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return "r-1", nil
			}),
			workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
				mu.Lock()
				defer mu.Unlock()
				undone = append(undone, c.Step())
				return nil
			}),
		),
		workflow.WaitForEvent("approval", workflow.WithEventTimeout(time.Second)),
	))
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Contains(t, status.Cause, `event "approval" timed out`)

	// The timeout is an ordinary failure as far as the saga is concerned, so what already succeeded is undone
	assert.Equal(t, workflow.CompensationCompleted, status.Compensation)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"reserve"}, undone)
}

// TestTheInstanceTimeoutEndsTheRun verifies the instance deadline is the backstop that guarantees an instance terminates, whatever it was waiting on
func TestTheInstanceTimeoutEndsTheRun(t *testing.T) {
	wf, err := workflow.New("instance-timeout",
		workflow.WithTimeout(time.Second),
		workflow.WithSteps(workflow.WaitForEvent("approval")),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Contains(t, status.Cause, "instance timeout elapsed")
}

// TestAStepTimeoutFailsTheStepItHit verifies what a timeout costs is decided by the step it elapsed on, exactly as a handler failure would be
func TestAStepTimeoutFailsTheStepItHit(t *testing.T) {
	var attempts int
	var mu sync.Mutex

	wf, err := workflow.New("step-timeout", workflow.WithSteps(
		workflow.Step("slow",
			// The handler fails with a retry far enough out that the step's own deadline is what settles it, rather than a blocked handler holding the host's only slot
			workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				mu.Lock()
				attempts++
				mu.Unlock()
				return nil, errors.New("not ready")
			}),
			workflow.WithMaxAttempts(10),
			workflow.WithRetryBackoff(time.Minute, time.Minute),
			workflow.WithStepTimeout(time.Second),
		),
		workflow.Step("never", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
			return nil, errors.New("this step should never run")
		})),
	))
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)
	assert.Contains(t, stepView(t, status, "slow").Error, `step "slow" timed out`)
	assert.Equal(t, workflow.StepSkipped, stepView(t, status, "never").Status)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, attempts, "the step's deadline settles it before its retry budget is spent")
}

// TestAFailedCompensationDoesNotStopTheUnwind verifies the default policy: a frame that cannot be undone is recorded and the rest of the stack is still unwound
func TestAFailedCompensationDoesNotStopTheUnwind(t *testing.T) {
	var undone []string
	var mu sync.Mutex

	record := func(step string) workflow.CompensateFunc {
		return func(ctx context.Context, c workflow.Compensation) error {
			mu.Lock()
			defer mu.Unlock()
			undone = append(undone, step)
			return nil
		}
	}

	wf, err := workflow.New("keep-unwinding", workflow.WithSteps(
		workflow.Step("first", workflow.WithRun(okRun("1")), workflow.WithCompensate(record("first"))),
		workflow.Step("second",
			workflow.WithRun(okRun("2")),
			workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
				mu.Lock()
				undone = append(undone, "second-attempted")
				mu.Unlock()
				return actor.ErrJobPermanentFailure
			}),
		),
		workflow.Step("boom", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
			return nil, actor.ErrJobPermanentFailure
		})),
	))
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)

	// A failed rollback leaves the system inconsistent, so the outcome says so rather than reporting a clean unwind
	assert.Equal(t, workflow.CompensationPartial, status.Compensation)
	assert.Equal(t, workflow.StepCompensationFailed, stepView(t, status, "second").Status)
	assert.Equal(t, workflow.StepCompensated, stepView(t, status, "first").Status)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"second-attempted", "first"}, undone, "the unwind carries on past the frame it could not undo")
}

// TestAbortUnwindingStopsAtTheFrameItCouldNotUndo verifies the opt-in policy for a saga where undoing out of order is worse than not undoing at all
func TestAbortUnwindingStopsAtTheFrameItCouldNotUndo(t *testing.T) {
	var undone []string
	var mu sync.Mutex

	wf, err := workflow.New("abort-unwinding",
		workflow.WithCompensationFailurePolicy(workflow.AbortUnwinding),
		workflow.WithSteps(
			workflow.Step("first",
				workflow.WithRun(okRun("1")),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					mu.Lock()
					defer mu.Unlock()
					undone = append(undone, "first")
					return nil
				}),
			),
			workflow.Step("second",
				workflow.WithRun(okRun("2")),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					mu.Lock()
					undone = append(undone, "second-attempted")
					mu.Unlock()
					return actor.ErrJobPermanentFailure
				}),
			),
			workflow.Step("boom", workflow.WithRun(func(ctx context.Context, tk workflow.Task) (any, error) {
				return nil, actor.ErrJobPermanentFailure
			})),
		),
	)
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)

	status := awaitStatus(t, svc, id, workflow.StatusFailed)

	assert.Equal(t, workflow.CompensationFailed, status.Compensation)
	assert.Equal(t, workflow.StepCompensationFailed, stepView(t, status, "second").Status)
	assert.Equal(t, workflow.StepCompleted, stepView(t, status, "first").Status, "the frame below the failure is left as it was")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"second-attempted"}, undone)
}

// TestDecodeOutputIsANoOpBeforeAnInstanceCompletes verifies a caller can ask for the output of a run that has not produced one without having to check the status first
func TestDecodeOutputIsANoOpBeforeAnInstanceCompletes(t *testing.T) {
	wf, err := workflow.New("no-output-yet", workflow.WithSteps(workflow.WaitForEvent("approval")))
	require.NoError(t, err)

	host := startHost(t, wf)
	svc := wf.Service(host.Service())

	id, _, err := svc.Start(t.Context(), nil)
	require.NoError(t, err)
	awaitStepStatus(t, svc, id, "approval", workflow.StepRunning)

	status, err := svc.GetStatus(t.Context(), id)
	require.NoError(t, err)

	out := "untouched"
	require.NoError(t, status.DecodeOutput(&out))
	assert.Equal(t, "untouched", out)
}

// okRun returns a handler that succeeds with a fixed output, for the steps of a test that only need a compensable effect
func okRun(output string) workflow.RunFunc {
	return func(ctx context.Context, tk workflow.Task) (any, error) {
		return output, nil
	}
}
