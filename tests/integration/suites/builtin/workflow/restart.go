//go:build integration

package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

// restartMatrix covers the providers that keep their store outside the host process, since only those have anything to resume
// The standalone in-memory provider is left out: its store lives in the host, so a restart is a fresh cluster
var restartMatrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
}{
	{cluster.Local, provider.SQLite},
	{cluster.Remote, provider.Postgres},
}

func init() {
	for _, m := range restartMatrix {
		suite.Register(&workflowRestart{kind: m.kind, variant: m.variant})
	}
}

// workflowRestart drives a single-host cluster, stops the host while an instance still has work outstanding, and brings it back
//
// It runs on a cluster of its own because restarting a host replaces its actor.Service and moves every actor it held, which the shared scenario's later subtests would then be running against
// One host is what makes the assertion mean something: with a second host the work would simply move there, and the restart would prove nothing
type workflowRestart struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster  *cluster.Cluster
	pipeline *workflow.Workflow

	// ran counts executions of the retrying step's handler, which is what shows the task ran again after the restart
	ran atomic.Int32
	// firstAttempt is closed by the step's first attempt, so the host is stopped once a retry is scheduled and not before
	firstAttempt chan struct{}
	// once keeps the first attempt's signal to a single close, since the step runs more than once
	once sync.Once
}

// retryGap is how long the retrying step waits before its next attempt
// It is comfortably longer than stopping a host and bringing it back, so the retry cannot fire in the middle of that
const retryGap = 10 * time.Second

func (s *workflowRestart) Name() string {
	return "workflowrestart/" + string(s.kind) + "/" + string(s.variant)
}

func (s *workflowRestart) Setup(t *testing.T) []framework.Option {
	s.firstAttempt = make(chan struct{})

	pipeline, err := workflow.New("e2e-restart",
		workflow.WithTimeout(10*time.Minute),
		workflow.WithRetention(workflow.RetentionPolicy{Completed: time.Hour, Failed: time.Hour, Cancelled: time.Hour}),
		workflow.WithSteps(
			// Completes before the restart, so its recorded output is there to be read back afterwards
			workflow.Step("first", workflow.WithRun(s.record)),
			// Fails once, leaving a retry scheduled that only a running host can pick up
			workflow.Step("work",
				workflow.WithRun(s.work),
				workflow.WithMaxAttempts(5),
				// The gap is long enough that the retry cannot fire while the host is being stopped and brought back
				workflow.WithRetryBackoff(retryGap, retryGap),
			),
		),
	)
	require.NoError(t, err)
	s.pipeline = pipeline

	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              1,
		BuiltInActors:      []builtinactor.BuiltInActor{pipeline},
		AlarmsPollInterval: pollInterval,
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

// record returns a value the test reads back after the restart, which is how the recorded output is shown to have survived
func (s *workflowRestart) record(ctx context.Context, t workflow.Task) (any, error) {
	return "before-restart", nil
}

// work fails its first attempt and succeeds on the retry
//
// The failure leaves a retry scheduled retryGap into the future, which is the outstanding work the host is stopped on top of
// Only a running host can execute it, so a second execution proves the restarted host picked the instance up rather than the test having raced ahead of the stop
func (s *workflowRestart) work(ctx context.Context, t workflow.Task) (any, error) {
	n := s.ran.Add(1)
	if n > 1 {
		return "after-restart", nil
	}

	s.once.Do(func() { close(s.firstAttempt) })
	return nil, errors.New("induced failure, so a retry is left scheduled")
}

func (s *workflowRestart) Run(t *testing.T) {
	ctx := t.Context()
	svc := s.pipeline.Service(s.cluster.Service(0))

	const id = "restart-1"
	_, created, err := svc.Start(ctx, nil, workflow.WithInstanceID(id))
	require.NoError(t, err)
	require.True(t, created)

	// Wait for the first attempt to fail, which leaves a retry scheduled retryGap out
	select {
	case <-s.firstAttempt:
	case <-time.After(eventuallyTimeout):
		t.Fatal("the retrying step never ran")
	}

	before, err := svc.GetStatus(ctx, id)
	require.NoError(t, err)
	require.Equal(t, workflow.StepCompleted, s.stepStatus(t, before, "first"), "the first step should have completed before the restart")
	require.Equal(t, workflow.StepRunning, s.stepStatus(t, before, "work"), "the retrying step should still be open when the host stops")

	// Stop the only host, with the retry still owed: nothing in the cluster can advance the instance now
	s.cluster.Host(0).Stop(t)
	require.Equal(t, int32(1), s.ran.Load(), "the retry must not have run before the host stopped")

	// Bring it back on the same address, against the same store
	s.cluster.Host(0).Run(t)

	// The restarted host serves a new actor.Service
	restarted := s.pipeline.Service(s.cluster.Service(0))

	// The instance is the same one, and what it recorded before the restart is still recorded
	after, err := restarted.GetStatus(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, before.CreatedAt.UTC(), after.CreatedAt.UTC(), "the instance should be the one that was running, not a new run")
	assert.Equal(t, workflow.StepCompleted, s.stepStatus(t, after, "first"), "a step completed before the restart should survive it")

	// Nothing else is asked of the cluster: the restarted host picks the instance up and runs it to the end
	status := s.awaitCompleted(t, restarted, id)
	assert.Equal(t, workflow.StepCompleted, s.stepStatus(t, status, "work"), "the interrupted step should complete on the restarted host")
	assert.Greater(t, s.ran.Load(), int32(1), "the owed retry should have run on the restarted host")

	var out string
	err = status.DecodeOutput(&out)
	require.NoError(t, err)
	assert.Equal(t, "after-restart", out)
}

// stepStatus returns one step's status by name
func (s *workflowRestart) stepStatus(t *testing.T, status workflow.InstanceStatus, name string) workflow.StepStatus {
	t.Helper()

	for _, step := range status.Steps {
		if step.Name == name {
			return step.Status
		}
	}

	require.FailNowf(t, "missing step", "status has no step %q", name)
	return ""
}

// awaitCompleted polls until the instance completes, reporting where it stopped if it does not
func (s *workflowRestart) awaitCompleted(t *testing.T, svc *workflow.WorkflowService, id string) workflow.InstanceStatus {
	t.Helper()

	var (
		last    workflow.InstanceStatus
		lastErr error
	)

	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		status, err := svc.GetStatus(t.Context(), id)
		switch {
		case err != nil:
			lastErr = err
		case status.Status == workflow.StatusCompleted:
			return status
		default:
			last, lastErr = status, nil
		}
		time.Sleep(eventuallyTick)
	}

	require.FailNowf(t, "the restarted instance did not complete",
		"instance %s should complete, last status was %q on step %q (last error: %v)", id, last.Status, last.CurrentStep, lastErr)
	return last
}
