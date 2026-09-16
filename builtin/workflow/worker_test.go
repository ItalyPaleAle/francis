package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

// newTestWorker builds a worker of either family over the fake host, which is what every worker test drives
func newTestWorker(t *testing.T, wf *Workflow, host *fakeHost, undo bool) *worker {
	t.Helper()

	suffix := workerTypeSuffix
	if undo {
		suffix = undoTypeSuffix
	}

	svc := actor.NewService(host)
	w, ok := newWorker(wf, wf.baseType+suffix, "worker-1", svc, undo).(*worker)
	require.True(t, ok, "the worker factory should build a worker")
	return w
}

// runPayloadFor builds the payload the orchestrator would dispatch for one attempt of a step
func runPayloadFor(wf *Workflow, step string) *runPayload {
	return &runPayload{
		InstanceID:       "inst-1",
		Workflow:         wf.name,
		Version:          wf.def.version,
		Step:             step,
		Attempt:          1,
		OrchestratorType: wf.baseType,
	}
}

// reportedPayload returns the one report a worker dispatched, whatever actor type it addressed
func reportedPayload(t *testing.T, host *fakeHost, method string) any {
	t.Helper()

	host.mu.Lock()
	defer host.mu.Unlock()

	for id, j := range host.jobs {
		if j.Method == method {
			return host.jobPayloads[id]
		}
	}
	t.Fatalf("no %q job was dispatched", method)
	return nil
}

// reportedRun returns the forward report a worker dispatched
func reportedRun(t *testing.T, host *fakeHost) reportPayload {
	t.Helper()

	p, ok := reportedPayload(t, host, methodDone).(reportPayload)
	require.True(t, ok, "the done job should carry a report payload")
	return p
}

func TestWorkerRejectsAMethodItDoesNotServe(t *testing.T) {
	host := newFakeHost()
	wf, err := New("methods", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// Only run and compensate reach a worker, so anything else is a bug in the engine rather than something to retry forever
	err = w.Job(t.Context(), "whatever", &payloadEnvelope{value: runPayloadFor(wf, "a")})
	require.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	assert.Empty(t, host.jobs, "a method the worker does not serve reports nothing")
}

func TestWorkerDeclinesAVersionItDoesNotServe(t *testing.T) {
	host := newFakeHost()
	wf, err := New("versions", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// A host whose code does not match the instance's version declines the task so it runs where the handlers match
	p := runPayloadFor(wf, "a")
	p.Version = wf.def.version + 1
	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: p})
	require.ErrorIs(t, err, actor.ErrJobRejected)
	assert.Empty(t, host.jobs, "a declined task reports nothing, so no attempt is counted against it")
}

func TestWorkerDeclinesAVersionTheRegistryRefuses(t *testing.T) {
	host := newFakeHost()
	host.registryResponse = registerResponse{Found: true, OK: false}

	wf, err := New("conflict", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")})
	require.ErrorIs(t, err, actor.ErrJobRejected)
	assert.Empty(t, host.jobs)
}

func TestWorkerReportsAHandlerFailureAsRetryable(t *testing.T) {
	host := newFakeHost()
	wf, err := New("retryable", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		return nil, errors.New("upstream is down")
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")}))

	// An ordinary error says another attempt could succeed, and the step's own policy decides whether it gets one
	report := reportedRun(t, host)
	assert.Contains(t, report.Error, "upstream is down")
	assert.True(t, report.Retryable)
	assert.False(t, report.Transport)
}

func TestWorkerReportsAPermanentFailureAsFinal(t *testing.T) {
	host := newFakeHost()
	wf, err := New("permanent", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		return nil, actor.ErrJobPermanentFailure
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")}))

	report := reportedRun(t, host)
	assert.False(t, report.Retryable, "a permanent failure says no further attempt could succeed")
}

func TestWorkerPassesAHandlerRejectionBackToFrancis(t *testing.T) {
	host := newFakeHost()
	wf, err := New("rejection", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		return nil, actor.ErrJobRejected
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// A handler declining this host is not a failed attempt, so it goes back to Francis to be re-routed and nothing is reported
	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")})
	require.ErrorIs(t, err, actor.ErrJobRejected)
	assert.Empty(t, host.jobs)
}

func TestWorkerFailsPermanentlyOnAnOutputItCannotEncode(t *testing.T) {
	host := newFakeHost()
	wf, err := New("unencodable", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		return make(chan int), nil
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")}))

	// A value JSON cannot represent fails the same way on every attempt, so retrying it would only burn the step's budget
	report := reportedRun(t, host)
	assert.Contains(t, report.Error, "failed to encode the task output")
	assert.False(t, report.Retryable)
}

func TestWorkerEnforcesTheOutputCap(t *testing.T) {
	host := newFakeHost()
	wf, err := New("output-cap", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		return strings.Repeat("x", 100), nil
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// The cap is checked on the worker so the orchestrator never spends a turn serializing something unbounded
	p := runPayloadFor(wf, "a")
	p.MaxOutputSize = 16
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}))

	report := reportedRun(t, host)
	assert.Contains(t, report.Error, ErrOutputTooLarge.Error())
	assert.False(t, report.Retryable)
	assert.Empty(t, report.Output, "an output over the cap is never carried to the journal")
}

func TestWorkerFailsAStepItsOwnGraphDoesNotHave(t *testing.T) {
	host := newFakeHost()
	wf, err := New("graph-gap", WithSteps(Step("a", WithRun(noopRun)), WaitForEvent("w")))
	require.NoError(t, err)

	t.Run("a step that is not in the definition", func(t *testing.T) {
		w := newTestWorker(t, wf, host, false)
		require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "nope")}))

		report := reportedRun(t, host)
		assert.Contains(t, report.Error, `has no step "nope"`)
		assert.False(t, report.Retryable)
	})

	t.Run("a step with no handler on this host", func(t *testing.T) {
		host := newFakeHost()
		w := newTestWorker(t, wf, host, false)

		// A wait step is completed by RaiseEvent rather than run, so a forward task addressed to one means the graph and the dispatch disagree
		require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: runPayloadFor(wf, "w")}))

		report := reportedRun(t, host)
		assert.Contains(t, report.Error, "has no handler on this host")
		assert.False(t, report.Retryable)
	})

	t.Run("a group member that is not in the definition", func(t *testing.T) {
		host := newFakeHost()
		w := newTestWorker(t, wf, host, false)

		p := runPayloadFor(wf, "a")
		p.Handler = "not-a-member"
		require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}))

		report := reportedRun(t, host)
		assert.Contains(t, report.Error, `has no step "not-a-member"`)
		assert.False(t, report.Retryable)
	})
}

func TestWorkerRunsTheHandlerOnceWhenTheReportIsRetried(t *testing.T) {
	host := newFakeHost()

	var runs int
	wf, err := New("memoized", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		runs++
		return "once", nil
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)
	p := runPayloadFor(wf, "a")

	// The first delivery runs the handler and then fails to report, which is the case the memo exists for
	host.failDispatch = true
	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: p})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to report the task outcome")
	assert.Equal(t, 1, runs)

	// Francis retries the job, and the retry re-sends the memoized result rather than running the handler a second time
	host.failDispatch = false
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}))
	assert.Equal(t, 1, runs, "a retried report must not run the handler again")
	assert.JSONEq(t, `"once"`, string(reportedRun(t, host).Output))
}

func TestWorkerRunsTheHandlerAgainForTheNextAttempt(t *testing.T) {
	host := newFakeHost()

	var attempts []int
	wf, err := New("next-attempt", WithSteps(Step("a", WithRun(func(ctx context.Context, tk Task) (any, error) {
		attempts = append(attempts, tk.Attempt())
		return nil, errors.New("not yet")
	}))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// The memo is keyed by the attempt number, so the engine's next attempt runs the handler even on the same activation
	first := runPayloadFor(wf, "a")
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: first}))

	second := runPayloadFor(wf, "a")
	second.Attempt = 2
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: second}))

	assert.Equal(t, []int{1, 2}, attempts)
}

func TestWorkerCompensatesNothingWhenTheStepHasNoCompensation(t *testing.T) {
	host := newFakeHost()
	wf, err := New("no-undo", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, true)

	// A frame with nothing to undo is compensated by doing nothing, which is how a group whose members compensate selectively works
	require.NoError(t, w.Job(t.Context(), methodCompensate, &payloadEnvelope{value: runPayloadFor(wf, "a")}))

	report, ok := reportedPayload(t, host, methodCompensated).(compReportPayload)
	require.True(t, ok, "the compensated job should carry a compensation report")
	assert.Empty(t, report.Error)
}

func TestWorkerReportsAFailedCompensation(t *testing.T) {
	host := newFakeHost()
	wf, err := New("failing-undo", WithSteps(Step("a",
		WithRun(noopRun),
		WithCompensate(func(ctx context.Context, c Compensation) error {
			return errors.New("the reservation is gone")
		}),
	)))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, true)
	require.NoError(t, w.Job(t.Context(), methodCompensate, &payloadEnvelope{value: runPayloadFor(wf, "a")}))

	report, ok := reportedPayload(t, host, methodCompensated).(compReportPayload)
	require.True(t, ok)
	assert.Contains(t, report.Error, "the reservation is gone")
	assert.True(t, report.Retryable, "a compensation gets its own, more generous attempt policy")
}

func TestWorkerJobFailedReportsADeadLetteredAttempt(t *testing.T) {
	wf, err := New("dead-lettered", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	t.Run("a forward task", func(t *testing.T) {
		host := newFakeHost()
		w := newTestWorker(t, wf, host, false)

		// A dead-lettered worker result is not journaled, so this report is the only thing that can get the attempt back into the workflow
		err := w.JobFailed(t.Context(), "job-1", methodRun, &payloadEnvelope{value: runPayloadFor(wf, "a")}, errors.New("no host accepted it"))
		require.NoError(t, err)

		report := reportedRun(t, host)
		assert.Contains(t, report.Error, "task could not report its outcome")
		assert.Contains(t, report.Error, "no host accepted it")
		assert.True(t, report.Retryable)
		assert.True(t, report.Transport, "the attempt failed in transport rather than in the handler")
	})

	t.Run("a compensation", func(t *testing.T) {
		host := newFakeHost()
		w := newTestWorker(t, wf, host, true)

		err := w.JobFailed(t.Context(), "job-1", methodCompensate, &payloadEnvelope{value: runPayloadFor(wf, "a")}, nil)
		require.NoError(t, err)

		report, ok := reportedPayload(t, host, methodCompensated).(compReportPayload)
		require.True(t, ok)
		assert.Equal(t, "task could not report its outcome", report.Error)
		assert.True(t, report.Transport)
	})

	t.Run("a method the worker does not serve", func(t *testing.T) {
		host := newFakeHost()
		w := newTestWorker(t, wf, host, false)

		err := w.JobFailed(t.Context(), "job-1", "whatever", &payloadEnvelope{value: runPayloadFor(wf, "a")}, nil)
		require.NoError(t, err)
		assert.Empty(t, host.jobs, "there is no journal record for a method that never belonged to a task")
	})
}

func TestWorkerReportsAreKeyedSoALateOneCannotBeMistakenForANewerOne(t *testing.T) {
	host := newFakeHost()
	wf, err := New("keyed", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	// Two deliveries of the same attempt coalesce on the idempotency key, so a duplicate report never reaches the journal twice
	p := runPayloadFor(wf, "a")
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}))
	require.NoError(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}))

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.jobs, 1)
}

func TestWorkerRejectsAPayloadItCannotDecode(t *testing.T) {
	host := newFakeHost()
	wf, err := New("bad-payload", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	w := newTestWorker(t, wf, host, false)

	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: json.RawMessage(`"not a run payload"`)})
	require.Error(t, err)
	assert.Empty(t, host.jobs)
}
