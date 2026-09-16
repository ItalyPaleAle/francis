package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type performanceHost struct {
	*fakeHost
	dispatchCalls map[string]int
	failAck       bool
}

func (h *performanceHost) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, props actor.JobProperties) (string, bool, error) {
	// Count provider requests before idempotency coalescing so repeated dispatch work remains visible
	h.mu.Lock()
	h.dispatchCalls[method]++
	h.mu.Unlock()
	return h.fakeHost.Dispatch(ctx, actorType, actorID, method, data, props)
}

func (h *performanceHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	// Reject only acknowledgement writes to reproduce a crash after job acceptance but before its marker becomes durable
	st, ok := state.(instanceState)
	if h.failAck && ok {
		for _, sr := range st.Steps {
			for _, tr := range sr.Tasks {
				if tr.DispatchedAttempt > 0 {
					return errors.New("injected dispatch acknowledgement failure")
				}
			}
		}
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func TestHardeningFanOutDispatchCallsGrowLinearly(t *testing.T) {
	for _, width := range []int{100, 200, 400} {
		for _, kind := range []string{"worker", "child"} {
			for _, window := range []int{0, 8} {
				t.Run(fmt.Sprintf("%s/%d/window-%d", kind, width, window), func(t *testing.T) {
					// Keep the provider jobs live so the old quadratic dispatch loop would still be counted despite coalescing
					host := &performanceHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
					work := ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithCompensate(noopCompensate), WithMaxParallel(window))
					forwardMethod := methodRun
					undoMethod := methodCompensate
					if kind == "child" {
						child, err := New("performance-child", WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))))
						require.NoError(t, err)
						work = ForEach("work", WithItemsFrom("plan"), WithChild(child), WithMaxParallel(window))
						forwardMethod = methodStart
						undoMethod = methodUnwind
					}
					wf, err := New("performance-fanout", WithSteps(
						Step("plan", WithRun(noopRun)),
						work,
						WaitForEvent("hold"),
					))
					require.NoError(t, err)
					svc := actor.NewService(host)
					o := newReviewOrchestrator(t, wf, "instance", svc)
					err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
					require.NoError(t, err)
					items := make([]int, width)
					encoded, err := json.Marshal(items)
					require.NoError(t, err)
					err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "plan", Attempt: 1, Output: encoded}})
					require.NoError(t, err)

					// Every result uses a fresh activation, proving the dispatch bound comes from durable markers
					for index := range width {
						o = newReviewOrchestrator(t, wf, "instance", svc)
						err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Index: index, Attempt: 1, Output: json.RawMessage(`"effect"`)}})
						require.NoError(t, err)
					}
					expectedForward := width
					if kind == "worker" {
						expectedForward++
					}
					assert.Equal(t, expectedForward, host.dispatchCalls[forwardMethod])

					// A wide compensation frame must also dispatch each member only once across sequential acknowledgements
					err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "undo"}})
					require.NoError(t, err)
					for index := range width {
						o = newReviewOrchestrator(t, wf, "instance", svc)
						err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Index: index, Attempt: 1}})
						require.NoError(t, err)
					}
					assert.Equal(t, width, host.dispatchCalls[undoMethod])
					st := readJournal(t, host.fakeHost, wf, "instance")
					assert.Equal(t, StatusCancelled, st.Status)
					assert.Equal(t, CompensationCompleted, st.Compensation)
				})
			}
		}
	}
}

func TestHardeningDispatchAcknowledgementFailureDoesNotPoisonRetry(t *testing.T) {
	// Accept the first task's job but fail the subsequent acknowledgement write
	host := &performanceHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}, failAck: true}
	wf, err := New("dispatch-ack-failure", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	svc := actor.NewService(host)
	o := newReviewOrchestrator(t, wf, "instance", svc)
	start := &payloadEnvelope{value: startPayload{Version: 1}}
	require.ErrorContains(t, o.Job(t.Context(), methodStart, start), "acknowledgement failure")
	st := readJournal(t, host.fakeHost, wf, "instance")
	assert.Zero(t, st.step("work").task(0).DispatchedAttempt)
	cached, err := o.client.GetState(t.Context())
	require.NoError(t, err)
	assert.Zero(t, cached.step("work").task(0).DispatchedAttempt)

	// The retry repeats the same idempotency key, then a new activation skips the durably acknowledged job
	host.failAck = false
	err = o.Job(t.Context(), methodStart, start)
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	assert.Len(t, host.dispatchedTo(builtinActorType(wf.workerType("")), workerActorID("instance", "work", 0)), 1)
	o = newReviewOrchestrator(t, wf, "instance", svc)
	err = o.Job(t.Context(), methodStart, start)
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	st = readJournal(t, host.fakeHost, wf, "instance")
	assert.Equal(t, 1, st.step("work").task(0).DispatchedAttempt)
}

func TestHardeningDispatchMarkersPermitNewAttempts(t *testing.T) {
	// A transport failure reported by a dead-lettered worker must still schedule the engine's next attempt
	host := &performanceHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
	wf, err := New("dispatch-attempts", WithSteps(
		Step("work", WithRun(noopRun), WithCompensate(noopCompensate)),
		WaitForEvent("hold"),
	))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 1, Error: "transport exhausted", Retryable: true}})
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodRun])
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 2}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "undo"}})
	require.NoError(t, err)

	// Compensation retries use their own durable attempt acknowledgement
	err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Attempt: 1, Error: "transport exhausted", Retryable: true}})
	require.NoError(t, err)
	assert.Equal(t, 2, host.dispatchCalls[methodCompensate])
	err = o.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "work", Attempt: 2}})
	require.NoError(t, err)
	assert.Equal(t, CompensationCompleted, readJournal(t, host.fakeHost, wf, "instance").Compensation)
}

func TestHardeningOversizedDispatchAcknowledgementRecordsTermination(t *testing.T) {
	// Measure the journal before its first dispatch acknowledgement so the marker alone crosses the configured cap
	baselineHost := newFakeHost()
	baselineWF, err := New("oversized-markers", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	baselineActor := newTestOrchestrator(t, baselineWF, baselineHost, "instance")
	err = baselineActor.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	baseline := readJournal(t, baselineHost, baselineWF, "instance")
	baseline.step("work").task(0).DispatchedAttempt = 0
	baseline.encoded = nil
	limit, err := journalSize(&baseline)
	require.NoError(t, err)

	// The second state write may terminate the instance after the start transition was already counted
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		err := provider.Shutdown(t.Context())
		require.NoError(t, err)
	})
	host := &performanceHost{fakeHost: newFakeHost(), dispatchCalls: map[string]int{}}
	wf, err := New("oversized-markers", WithMaxJournalSize(limit), WithMeter(provider.Meter("markers")), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	st := readJournal(t, host.fakeHost, wf, "instance")
	assert.Equal(t, 1, host.dispatchCalls[methodRun], "the ordinary journal must fit until dispatch is acknowledged")
	assert.Equal(t, StatusFailed, st.Status)
	assert.Contains(t, st.Cause, ErrJournalTooLarge.Error())
	assert.Equal(t, int64(0), int64MetricTotal(t, reader, "francis.workflow.instances.running"))
	assert.Equal(t, int64(1), int64MetricTotal(t, reader, "francis.workflow.instances.terminated"))
}

func TestHardeningFanOutShipsSourceArrayOnlyWhenRequested(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit-%t", explicit), func(t *testing.T) {
			// The item is sufficient for an ordinary fan-out handler, while explicit input dependencies retain their contract
			now := time.Now()
			work := ForEach("work", WithItemsFrom("plan"), WithRun(noopRun), WithCompensate(noopCompensate))
			if explicit {
				work = work.With(WithInputFrom("plan"))
			}
			def := testDefinition(t, "fanout-payload", WithSteps(Step("plan", WithRun(noopRun)), work))
			st := startJournal(t, def, now)
			reportSuccess(t, st, def, "plan", 0, []string{"first", "second"}, now)
			advance(st, def, "instance", now)
			o := &orchestrator{instanceID: "instance", def: def, wf: &Workflow{baseType: "workflow-payload"}}
			sr := st.step("work")
			d := def.byName["work"]
			payload := o.buildRunPayload(st, sr, d, d, sr.task(0))
			assert.JSONEq(t, `"first"`, string(payload.Item))
			if explicit {
				assert.JSONEq(t, `["first","second"]`, string(payload.Outputs["plan"]))
			} else {
				assert.NotContains(t, payload.Outputs, "plan")
			}
		})
	}
}
