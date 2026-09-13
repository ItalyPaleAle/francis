package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/tracing"
)

// worker performs one attempt of one task and reports the outcome back to the orchestrator
// Every user RunFunc and CompensateFunc runs here, and every external call with it, which is what keeps the Workflow actor free to accept the next report, the cancel, and its own deadline
//
// A worker is stateless apart from one thing: it keeps the result of the attempt it just ran for the life of its activation, so a retried report does not run the handler again
type worker struct {
	wf       *Workflow
	def      *definition
	bareType string
	actorID  string
	svc      *actor.Service
	log      *slog.Logger
	// undo marks the compensation half, which is a separate actor type so compensations have their own per-host budget
	undo bool
	// client is a privileged client bound to this worker, used to halt it once it has reported
	client actor.Client[struct{}]

	// mu guards the memoized attempt result
	mu sync.Mutex
	// result is the outcome of the attempt this activation ran, kept so a report Francis retries re-sends it rather than running the handler a second time
	result *attemptResult
}

// attemptResult is what one attempt produced, memoized for the life of the activation
type attemptResult struct {
	key    string
	output json.RawMessage
	err    error
}

// newWorker builds a worker of either the forward or the undo family
func newWorker(wf *Workflow, bareType string, actorID string, svc *actor.Service, undo bool) actor.Actor {
	log := wf.log
	if log != nil {
		log = log.With(slog.String("worker", actorID))
	}

	return &worker{
		wf:       wf,
		def:      wf.def,
		bareType: bareType,
		actorID:  actorID,
		svc:      svc,
		log:      log,
		undo:     undo,
		client:   builtinactor.NewClient[struct{}](bareType, actorID, svc),
	}
}

// Job runs one attempt and dispatches its report, then halts so the host's slot frees up immediately rather than at the idle timeout
// It returns an error to Francis in exactly one situation: the report dispatch itself failed
func (w *worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	if method != methodRun && method != methodCompensate {
		// Only these two methods reach a worker, so anything else is a programming error and should not retry forever
		return fmt.Errorf("%w: unknown workflow worker method %q", actor.ErrJobPermanentFailure, method)
	}

	var p runPayload
	err := decodePayload(data, &p)
	if err != nil {
		return err
	}

	// A host whose code does not match the graph registered for the version declines the task, so it runs where the handlers match
	ok, err := w.wf.serveVersion(ctx, w.svc, p.Version)
	if err != nil {
		return err
	}
	if !ok {
		return actor.ErrJobRejected
	}

	res := w.runAttempt(ctx, method, &p)
	w.recordAttempt(ctx, &p, res.err != nil)

	// A rejection is the handler declining this host, so it goes back to Francis to be re-routed without counting an attempt
	if errors.Is(res.err, actor.ErrJobRejected) {
		return actor.ErrJobRejected
	}

	err = w.report(ctx, method, &p, res)
	if err != nil {
		// The attempt's result stays memoized, so the retried job re-sends the report rather than running the handler again
		return fmt.Errorf("failed to report the task outcome: %w", err)
	}

	w.client.Halt()
	return nil
}

// runAttempt runs the handler once, or returns the result this activation already produced for the same attempt
func (w *worker) runAttempt(ctx context.Context, method string, p *runPayload) *attemptResult {
	key := fmt.Sprintf("%s|%s|%d|%d", method, p.Step, p.Index, p.Attempt)

	w.mu.Lock()
	if w.result != nil && w.result.key == key {
		res := w.result
		w.mu.Unlock()
		return res
	}
	w.mu.Unlock()

	res := w.invokeHandler(ctx, method, p, key)

	w.mu.Lock()
	w.result = res
	w.mu.Unlock()

	return res
}

// invokeHandler resolves the step's handler and runs it, timing the attempt and tagging its span with the task's identity
func (w *worker) invokeHandler(ctx context.Context, method string, p *runPayload, key string) *attemptResult {
	res := &attemptResult{key: key}

	d := w.def.byName[p.Step]
	if d == nil {
		res.err = fmt.Errorf("%w: this host's definition has no step %q", actor.ErrJobPermanentFailure, p.Step)
		return res
	}
	if p.Handler != "" {
		member := w.def.byName[p.Handler]
		if member == nil {
			res.err = fmt.Errorf("%w: this host's definition has no step %q", actor.ErrJobPermanentFailure, p.Handler)
			return res
		}
		d = member
	}

	ctx, span := tracing.Start(ctx, "workflow.attempt", trace.WithAttributes(
		attribute.String("francis.workflow.name", p.Workflow),
		attribute.String("francis.workflow.instance", p.InstanceID),
		attribute.String("francis.workflow.step", p.Step),
		attribute.Int("francis.workflow.index", p.Index),
		attribute.Int("francis.workflow.attempt", p.Attempt),
	))
	start := time.Now()

	task := &taskEnvelope{p: p, undo: w.undo}

	if method == methodCompensate {
		res.err = w.runCompensate(ctx, d, task)
	} else {
		res.output, res.err = w.runForward(ctx, d, task)
	}

	tracing.End(span, res.err)

	if w.log != nil {
		attrs := []any{
			slog.String("instanceID", p.InstanceID),
			slog.String("step", p.Step),
			slog.Int("index", p.Index),
			slog.Int("attempt", p.Attempt),
			slog.Duration("duration", time.Since(start)),
		}
		if res.err != nil {
			w.log.WarnContext(ctx, "Workflow task attempt failed", append(attrs, slog.Any("error", res.err))...)
		} else {
			w.log.DebugContext(ctx, "Workflow task attempt completed", attrs...)
		}
	}

	return res
}

// runForward runs a step's handler and encodes what it returned, enforcing the output cap here rather than on the orchestrator
// Checking the size on the worker is what keeps the orchestrator from ever spending a turn serializing something unbounded
func (w *worker) runForward(ctx context.Context, d *stepDef, task *taskEnvelope) (json.RawMessage, error) {
	if d.run == nil {
		return nil, fmt.Errorf("%w: step %q has no handler on this host", actor.ErrJobPermanentFailure, d.name)
	}

	out, err := d.run(ctx, task)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}

	enc, err := json.Marshal(out)
	if err != nil {
		// A value that cannot be encoded fails the same way on every attempt
		return nil, fmt.Errorf("%w: failed to encode the task output: %w", actor.ErrJobPermanentFailure, err)
	}

	limit := task.p.MaxOutputSize
	if limit > 0 && len(enc) > limit {
		return nil, fmt.Errorf("%w: %w: %d bytes exceeds the %d byte limit", actor.ErrJobPermanentFailure, ErrOutputTooLarge, len(enc), limit)
	}

	return enc, nil
}

// runCompensate runs a step's compensation, which undoes the effect of a task that had completed successfully
func (w *worker) runCompensate(ctx context.Context, d *stepDef, task *taskEnvelope) error {
	if d.compensate == nil {
		// A frame with nothing to undo is compensated by doing nothing, which is how a group whose members compensate selectively works
		return nil
	}
	return d.compensate(ctx, task)
}

// report dispatches the attempt's outcome back to the orchestrator, keyed by step, index, and attempt so a late report can never be mistaken for a newer one's
func (w *worker) report(ctx context.Context, method string, p *runPayload, res *attemptResult) error {
	client := builtinactor.NewClient[struct{}](p.OrchestratorType, p.InstanceID, w.svc)

	errMsg := ""
	retryable := false
	if res.err != nil {
		errMsg = res.err.Error()
		// An ordinary error says another attempt could succeed, and a permanent failure says it could not
		retryable = !errors.Is(res.err, actor.ErrJobPermanentFailure)
	}

	if method == methodCompensate {
		key := fmt.Sprintf("comp%s%s%s%d%s%d", idDelimiter, p.Step, idDelimiter, p.Index, idDelimiter, p.Attempt)
		_, err := client.Dispatch(ctx, methodCompensated, compReportPayload{
			Step:        p.Step,
			Index:       p.Index,
			Attempt:     p.Attempt,
			Error:       errMsg,
			Retryable:   retryable,
			TraceParent: p.TraceParent,
		}, actor.WithIdempotencyKey(key))
		return err
	}

	key := fmt.Sprintf("done%s%s%s%d%s%d", idDelimiter, p.Step, idDelimiter, p.Index, idDelimiter, p.Attempt)
	_, err := client.Dispatch(ctx, methodDone, reportPayload{
		Step:        p.Step,
		Index:       p.Index,
		Attempt:     p.Attempt,
		Output:      res.output,
		Error:       errMsg,
		Retryable:   retryable,
		TraceParent: p.TraceParent,
	}, actor.WithIdempotencyKey(key))
	return err
}

// JobFailed reports a dead-lettered attempt to the orchestrator, which is the fast path for a worker that could not report
// It is best-effort and it is the only thing that can recover the attempt, since a dead-lettered job takes its payload with it
func (w *worker) JobFailed(ctx context.Context, _ string, method string, data actor.Envelope, jobErr error) error {
	if method != methodRun && method != methodCompensate {
		return nil
	}

	var p runPayload
	err := decodePayload(data, &p)
	if err != nil {
		return err
	}

	errMsg := "task could not report its outcome"
	if jobErr != nil {
		errMsg += ": " + jobErr.Error()
	}

	client := builtinactor.NewClient[struct{}](p.OrchestratorType, p.InstanceID, w.svc)

	if method == methodCompensate {
		key := fmt.Sprintf("comp%s%s%s%d%s%d%sdl", idDelimiter, p.Step, idDelimiter, p.Index, idDelimiter, p.Attempt, idDelimiter)
		_, err = client.Dispatch(ctx, methodCompensated, compReportPayload{
			Step:      p.Step,
			Index:     p.Index,
			Attempt:   p.Attempt,
			Error:     errMsg,
			Retryable: true,
			Transport: true,
		}, actor.WithIdempotencyKey(key))
		return err
	}

	key := fmt.Sprintf("done%s%s%s%d%s%d%sdl", idDelimiter, p.Step, idDelimiter, p.Index, idDelimiter, p.Attempt, idDelimiter)
	_, err = client.Dispatch(ctx, methodDone, reportPayload{
		Step:      p.Step,
		Index:     p.Index,
		Attempt:   p.Attempt,
		Error:     errMsg,
		Retryable: true,
		Transport: true,
	}, actor.WithIdempotencyKey(key))
	return err
}

// Task is what a step's handler receives: the task's identity, the data the step declared it needs, and nothing else
type Task interface {
	// InstanceID returns the ID of the workflow instance this task belongs to
	InstanceID() string
	// Workflow returns the name of the workflow being run
	Workflow() string
	// Step returns the name of the step this task belongs to
	Step() string
	// Index is the position within a parallel group or fan-out, and -1 for a plain step
	Index() int
	// Attempt is 1 on the first execution and increases with each retry, as recorded in the journal
	Attempt() int

	// DecodeInput reads the workflow input, as given to Start
	DecodeInput(into any) error
	// DecodeItem reads this task's fan-out item, and is a no-op for a step that is not a fan-out
	DecodeItem(into any) error
	// DecodeOutput reads the output of an upstream step, which must be the preceding step or one named with WithInputFrom
	// It returns ErrStepSkipped when that step was skipped, and ErrStepNotFound when the step is not one this task may read
	DecodeOutput(step string, into any) error
}

// Compensation is what a step's compensation receives: everything a Task carries, plus the result of the task being undone and why the workflow is unwinding
type Compensation interface {
	Task
	// DecodeResult reads the output this task produced when it succeeded, which is usually what identifies the effect to undo
	DecodeResult(into any) error
	// Cause is the error that caused the workflow to unwind, or the cancellation reason
	Cause() string
}

// taskEnvelope is the concrete Task and Compensation handed to a handler, backed by the job payload the orchestrator built
type taskEnvelope struct {
	p    *runPayload
	undo bool
}

// InstanceID returns the ID of the workflow instance this task belongs to
func (t *taskEnvelope) InstanceID() string {
	return t.p.InstanceID
}

// Workflow returns the name of the workflow being run
func (t *taskEnvelope) Workflow() string {
	return t.p.Workflow
}

// Step returns the name of the step this task belongs to
func (t *taskEnvelope) Step() string {
	return t.p.Step
}

// Index is the position within a parallel group or fan-out, and -1 for a plain step
func (t *taskEnvelope) Index() int {
	if !t.p.Positional {
		return -1
	}
	return t.p.Index
}

// Attempt is 1 on the first execution and increases with each retry
func (t *taskEnvelope) Attempt() int {
	return t.p.Attempt
}

// DecodeInput reads the workflow input, as given to Start
func (t *taskEnvelope) DecodeInput(into any) error {
	if len(t.p.Input) == 0 {
		return nil
	}
	return json.Unmarshal(t.p.Input, into)
}

// DecodeItem reads this task's fan-out item, and is a no-op for a step that is not a fan-out
func (t *taskEnvelope) DecodeItem(into any) error {
	if len(t.p.Item) == 0 {
		return nil
	}
	return json.Unmarshal(t.p.Item, into)
}

// DecodeOutput reads the output of an upstream step this task was given
func (t *taskEnvelope) DecodeOutput(step string, into any) error {
	if slices.Contains(t.p.Skipped, step) {
		return fmt.Errorf("%w: %q", ErrStepSkipped, step)
	}

	out, ok := t.p.Outputs[step]
	if !ok {
		return fmt.Errorf("%w: %q is neither the preceding step nor named with WithInputFrom", ErrStepNotFound, step)
	}
	if len(out) == 0 {
		return nil
	}
	return json.Unmarshal(out, into)
}

// DecodeResult reads the output the task being compensated produced when it succeeded
func (t *taskEnvelope) DecodeResult(into any) error {
	if len(t.p.Result) == 0 {
		return nil
	}
	return json.Unmarshal(t.p.Result, into)
}

// Cause is the error that caused the workflow to unwind, or the cancellation reason
func (t *taskEnvelope) Cause() string {
	return t.p.Cause
}

// recordAttempt is the instrument update a completed attempt calls for, recorded once the outcome is known
func (w *worker) recordAttempt(ctx context.Context, p *runPayload, failed bool) {
	w.wf.metrics.taskAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", p.Workflow),
		attribute.String("step", p.Step),
		attribute.Bool("failed", failed),
	))
}
