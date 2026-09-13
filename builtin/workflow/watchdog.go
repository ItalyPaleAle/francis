package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// scanDeadLetters is the watchdog's recovery pass, and is what makes a lost report something the engine heals rather than something an operator does
//
// A worker's job dead-letters only when it could not report: five failed report dispatches, or a host that died mid-attempt more than five times
// Dead-lettering frees the job's idempotency key, so reconcile must never redispatch a scheduled-not-done task without first asking whether its previous job dead-lettered, or a task that cannot report would be run again on every turn
//
// A worker's dead-letter is folded into the journal as one failed attempt of transport kind and its record removed, which lets reconcile dispatch the next attempt under its own key in the same turn
// The instance's own dead-lettered reports and events are retried instead, because nothing else can re-derive them: a lost "done" is not something reconcile can reconstruct
func (o *orchestrator) scanDeadLetters(ctx context.Context, st *instanceState, now time.Time) error {
	err := o.scanWorkerDeadLetters(ctx, st, now)
	if err != nil {
		return err
	}

	return o.scanOwnDeadLetters(ctx)
}

// scanWorkerDeadLetters looks for a dead-lettered run or compensate job on the worker of every scheduled-not-done task
func (o *orchestrator) scanWorkerDeadLetters(ctx context.Context, st *instanceState, now time.Time) error {
	for i := range st.Steps {
		sr := &st.Steps[i]
		if sr.Status != StepRunning && sr.Status != StepCompensating {
			continue
		}

		d := o.def.byName[sr.Name]
		if d == nil {
			continue
		}

		for j := range sr.Tasks {
			tr := &sr.Tasks[j]
			compensating := sr.Status == StepCompensating

			// Only a task with work outstanding can have a dead-letter worth recovering
			if compensating {
				if tr.Comp == nil || tr.Comp.Done {
					continue
				}
			} else if tr.Done {
				continue
			}

			err := o.recoverTaskDeadLetter(ctx, st, sr, d, tr, compensating, now)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// recoverTaskDeadLetter folds one task's dead-lettered job into the journal and removes its record
func (o *orchestrator) recoverTaskDeadLetter(ctx context.Context, st *instanceState, sr *stepRecord, d *stepDef, tr *taskRecord, compensating bool, now time.Time) error {
	member := memberDef(d, tr.Index)

	// A child task's work runs in its own instance rather than on a worker, so there is no worker to scan
	if member.kind == KindChild || (d.kind == KindForEach && d.child != nil) {
		return nil
	}

	bareType := o.wf.workerType(member.capability)
	method := methodRun
	if compensating {
		bareType = o.wf.undoType(member.capability)
		method = methodCompensate
	}

	client := builtinactor.NewClient[struct{}](bareType, workerActorID(o.instanceID, sr.Name, tr.Index), o.svc)
	jobs, err := client.ListJobs(ctx)
	if err != nil {
		// A listing that fails is not worth failing the turn over: the next tick looks again, and the deadline remains the backstop
		if o.log != nil {
			o.log.WarnContext(ctx, "Failed to list a worker's jobs during the watchdog scan", slog.String("step", sr.Name), slog.Int("index", tr.Index), slog.Any("error", err))
		}
		return nil
	}

	for _, j := range jobs {
		if j.Status != actor.JobStatusDeadLettered || j.Method != method {
			continue
		}

		// The same dead job seen twice must not be counted twice, which is what the recorded job ID guards against when a crash lands between the journal write and the removal
		alreadyFolded := (compensating && tr.Comp != nil && tr.Comp.DeadJobID == j.JobID) || (!compensating && tr.DeadJobID == j.JobID)
		if !alreadyFolded {
			o.foldTransportFailure(ctx, st, sr, tr, compensating, j, now)
		}

		// Removing the record is what keeps the invariant that no dead-letter outlives the journal entry accounting for it, which in turn is what lets Purge be a bounded sweep rather than a search
		dErr := o.svc.DeleteJob(ctx, j.JobID)
		if dErr != nil && !errors.Is(dErr, actor.ErrJobNotFound) {
			return fmt.Errorf("failed to remove the dead-lettered job %s: %w", j.JobID, dErr)
		}

		o.wf.metrics.deadLettersRecovered.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow", o.def.name),
			attribute.String("step", sr.Name),
		))
	}

	return nil
}

// foldTransportFailure records a dead-lettered job as one failed attempt of transport kind, which the step's own policy then treats like any other retryable failure
func (o *orchestrator) foldTransportFailure(ctx context.Context, st *instanceState, sr *stepRecord, tr *taskRecord, compensating bool, j actor.JobInfo, now time.Time) {
	errMsg := "task could not report its outcome"
	if j.LastError != "" {
		errMsg += ": " + j.LastError
	}

	if compensating {
		tr.Comp.DeadJobID = j.JobID
		applyCompReport(st, o.def, &compReportPayload{
			Step:      sr.Name,
			Index:     tr.Index,
			Attempt:   tr.Comp.Attempts,
			Error:     errMsg,
			Retryable: true,
			Transport: true,
		}, now)
	} else {
		tr.DeadJobID = j.JobID
		applyReport(st, o.def, &reportPayload{
			Step:      sr.Name,
			Index:     tr.Index,
			Attempt:   tr.Attempts,
			Error:     errMsg,
			Retryable: true,
			Transport: true,
		}, now)
	}

	o.wf.metrics.transportFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", o.def.name),
		attribute.String("step", sr.Name),
	))

	if o.log != nil {
		o.log.WarnContext(ctx, "Recovered a dead-lettered task job",
			slog.String("step", sr.Name), slog.Int("index", tr.Index), slog.String("jobID", j.JobID), slog.String("lastError", j.LastError))
	}
}

// scanOwnDeadLetters retries the instance's own dead-lettered reports and events, which nothing else can re-derive
func (o *orchestrator) scanOwnDeadLetters(ctx context.Context) error {
	jobs, err := o.client.ListJobs(ctx)
	if err != nil {
		if o.log != nil {
			o.log.WarnContext(ctx, "Failed to list the instance's jobs during the watchdog scan", slog.Any("error", err))
		}
		return nil
	}

	for _, j := range jobs {
		if j.Status != actor.JobStatusDeadLettered {
			continue
		}

		switch j.Method {
		case methodDone, methodCompensated, methodEvent, methodStart, methodCancel, methodUnwind, methodSuspend, methodResume:
			// RetryJob re-dispatches and removes the record atomically, so a crash cannot leave the work lost or the record duplicated
			_, rErr := o.client.RetryJob(ctx, j.JobID)
			if rErr != nil && !errors.Is(rErr, actor.ErrJobNotFound) {
				return fmt.Errorf("failed to retry the dead-lettered job %s: %w", j.JobID, rErr)
			}
			o.wf.metrics.deadLettersRecovered.Add(ctx, 1, metric.WithAttributes(
				attribute.String("workflow", o.def.name),
				attribute.String("method", j.Method),
			))
		default:
			// A dead-lettered tick is dropped rather than retried, since the recurrence itself carries on
			dErr := o.svc.DeleteJob(ctx, j.JobID)
			if dErr != nil && !errors.Is(dErr, actor.ErrJobNotFound) {
				return fmt.Errorf("failed to remove the dead-lettered job %s: %w", j.JobID, dErr)
			}
		}
	}

	return nil
}

// armWatchdog dispatches the repeating job an instance uses to re-run advance and reconcile, re-arm its deadline, and scan for dead-letters
// It is a job rather than an alarm because a repeating job survives a failed occurrence, whereas an alarm whose handler fails its attempts is deleted, repeating or not, and the watchdog's whole purpose is to survive the conditions that make handlers fail
func (o *orchestrator) armWatchdog(ctx context.Context, st *instanceState) error {
	if o.def.watchdog <= 0 {
		return nil
	}

	interval := isoInterval(o.def.watchdog)
	_, err := o.client.Dispatch(ctx, methodTick, nil,
		actor.WithIdempotencyKey(keyWatchdog),
		actor.WithJobInterval(interval),
		actor.WithJobDelay(o.def.watchdog),
		actor.WithJobTTL(instanceDeadline(st, o.def)),
	)
	if err != nil {
		return fmt.Errorf("failed to arm the watchdog: %w", err)
	}
	return nil
}

// ensureWatchdog arms the repeating watchdog job once per activation, which its idempotency key makes safe to call on every turn
func (o *orchestrator) ensureWatchdog(ctx context.Context, st *instanceState) error {
	if o.watchdogArmed {
		return nil
	}

	err := o.armWatchdog(ctx, st)
	if err != nil {
		return err
	}

	o.watchdogArmed = true
	return nil
}

// isoInterval renders a duration as the ISO8601 form the job scheduler takes
func isoInterval(d time.Duration) string {
	return fmt.Sprintf("PT%dS", int(d.Round(time.Second).Seconds()))
}
