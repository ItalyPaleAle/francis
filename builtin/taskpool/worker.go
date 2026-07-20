package taskpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/italypaleale/francis/actor"
)

// worker is one task's worker: a single actor instance, created per task, that runs the pool's handler once and then halts itself
// It implements actor.ActorJob, and the submitted task is delivered as a run job, which the public client cannot reach because the Service rejects built-in actor types
type worker struct {
	// handler runs the task
	handler func(ctx context.Context, task Task) error
	// accept, when set, decides whether this host runs the task or declines it for re-routing
	accept func(ctx context.Context, task Task) bool
	// actorID is this worker's actor ID, which is also the task ID
	actorID string
	// capability is the capability this worker's queue serves, empty for the base queue
	capability string
	// log reports task start and completion events
	log *slog.Logger
	// client is a privileged client bound to this worker, used to halt it once the task is done
	client actor.Client[struct{}]
}

// Job runs the submitted task, then halts the worker on a terminal outcome so its host capacity frees up for the next task
func (w *worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	err := w.run(ctx, method, data)

	// Free the host's slot promptly on a terminal outcome so the next task can start here without waiting for the idle timeout
	// A retryable error is left to run again on this same host, and a reject is handled (and the actor halted) by the framework, so neither halts here
	if err == nil || errors.Is(err, actor.ErrJobPermanentFailure) {
		w.client.Halt()
	}

	return err
}

// run performs the accept check and runs the handler, timing the execution for the logs
func (w *worker) run(ctx context.Context, method string, data actor.Envelope) error {
	if method != methodRun {
		// Only run jobs are dispatched to a task pool worker, so an unknown method is a programming error and should not retry forever
		return fmt.Errorf("%w: unknown task pool method %q", actor.ErrJobPermanentFailure, method)
	}

	task := &taskEnvelope{
		id:         w.actorID,
		capability: w.capability,
		data:       data,
	}

	// A host may decline a task it should not run, handing it to another host without counting an attempt
	if w.accept != nil && !w.accept(ctx, task) {
		if w.log != nil {
			w.log.Debug("Task rejected by host; re-routing", slog.String("taskID", w.actorID), slog.String("capability", w.capability))
		}
		return actor.ErrJobRejected
	}

	if w.log != nil {
		w.log.Info("Task started", slog.String("taskID", w.actorID), slog.String("capability", w.capability))
	}
	start := time.Now()

	err := w.handler(ctx, task)

	duration := time.Since(start)
	if err != nil {
		if w.log != nil {
			w.log.Warn("Task completed with error", slog.String("taskID", w.actorID), slog.Duration("duration", duration), slog.Any("error", err))
		}
		return err
	}

	if w.log != nil {
		w.log.Info("Task completed", slog.String("taskID", w.actorID), slog.Duration("duration", duration))
	}

	return nil
}
