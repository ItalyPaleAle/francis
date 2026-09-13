---
title: "Jobs"
weight: 27
---

A **job** is a durable, fire-and-forget task dispatched to a specific actor. A client (an external app, or another actor) dispatches a job to an actor `(type, id)`, which Francis then runs in the background on whatever host serves that actor type, delivered to the actor's `Job` method. Jobs are stored in the database, so they survive restarts, are retried automatically, and on permanent failure are dead-lettered (recorded for inspection and replay) rather than lost.

Jobs run immediately, after a delay, on a repeating interval, or on a cron schedule. They take input and return nothing.

## Jobs vs. alarms

Jobs and [alarms](/docs/alarms) both ride on the same durable scheduling engine, but they serve different purposes:

| | Alarms | Jobs |
|---|---|---|
| Delivered to | `Alarm(name, data)` | `Job(method, data)` |
| Keyed by | name (set replaces on collision) | server-issued JobID (each dispatch is distinct) |
| Dispatching N | N alarms with the same name coalesce to one | N dispatches run N times |
| On permanent failure | the alarm is deleted | the job is dead-lettered |
| On success | the alarm is deleted | the job is deleted, or retained if the actor type asks for it |

Use an **alarm** for a self-scheduled reminder or timer that an actor owns (one per name). Use a **job** to dispatch background work to an actor, especially when you need each dispatch to run and failures to be recorded rather than dropped.

> To distribute many independent tasks across a cluster — running a bounded number per host and scaling out with more hosts — use the built-in [task pool](/builtin-actors/task-pool/), which builds this pattern on top of jobs.

## Receiving jobs

To receive jobs, an actor must implement the `actor.ActorJob` interface:

```go
func (w *Worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	// "method" identifies which job to run
	// "data" is the optional payload attached when the job was dispatched (may be nil)
	switch method {
	case "send-email":
		// ... send the email ...
	}
	return nil
}
```

- `method` is the value passed at dispatch time. An actor can handle many job methods.
- `data` is an `actor.Envelope`. Call `data.Decode(&dest)` to read the payload. It may be `nil`.
- Returning an error retries the job per the actor type's `MaxAttempts` / `InitialRetryDelay`. Once retries are exhausted the job is dead-lettered.
- Returning `actor.ErrJobPermanentFailure` skips the remaining retries and dead-letters the job immediately.

## Dispatching a job

Use the client (from inside an actor) to dispatch to the current actor:

```go
payload := map[string]any{"to": "user@example.com"}
jobID, err := w.client.Dispatch(ctx, "send-email", payload)
```

From outside an actor, use the service, which targets any actor:

```go
jobID, err := service.Dispatch(ctx, "worker", "worker-7", "send-email", payload)
```

`Dispatch` returns a server-issued `jobID` that is globally unique. Each call dispatches a distinct job: dispatching the same method twice runs it twice, unless you supply an idempotency key (see below).

### Job options

Scheduling is controlled with `actor.JobOption` values:

| Option | Description |
|--------|-------------|
| `WithJobDueTime(t)` | Run first at an absolute time. The default is as soon as possible. |
| `WithJobDelay(d)` | Run first after a delay, relative to dispatch. Mutually exclusive with `WithJobDueTime`. |
| `WithJobInterval(iso8601)` | Repeat on an ISO 8601 / Go-duration interval. Mutually exclusive with `WithJobCron`. |
| `WithJobCron(expr)` | Repeat on a standard cron expression. Mutually exclusive with `WithJobInterval`. |
| `WithJobTTL(t)` | Stop a repeating job after this time. |
| `WithIdempotencyKey(key)` | Dedup re-dispatch within the same actor (first dispatch wins). |

```go
// Run in 5 minutes
jobID, err := w.client.Dispatch(ctx, "reconcile", payload, actor.WithJobDelay(5*time.Minute))

// Run every weekday at 9am
jobID, err := w.client.Dispatch(ctx, "daily-report", nil, actor.WithJobCron("0 9 * * 1-5"))
```

### Idempotency keys

A job ID is server-issued and always unique. An idempotency key is caller-supplied and scoped to the actor: dispatching twice with the same key produces a single job (the first wins), so a retry of the dispatch itself does not enqueue duplicate work.

```go
// Re-dispatching with the same key returns the same job
// The work runs once
jobID, err := w.client.Dispatch(ctx, "charge", payload, actor.WithIdempotencyKey("order-42"))
```

Without a key, every dispatch is a distinct job: this is the anti-coalescing guarantee that distinguishes jobs from alarms.

## When a job ends

A job that ends — whether it completed or failed terminally — can leave a record behind. Both kinds of record live in the same store and are read back with the same calls.

When a job exhausts its retries, or returns `actor.ErrJobPermanentFailure`, it is **dead-lettered** rather than dropped. You can inspect, replay, or delete dead jobs (see below).

A job that **completed** leaves nothing behind by default: once it has run, its row is deleted, which is why `GetJob` on a finished job reports `actor.ErrJobNotFound`. Set a retention on the actor type to keep a record instead:

```go
host.RegisterActor("worker", newWorker,
	local.WithJobRetention(24*time.Hour),
)
```

With a retention set, a job that completes is recorded with `actor.JobStatusCompleted` and stays readable for that long, which is what makes a successful run observable rather than invisible. The same retention bounds a dead-lettered record, which is otherwise kept until something removes it. A completed record carries the job's metadata but **not its payload** — only a dead job is ever replayed, so only a dead job keeps the data a replay would need.

Expired records are garbage collected in the background, and read as gone as soon as the retention elapses whether or not the collector has run.

Optionally, an actor can react to a dead-lettering by implementing `actor.ActorJobFailed`:

```go
func (w *Worker) JobFailed(ctx context.Context, jobID string, method string, data actor.Envelope, jobErr error) error {
	// Best-effort hook called after the job has been recorded in the dead-letter store
	// The dead-letter record is the source of truth: an error returned here is only logged
	return nil
}
```

The hook is best-effort: the dead-letter record is written first, then the hook is delivered on the actor's turn-based lock. An error from the hook is logged and does not re-trigger dead-lettering.

For a _repeating_ job, a single terminally-failing occurrence is dead-lettered while the recurrence continues: later occurrences still run.

## Managing jobs

`GetJob`, `ListJobs`, `DeleteJob`, and `RetryJob` are available on both the client (self-bound) and the service (for any actor):

```go
// Inspect a job, spanning live and ended jobs alike
info, err := service.GetJob(ctx, jobID)
// info.Status is one of actor.JobStatusPending, JobStatusActive, JobStatusCompleted, JobStatusDeadLettered
// info.Status.IsTerminal() reports whether the job has ended

// List an actor's jobs: the live ones, plus any retained record
jobs, err := service.ListJobs(ctx, "worker", "worker-7")

// Remove a job, whatever state it is in
// One still scheduled is cancelled before it runs; one that has ended has its record removed
err = service.DeleteJob(ctx, "worker", "worker-7", jobID)

// Re-dispatch a dead-lettered job (runs as soon as possible) and remove the dead record
newJobID, err := service.RetryJob(ctx, jobID)
```

There is deliberately one removal verb rather than separate cancel and delete calls: the caller knows the job it wants gone, not which half of its lifecycle it happens to be in. `RetryJob` is the exception that stays specific to dead jobs, since a job that completed has nothing to retry.

`GetJob` returns `actor.JobInfo`; `Attempts` and `EndedAt` are populated only once a job has ended, and `LastError` only for one that dead-lettered. Missing jobs are reported as `actor.ErrJobNotFound`.

## Delivery semantics

- **Durable**: jobs are persisted, so they survive restarts and are delivered after downtime.
- **Leased execution**: hosts (or the runtime, in remote topologies) lease a job before running it, so it isn't executed by two hosts at once.
- **Activation on dispatch**: when a job is due, Francis activates the target actor if it isn't already active, then calls `Job`.
- **Turn-based serialization**: `Job`, `Alarm`, and `Invoke` all share the actor's single turn lock, so a job never runs concurrently with other work on the same actor.
- **At-least-once with retries**: a failing `Job` is retried per the actor type's `MaxAttempts` and `InitialRetryDelay`, then dead-lettered. Design job handlers to be idempotent.

## Full example

```go
func (w *Worker) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case "enqueue-report":
		// Dispatch background work to self
		// It runs immediately on whatever host serves this actor
		jobID, err := w.client.Dispatch(ctx, "report", map[string]any{"day": "today"})
		if err != nil {
			return nil, err
		}
		return map[string]string{"jobID": jobID}, nil
	}
	return nil, nil
}

func (w *Worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	if method == "report" {
		// ... build and store the report ...
	}
	return nil
}

func (w *Worker) JobFailed(ctx context.Context, jobID string, method string, data actor.Envelope, jobErr error) error {
	// ... alert that a report job was dead-lettered ...
	return nil
}
```
