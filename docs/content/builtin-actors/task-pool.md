---
title: "Task pool"
weight: 30
description: "Run a distributed pool of long-running tasks"
---

A task pool runs a **distributed pool of task workers**: each submitted task becomes a durable [job](/docs/jobs) delivered to its own worker actor, which runs the task once and then frees its slot. Tasks are drained from a shared queue by whichever hosts have spare capacity, so **the more hosts you run, the more tasks run in parallel**. It is designed for long-running work such as media conversion, report generation, or batch processing.

Two properties make it more than "dispatch a job per task":

- **Strict per-host concurrency**: each host runs at most `WithConcurrency` tasks at once, enforced exactly in-process, so a host is never overloaded.
- **Capabilities**: a task can require a capability (such as `gpu`), and it is only ever run on a host that advertises it. A task with no required capability runs anywhere.

## Registering

Build a task pool with `taskpool.New` and register it on the host, before the host starts:

```go
import "github.com/italypaleale/francis/builtin/taskpool"

pool, err := taskpool.New("video-convert",
	taskpool.WithConcurrency(2),       // at most 2 tasks at a time on this host
	taskpool.WithCapability("gpu"),    // this host can also run tasks that require a GPU
	taskpool.WithHandler(func(ctx context.Context, task taskpool.Task) error {
		var req ConvertRequest
		err := task.Decode(&req)
		if err != nil {
			return err
		}
		// ... do the (long-running) work, respecting ctx cancellation ...
		return nil
	}),
)
if err != nil {
	return err
}

host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Register the built-in actor before calling host.Run
err = host.RegisterBuiltInActor(pool)
```

Register the same pool (same name) on every host that should run its tasks. Each host passes its **own** capabilities: a host with a GPU adds `WithCapability("gpu")`, while a plain host declares nothing and still serves every task submitted without a required capability. `WithConcurrency` is per-host too, so different hosts can run different numbers of tasks at once.

## Options

`taskpool.New(name, opts...)` takes a unique `name` (used to build the reserved actor types, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithHandler(fn)` | The function that runs each task. **Required.** |
| `WithConcurrency(n)` | The strict maximum number of tasks this host runs at once, across all of the pool's queues. Defaults to **1**. |
| `WithCapability(cap)` | Advertise a capability on this host, so tasks that require it can run here. Repeatable. |
| `WithAccept(fn)` | An optional predicate that runs before a task, letting this host decline it so it re-routes to another host (see below). |
| `WithMaxAttempts(n)` | How many times a failing task is retried before it is dead-lettered. |
| `WithInitialRetryDelay(d)` | The base backoff between retries of a failing task. |
| `WithIdleTimeout(d)` | Safety net for reclaiming a finished worker; workers halt themselves after each task, so this rarely matters. |
| `WithLogger(l)` | A logger for task lifecycle events. |

## Submitting and managing tasks

The operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
svc := pool.Service(host.Service())

// Submit a task
// Input is decoded by the handler with Task.Decode
taskID, err := svc.Submit(ctx, ConvertRequest{Source: "a.mov"})

// Require a capability: only hosts advertising "gpu" will run it
taskID, err = svc.Submit(ctx, req, taskpool.WithRequiredCapability("gpu"))

// Make a submission idempotent: the same key produces a single task
taskID, err = svc.Submit(ctx, req, taskpool.WithTaskKey("video-42"))

// Inspect, cancel, or replay a task
info, err := svc.GetTask(ctx, taskID)   // info.Status is pending, active, or dead-lettered
err = svc.CancelTask(ctx, taskID)
newID, err := svc.RetryTask(ctx, taskID) // re-submit a dead-lettered task
```

`Submit` returns a `taskID`. Each submission is a distinct task unless you pass `WithTaskKey`, which makes it idempotent (the first submission wins).

You do not have to advertise a capability to submit a task requiring it: the task simply stays **pending** until a host that advertises the capability picks it up.

## Communicating results

A task pool is **fire-and-forget**: it runs your handler and records failures, but it does not capture or return a result. Communicating the outcome is your handler's responsibility — for example by writing to a database, calling an API, uploading the output, or invoking another actor. Returning from the handler only tells the pool the task succeeded.

## Capacity groups

All of a pool's queues (the base queue plus one per capability) share a single **capacity group** on each host: `WithConcurrency(n)` is the total budget across them, not per queue. So a host that advertises `gpu` and runs with `WithConcurrency(1)` runs at most one task at a time whether it is a GPU task or a plain one. The limit is enforced exactly in-process; a cluster-wide placement hint keeps hosts from being handed much more than they can run, and any occasional overshoot is re-routed rather than run.

## Declining a task (re-routing)

Capabilities cover **static** differences between hosts. For **dynamic**, per-task decisions — "only run where the input file is already local", "I'm temporarily overloaded" — a handler can decline a task and have it re-routed to another host:

- return `actor.ErrJobRejected` from the handler, or
- supply a `WithAccept(fn)` predicate that returns `false`.

A declined task is handed back to the pool and run elsewhere, **without counting as a failed attempt** and without dead-lettering. If no host accepts it, it is retried with a short backoff; make sure at least one host will eventually accept it, or it will keep re-routing.

## Delivery semantics

- **Strict per-host concurrency**: at most `WithConcurrency` tasks run at once on a host, guaranteed.
- **Scales with hosts**: more hosts (or a higher limit) mean more tasks run in parallel, drained from one shared queue.
- **Durable and leased**: tasks survive restarts and are leased before running, so a task is not run by two hosts at once. Long-running tasks keep their lease renewed while they run.
- **At-least-once with retries**: a failing task is retried per `WithMaxAttempts`, then dead-lettered. A host that dies mid-task means the task runs again elsewhere, so design handlers to be idempotent (and checkpoint long tasks where you can).
