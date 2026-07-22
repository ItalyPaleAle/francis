---
title: "Built-in actors"
weight: 29
---

A built-in actor is a framework-managed actor that a host registers under a reserved type and bootstraps at startup.

You register one by calling `host.RegisterBuiltInActor(...)` before the host starts (available on both the local and remote hosts), similarly to how register your own actors with `RegisterActor`.

Built-in actors are reserved: their type names carry a `francis.builtin.` prefix, and clients **cannot target them directly**.

## Cron job

A cron job actor runs a function you supply on a schedule, **across the cluster on a single node at a time**. It is a cluster-wide singleton backed by one durable, repeating [job](/docs/jobs): the schedule is registered exactly once, and each occurrence is leased so only one host runs it.

### Registering

Build a cron job with `cronjob.New` and register it on the host, before the host starts:

```go
import "github.com/italypaleale/francis/builtin/cronjob"

cleanupJob, err := cronjob.New("nightly-cleanup",
	cronjob.WithCron("0 2 * * *"),
	cronjob.WithJob(func(ctx context.Context) error {
		// ... runs once across the cluster, every night at 2am ...
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
err = host.RegisterBuiltInActor(cleanupJob)
```

`RegisterBuiltInActor` can be called more than once to register several built-in actors, and must be called before `host.Run`. Register the same cron job (same name and options) on every host that should be able to run it: at startup each host triggers the registration, but the schedule is set up only once for the cluster.

### Options

`cronjob.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithJob(fn)` | The function to run on each occurrence. **Required.** |
| `WithInterval(d)` | Repeat every `time.Duration` `d`. |
| `WithPeriod(iso8601)` | Repeat on an ISO 8601 duration string, e.g. `"PT5M"` or `"P1D"`. |
| `WithCron(expr)` | Repeat on a standard cron expression, e.g. `"0 9 * * 1-5"`. |
| `WithImmediate()` | Also run the job once right away, but only the first time it is registered. |

Exactly one of `WithInterval`, `WithPeriod`, or `WithCron` is required, and `WithJob` is required. Without `WithImmediate`, the first run happens after one interval (or at the next cron tick).

### How it works

At startup each host bootstraps the cron job's scheduler (the cluster-wide singleton), which sets up the schedule:

1. If the schedule is already registered, bootstrapping does nothing — so it is safe for every host to trigger it, and it stays registered across restarts.
2. Otherwise it dispatches the repeating job that drives the schedule and records its ID. `WithImmediate` additionally runs the job once right away on this first registration.

Because the actor is a single cluster-wide instance with turn-based execution, concurrent registrations from multiple hosts are automatically collapsed to a single recurring job. It is safe to re-register the actor on every instance in the cluster.

### Triggering a run on demand

The on-demand operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`

 Call `Trigger` on the resulting service to run the job once, immediately, regardless of the schedule:

```go
cleanup := cleanupJob.Service(host.Service())

err := cleanup.Trigger(ctx)
```

The run happens on the runner, so triggering returns promptly even if a previous run is still going. Multiple triggers that pile up while a run is still pending are **collapsed into a single run**.

### Unregistering

Calling `Unregister` cancels the recurring job and clears the actor's state, so a later startup re-registers it cleanly:

```go
cleanup := cleanupJob.Service(host.Service())

err := cleanup.Unregister(ctx)
```

## Rate limiter

A rate limiter actor throttles calls **per key**, a free-form string you choose (e.g. an IP address, user ID, route, API token, etc). Each key is limited independently, and its limiter state lives only in the activated actor's memory, for optimal performance.

It follows the token-bucket model, and `Allow` is a non-blocking check: it reports whether the call is admitted right now and, when it is not, how long the caller should wait before retrying.  
The returned wait can be used as a  `Retry-After` header on a `429 Too Many Requests` response.

### Registering

Build a rate limiter with `ratelimit.New` and pass it to the host:

```go
import "github.com/italypaleale/francis/builtin/ratelimit"

limiter, err := ratelimit.New("api",
	ratelimit.WithRate(100), // 100 calls per second, per key
)
if err != nil {
	return err
}

host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Register the built-in actor before calling host.Run
err = host.RegisterBuiltInActor(limiter)
```

As with any built-in actor, register the same rate limiter (same name and options) on every host that should serve it. A given key is always placed on a single host at a time, so its limiter is consistent cluster-wide.

### Options

`ratelimit.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithRate(n)` | Number of calls admitted per period. **Required**, must be greater than zero. |
| `WithPer(d)` | The window the rate applies over. Defaults to one second, so `WithRate(100)` alone is 100/s; combine with `WithPer(time.Minute)` for a per-minute rate. |
| `WithBurst(n)` | The token bucket's capacity: how many calls may be admitted instantly before throttling kicks in, refilling at the configured rate. Defaults to **1** (strict), so calls are admitted one at a time - raise it to tolerate short bursts above the steady rate. |
| `WithIdleTimeout(d)` | How long a key's in-memory limiter is kept after its last call before the actor is deactivated. Defaults to double the period (the `WithPer` window), with a minimum of one minute. Lower it to reclaim memory faster when limiting many distinct keys. |

### Throttling by key

The `Allow` operation is bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
rl := limiter.Service(host.Service())

// Non-blocking: reports whether this key may proceed under the configured rate
allowed, retryAfter, err := rl.Allow(ctx, clientIP)
if err != nil {
	// The key was invalid or the invocation failed (e.g. ctx was cancelled)
	return err
}
if !allowed {
	// Throttled: retryAfter is how long until the key admits another call
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}
// ... handle the request ...
```

`Allow` never blocks. When `allowed` is `false`, `retryAfter` tells the caller how long to wait before the key admits another call (it is zero when `allowed` is `true`).  
The returned `error` is non-nil only when the key is invalid or the underlying actor invocation fails, including context cancellation - it never signals throttling.

## Task pool

A task pool runs a **distributed pool of task workers**: each submitted task becomes a durable [job](/docs/jobs) delivered to its own worker actor, which runs the task once and then frees its slot. Tasks are drained from a shared queue by whichever hosts have spare capacity, so **the more hosts you run, the more tasks run in parallel**. It is designed for long-running work such as media conversion, report generation, or batch processing.

Two properties make it more than "dispatch a job per task":

- **Strict per-host concurrency**: each host runs at most `WithConcurrency` tasks at once, enforced exactly in-process, so a host is never overloaded.
- **Capabilities**: a task can require a capability (such as `gpu`), and it is only ever run on a host that advertises it. A task with no required capability runs anywhere.

### Registering

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

### Options

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

### Submitting and managing tasks

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

### Communicating results

A task pool is **fire-and-forget**: it runs your handler and records failures, but it does not capture or return a result. Communicating the outcome is your handler's responsibility — for example by writing to a database, calling an API, uploading the output, or invoking another actor. Returning from the handler only tells the pool the task succeeded.

### Capacity groups

All of a pool's queues (the base queue plus one per capability) share a single **capacity group** on each host: `WithConcurrency(n)` is the total budget across them, not per queue. So a host that advertises `gpu` and runs with `WithConcurrency(1)` runs at most one task at a time whether it is a GPU task or a plain one. The limit is enforced exactly in-process; a cluster-wide placement hint keeps hosts from being handed much more than they can run, and any occasional overshoot is re-routed rather than run.

### Declining a task (re-routing)

Capabilities cover **static** differences between hosts. For **dynamic**, per-task decisions — "only run where the input file is already local", "I'm temporarily overloaded" — a handler can decline a task and have it re-routed to another host:

- return `actor.ErrJobRejected` from the handler, or
- supply a `WithAccept(fn)` predicate that returns `false`.

A declined task is handed back to the pool and run elsewhere, **without counting as a failed attempt** and without dead-lettering. If no host accepts it, it is retried with a short backoff; make sure at least one host will eventually accept it, or it will keep re-routing.

### Delivery semantics

- **Strict per-host concurrency**: at most `WithConcurrency` tasks run at once on a host, guaranteed.
- **Scales with hosts**: more hosts (or a higher limit) mean more tasks run in parallel, drained from one shared queue.
- **Durable and leased**: tasks survive restarts and are leased before running, so a task is not run by two hosts at once. Long-running tasks keep their lease renewed while they run.
- **At-least-once with retries**: a failing task is retried per `WithMaxAttempts`, then dead-lettered. A host that dies mid-task means the task runs again elsewhere, so design handlers to be idempotent (and checkpoint long tasks where you can).
