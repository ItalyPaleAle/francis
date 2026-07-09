---
title: "Built-in actors"
weight: 28
---

A built-in actor is a framework-managed actor that a host registers under a reserved type and bootstraps at startup. You register one by calling `host.RegisterBuiltInActor(...)` before the host starts (available on both the local and remote hosts), mirroring how you register your own actors with `RegisterActor`.

Built-in actors are reserved: their type names carry a `francis.builtin.` prefix, and clients **cannot target them directly**.

Under the hood, a built-in actor that needs one-time setup (like the cron job) is just a [singleton actor](#singleton-actors): the same public mechanism you can use for your own actors.

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

## Singleton actors

Built-in actors like the cron job are built on a public mechanism you can use for your own actors: a **singleton actor**, bootstrapped once the host is ready. Use it when you need exactly one instance of an actor cluster-wide, with a one-time setup step that runs on startup — for example, a coordinator that registers a single durable [job](/docs/jobs) for the whole cluster.

A singleton actor is reached at the well-known ID `actor.SingletonActorID` from every host, so all callers target the same instance. Register it with `RegisterSingletonActor` instead of `RegisterActor`, and implement the `actor.Bootstrapper` interface for its startup setup:

```go
// Bootstrapper is called once the host is ready, on the singleton instance
type Bootstrapper interface {
	Bootstrap(ctx context.Context) error
}
```

```go
host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Register before calling host.Run; can be called multiple times for several singletons
err = host.RegisterSingletonActor("scheduler", schedulerFactory, host.RegisterActorOptions{})
```

Once the host is ready, it invokes `Bootstrap` on the singleton instance, **routed through placement** so it runs on the single owning host at a time and is serialized by that instance's turn lock — exactly like a normal invocation. Every host triggers it at startup, so `Bootstrap` **must be idempotent**: reconcile existing durable work rather than duplicating it. An actor registered this way that does not implement `Bootstrapper` simply has no startup step.

To reach the singleton from application code, invoke it at `actor.SingletonActorID`:

```go
_, err := host.Service().Invoke(ctx, "scheduler", actor.SingletonActorID, "someMethod", nil)
```

> [!NOTE]
> The reserved `Bootstrap` lifecycle is driven by the framework; clients cannot invoke it directly, and reserved method names (prefixed with `francis.`) are rejected.
