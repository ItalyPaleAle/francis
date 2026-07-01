---
title: "Built-in actors"
weight: 28
---

A built-in actor is a framework-managed actor that a host registers automatically and bootstraps at startup. You opt in with the `WithBuiltInActor(...)` host option (available on both the local and remote hosts), rather than calling `RegisterActor` yourself.

Built-in actors are reserved: their type names carry a `francis.builtin.` prefix, and clients **cannot target them directly**.

## Cron job

A cron job actor runs a function you supply on a schedule, **across the cluster on a single node at a time**. It is a cluster-wide singleton backed by one durable, repeating [job](/docs/jobs): the schedule is registered exactly once, and each occurrence is leased so only one host runs it.

### Registering

Build a cron job with `cronjob.New` and pass it to the host:

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

host, err := local.NewHost(
	// ... other options ...
	local.WithBuiltInActor(cleanupJob),
)
```

`WithBuiltInActor` can be repeated to register more than one. Register the same cron job (same name and options) on every host that should be able to run it: at startup each host triggers the registration, but the schedule is set up only once for the cluster.

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

At startup each host invokes a one-time `register` method on the cron job's scheduler:

1. If the schedule is already registered, `register` does nothing — so it is safe for every host to trigger it, and it stays registered across restarts.
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

It follows the leaky-bucket model: `Take` blocks until the key's limiter admits the call, smoothing bursts down to the configured rate rather than rejecting them.  

Calls for the same key are serialized by the actor's turn lock, so holding the turn for the throttle delay is the intended backpressure (calls for different keys run on independent instances and never block each other).

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

host, err := local.NewHost(
	// ... other options ...
	local.WithBuiltInActor(limiter),
)
```

As with any built-in actor, register the same rate limiter (same name and options) on every host that should serve it. A given key is always placed on a single host at a time, so its limiter is consistent cluster-wide.

### Options

`ratelimit.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithRate(n)` | Number of calls admitted per period. **Required**, must be greater than zero. |
| `WithPer(d)` | The window the rate applies over. Defaults to one second, so `WithRate(100)` alone is 100/s; combine with `WithPer(time.Minute)` for a per-minute rate. |
| `WithSlack(n)` | Burst allowance: how many unspent calls may accumulate for a later burst. By default the limiter is **strict** (no slack), so calls for a key are evenly spaced - pass this to opt into bursting. |
| `WithIdleTimeout(d)` | How long a key's in-memory limiter is kept after its last call before the actor is deactivated. Defaults to double the period (the `WithPer` window), with a minimum of one minute. Lower it to reclaim memory faster when limiting many distinct keys. |

### Throttling by key

The `Take` operation is bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
rl := limiter.Service(host.Service())

// Blocks until this key is allowed to proceed under the configured rate
err := rl.Take(ctx, clientIP)
if err != nil {
	// ctx was cancelled before the call was admitted
	return err
}
// ... handle the request ...
```

`Take` returns the context error if the context is cancelled before the call is admitted.
