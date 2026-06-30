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
