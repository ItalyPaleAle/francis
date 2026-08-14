---
title: "Cron job"
weight: 10
description: "Run a function on a schedule, once across the cluster"
---

A cron job actor runs a function you supply on a schedule, **across the cluster on a single node at a time**. It is a cluster-wide singleton backed by one durable, repeating [job](/docs/jobs): the schedule is registered exactly once, and each occurrence is leased so only one host runs it.

## Registering

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

## Options

`cronjob.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithJob(fn)` | The function to run on each occurrence. **Required.** |
| `WithInterval(d)` | Repeat every `time.Duration` `d`. |
| `WithPeriod(iso8601)` | Repeat on an ISO 8601 duration string, e.g. `"PT5M"` or `"P1D"`. |
| `WithCron(expr)` | Repeat on a standard cron expression, e.g. `"0 9 * * 1-5"`. |
| `WithImmediate()` | Also run the job once right away, but only the first time it is registered. |
| `WithJitter(d)` | Spread each occurrence by a random offset within `±d` of its scheduled time. |

Exactly one of `WithInterval`, `WithPeriod`, or `WithCron` is required, and `WithJob` is required. Without `WithImmediate`, the first run happens after one interval (or at the next cron tick).

## Jitter

Cron jobs that share a schedule all fire at the same instant, so the work lands in one spike. For example, a fleet of jobs set to `0 2 * * *` all start at exactly 2am. Use `WithJitter` to spread them apart:

```go
cleanupJob, err := cronjob.New("nightly-cleanup",
	cronjob.WithCron("0 2 * * *"),
	// Each run lands somewhere between 01:55 and 02:05
	cronjob.WithJitter(5*time.Minute),
	cronjob.WithJob(func(ctx context.Context) error {
		return nil
	}),
)
```

A fresh offset is drawn for every occurrence within `±d` of the scheduled time. For example, with a cron job set for `0 2 * * *` (every day at 2am) and a jitter of 5 minutes, jobs are executed at a random time between 1.55-2.05am.

Note: `WithImmediate` and `Trigger` runs are never jittered, since both mean "run now".

## How it works

At startup each host bootstraps the cron job's scheduler (the cluster-wide singleton), which sets up the schedule:

1. If the schedule is already registered, bootstrapping does nothing — so it is safe for every host to trigger it, and it stays registered across restarts.
2. Otherwise it dispatches the repeating job that drives the schedule and records its ID. `WithImmediate` additionally runs the job once right away on this first registration.

Because the actor is a single cluster-wide instance with turn-based execution, concurrent registrations from multiple hosts are automatically collapsed to a single recurring job. It is safe to re-register the actor on every instance in the cluster.

## Triggering a run on demand

The on-demand operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`

 Call `Trigger` on the resulting service to run the job once, immediately, regardless of the schedule:

```go
cleanup := cleanupJob.Service(host.Service())

err := cleanup.Trigger(ctx)
```

The run happens on the runner, so triggering returns promptly even if a previous run is still going. Multiple triggers that pile up while a run is still pending are **collapsed into a single run**.

## Unregistering

Calling `Unregister` cancels the recurring job and clears the actor's state, so a later startup re-registers it cleanly:

```go
cleanup := cleanupJob.Service(host.Service())

err := cleanup.Unregister(ctx)
```
