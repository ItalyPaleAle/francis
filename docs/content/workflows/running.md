---
title: "Running and observing"
weight: 70
description: "Starting, status, listing, cancelling, retention, and purging"
---

The operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
svc := orders.Service(host.Service())
```

**Authorization is your application's responsibility**, as it is for every other actor invocation in Francis: who may start, cancel, suspend, raise an event on, or purge a given instance. There is no hook for it in the engine, so check permissions before you call these.

## Starting

```go
// Without WithInstanceID, the engine mints a UUIDv7, which sorts by creation time
id, created, err := svc.Start(ctx, OrderInput{OrderID: "A-91", Total: 4999})

// Use a natural key to make starting idempotent
id, created, err = svc.Start(ctx, input, workflow.WithInstanceID("order-A-91"))
```

`Start` returns as soon as the start job is durable: from that point on the work survives a restart of the process.

It is idempotent only for suppressing a duplicate of the **same** request. A second `Start` with an instance ID that already exists finds the first: `created` comes back `false` and the second call's input is discarded, so check it when that matters. An instance ID that has already **terminated** is not restarted either, so re-driving a failed run means using a fresh instance ID.

The input is JSON-encoded and capped by `WithMaxInputSize` (64 KiB by default), because it is shipped in every task's payload. An oversized input returns `ErrInputTooLarge` rather than starting a run that cannot work.

## Status

```go
status, err := svc.GetStatus(ctx, id)
if errors.Is(err, workflow.ErrInstanceNotFound) {
	// No such instance, or it was purged after passing its retention
}
```

For child steps, `StepStatusView.Children` pairs each child instance ID with the terminal status and compensation outcome it reported. The outcome stays on the parent after the child is purged, so an optional or tolerated child failure never hides a partial rollback.

`GetStatus` is a read, so it is cheap, it runs concurrently with other reads, and polling it does not slow an instance down.

```go
type InstanceStatus struct {
	InstanceID   string
	Workflow     string
	Version      int
	Status       Status
	Compensation CompensationOutcome
	CurrentStep  string
	Steps        []StepStatusView // every step: status, task counts, attempts, timings, error, and child outcomes
	Cause        string
	Output       json.RawMessage  // what the run produced, once it has completed
	Suspended    *SuspendView     // when suspended: since when, why, and what it was
	Parent       *ParentView      // when a child: whose
	CreatedAt, StartedAt, CompletedAt time.Time
}
```

**The output** is what the instance produced: the output of the step named with `WithOutput`, or of the last step otherwise. It is set once the instance completes and stays readable for as long as the instance is retained, so a caller can ask what a run returned long after it ended. It is the same value a parent reads back from a child. Decode it with `DecodeOutput`:

```go
var result CheckoutResult
err = status.DecodeOutput(&result)
```

It is empty for any instance that has not completed, and `DecodeOutput` leaves the destination untouched in that case.

An instance that has been accepted but has not started running yet reports `pending`, which is how you tell it apart from one that does not exist. A caller never sees `completed` before every step has finished, including the optional ones.

The statuses are:

| Status | Meaning |
|--------|---------|
| `pending` | accepted and durable, but not started yet |
| `running` | executing its steps |
| `suspended` | paused by `Suspend` |
| `compensating` | unwinding its compensation stack |
| `completed` | terminal: ran to the end |
| `failed` | terminal: could not |
| `cancelled` | terminal: cancelled and unwound |

## Listing

```go
page, err := svc.List(ctx, &workflow.ListOptions{
	Status:  workflow.StatusRunning, // or any status - empty means all
	Version: 3,                      // optional
	Parent:  parentID,               // optional: the children of one instance
	Limit:   50,
	After:   page.AfterID(),         // pagination cursor, an instance ID
})
```

Each of `status`, `version`, and `parent` is indexed, so "every running instance" stays fast however many instances you have retained. There is nothing to configure, and those three are the only filters available.

Because the default instance ID is a UUIDv7, a listing is in creation order. Page until `AfterID()` returns empty:

```go
var cursor string
for {
	page, err := svc.List(ctx, &workflow.ListOptions{Status: workflow.StatusFailed, After: cursor, Limit: 100})
	if err != nil {
		return err
	}
	for _, inst := range page.Instances {
		// ...
	}

	cursor = page.AfterID()
	if cursor == "" {
		break
	}
}
```

## Cancelling

```go
err := svc.Cancel(ctx, id, "customer cancelled the order")
```

`Cancel` moves a running or suspended instance into `compensating` and unwinds its stack, with the reason recorded as the cause every compensation receives. The step that was in flight is closed out rather than waited on: work that has not started is dropped, and an attempt already running is not interrupted. If such an attempt succeeds after the rollback has begun, its result is still recorded and still compensated.

The instance terminates as `cancelled`.

## Retention and purging

`WithRetention` takes one duration per terminal status, because a failed run is usually worth keeping longer than a successful one:

```go
workflow.WithRetention(workflow.RetentionPolicy{
	Completed: 24 * time.Hour,
	Failed:    7 * 24 * time.Hour,
	Cancelled: 7 * 24 * time.Hour,
})
```

A **purge** is what removes a terminated instance: its children, the jobs it used, and its recorded history. Run it yourself, or let `WithAutoPurge` run it on a schedule. An instance that is never swept expires on its own as a backstop, so a cluster with no purge configured still does not grow without bound.

You can purge at three levels, from explicit to automatic:

```go
// One terminated instance, and everything it owns
err := svc.Purge(ctx, id)

// Every terminated instance past its retention, skipping any that still has a parent
n, err := svc.PurgeTerminated(ctx)
```

```go
// The same sweep on a schedule, as a cluster-wide singleton cron job
workflow.WithAutoPurge("0 3 * * *")
```

`Purge` refuses a running or suspended instance with `ErrInstanceActive`, and one that is already gone with `ErrInstanceNotFound`. It is idempotent, so an interrupted purge is safe to repeat. It also refuses an instance whose parent is still running, since the parent's own purge reaches it.

`PurgeTerminated` works through a backlog in pages, so a million terminated instances is a long call rather than a large one. However many hosts registered the workflow, `WithAutoPurge` runs the sweep on **one** of them per schedule.

Once an instance is purged, `GetStatus` reports not found. **An instance's history is an operational record, not an audit log.** If you need a permanent one, write it from a step, which is what the manifest in the [thumbnails example](/workflows/examples/thumbnails) does.
