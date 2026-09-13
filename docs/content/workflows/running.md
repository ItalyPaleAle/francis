---
title: "Running and observing"
weight: 70
description: "Starting, status, listing, cancelling, retention, and purging"
---

The operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
svc := orders.Service(host.Service())
```

**Authorization is your application's responsibility**, as it is for every other actor invocation in Francis: who may start, cancel, suspend, raise an event on, or purge a given instance. The engine offers no hook for it, deliberately.

## Starting

```go
// Without WithInstanceID, the engine mints a UUIDv7, which sorts by creation time
id, created, err := svc.Start(ctx, OrderInput{OrderID: "A-91", Total: 4999})

// Use a natural key to make starting idempotent
id, created, err = svc.Start(ctx, input, workflow.WithInstanceID("order-A-91"))
```

`Start` returns as soon as the start job is durable: from that point on the work survives a restart of the process.

It is idempotent only for suppressing a duplicate dispatch of the **same** request. A second `Start` with an instance ID that already exists finds the first: `created` comes back `false` and the second call's input is discarded, so a caller that needs to know can check it. An instance ID that has already **terminated** is not restarted either — re-driving a failed run means minting a fresh instance ID.

The input is JSON-encoded and capped by `WithMaxInputSize` (64 KiB by default), because it is shipped in every task's payload. An oversized input returns `ErrInputTooLarge` rather than starting a run that cannot work.

## Status

```go
status, err := svc.GetStatus(ctx, id)
if errors.Is(err, workflow.ErrInstanceNotFound) {
	// No such instance, or its journal has passed its retention
}
```

`GetStatus` is a read-only peek, so status reads run concurrently with each other and never queue behind one another — only behind a write turn, which the orchestration boundary keeps short. It reads through the provider rather than an activation's cache, so an active actor cannot serve a journal past its retention.

```go
type InstanceStatus struct {
	InstanceID   string
	Workflow     string
	Version      int
	Status       Status
	Compensation CompensationOutcome
	CurrentStep  string
	Steps        []StepStatusView // every step: status, task counts, attempts, timings, error, child IDs
	Cause        string
	Suspended    *SuspendView     // when suspended: since when, why, and what it was
	Parent       *ParentView      // when a child: whose
	CreatedAt, StartedAt, CompletedAt time.Time
}
```

An instance whose start job is durable but has not run yet reports `pending`, which is how it is told apart from one that does not exist. A caller never sees `completed` before every step has reported, including the optional ones.

The statuses are:

| Status | Meaning |
|--------|---------|
| `pending` | the start job is durable but has not run |
| `running` | executing its steps |
| `suspended` | paused by `Suspend` |
| `compensating` | unwinding its compensation stack |
| `completed` | terminal: ran to the end |
| `failed` | terminal: could not |
| `cancelled` | terminal: cancelled and unwound |

## Listing

```go
page, err := svc.List(ctx, &workflow.ListOptions{
	Status:  workflow.StatusRunning, // or any status; empty means all
	Version: 3,                      // optional
	Parent:  parentID,               // optional: the children of one instance
	Limit:   50,
	After:   page.AfterID(),         // pagination cursor, an instance ID
})
```

Listing is built on **state labels** the orchestrator writes in the same operation as the journal, so the index can never disagree with it. A label filter is an equality on an indexed column, which makes "every running instance" a range scan rather than a walk of every retained journal.

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

`Cancel` moves a running or suspended instance into `compensating` and unwinds its stack, with the reason recorded as the cause every compensation receives. The step that was in flight is closed out rather than waited on — its pending job is cancelled, and an attempt already executing is not interrupted. If such an attempt succeeds after the unwind opened, its result is still recorded and still compensated.

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

Retention is enforced in two layers. The **purge sweep** is the primary mechanism: it removes the journal, the instance's dead-letters, and its children. A **state TTL** is the backstop: on termination the journal is written with a TTL of twice the policy duration, so an instance whose sweep never runs still expires. It is twice the duration so the sweep always finds the journal it needs in order to clean up the rest.

Three levels, from explicit to automatic:

```go
// One terminated instance: its children first, then its dead-letters, then its journal
err := svc.Purge(ctx, id)

// Every terminated instance past its retention, skipping any that still has a parent
n, err := svc.PurgeTerminated(ctx)
```

```go
// The same sweep on a schedule, as a cluster-wide singleton cron job
workflow.WithAutoPurge("0 3 * * *")
```

`Purge` refuses a running or suspended instance with `ErrInstanceActive`, and an instance that is already gone with `ErrInstanceNotFound`. It is idempotent, so an interrupted purge is safe to repeat. `Purge` on an instance with a running parent is refused too: the parent's own purge reaches it.

`PurgeTerminated` pages, so a backlog of a million terminated instances is a long call rather than a large one. However many hosts registered the workflow, the `WithAutoPurge` cron job runs the sweep on **one** of them per schedule.

Once the journal is gone, `GetStatus` reports not found and a late report for that instance is dropped. **The journal is an operational record, not an audit log.** An application that needs a permanent record should write one from a terminal step — which is exactly what the manifest in the [thumbnails example](/workflows/examples/thumbnails) is for.
