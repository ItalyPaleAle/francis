---
title: "Waiting and pausing"
weight: 50
description: "WaitForEvent, RaiseEvent, Suspend, and Resume"
---

Two different things stop a workflow: a step that is **waiting for something to happen**, and an operator **pausing the whole instance**. They look similar and behave differently.

## Waiting for an event

`workflow.WaitForEvent` parks the instance until an external event arrives or its own timeout elapses. It has no task and no handler: nothing runs while it waits.

```go
workflow.Step("request-review", workflow.WithRun(openReviewTicket)),

// Parks the instance until a manager approves, or three days pass
workflow.WaitForEvent("approval",
	workflow.WithEventTimeout(72*time.Hour),
),

workflow.Step("approved", workflow.WithRun(readApproval)),
```

The event arrives from outside — a button in an admin console, a chat command, a webhook — as a single call:

```go
err := svc.RaiseEvent(ctx, tenantID, "approval", approvalPayload{Approved: true, By: user})
```

The payload becomes the step's output, so the next step reads it like any other:

```go
func readApproval(ctx context.Context, t workflow.Task) (any, error) {
	var payload approvalPayload
	err := t.DecodeOutput("approval", &payload)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}
	return payload.Approved, nil
}
```

A few rules:

- The event name **defaults to the step's name**, and `WithEventName` sets it explicitly. No two steps may listen for the same name, so an event is never ambiguous about which record it belongs to.
- `RaiseEvent` returns `ErrNoSuchEvent` for a name nothing in the definition waits for, rather than dispatching a job nothing reads.
- Only the **open** wait accepts its event. One raised before the step is reached, or after it completed, is discarded.
- Repeated calls with the same name coalesce, so a double-clicked approve button produces one event.
- If the timeout elapses first, the instance **unwinds**, with `event "approval" timed out` as the cause every compensation receives.

A rejection is not a failure. It is a boolean, and the step after the wait decides what to do with it:

```go
workflow.Step("approved", workflow.WithRun(readApproval)),

// Skipped when the manager said no
workflow.Step("verify",
	workflow.WithRun(verifyTenant),
	workflow.WithSkipIf("approved", false),
),
```

## Suspending an instance

`Suspend` pauses an instance without losing its place; `Resume` continues it.

```go
err := svc.Suspend(ctx, id, "downstream maintenance")
// ... later ...
err = svc.Resume(ctx, id)
```

Both are durable, so they survive a restart of the process. Repeated calls coalesce, and a call on an instance that is already in the requested state does nothing.

While suspended:

- **Nothing new is started.** No next step, no next attempt, no compensation. Suspension gates what gets started, and nothing else.
- **In-flight work finishes.** A task that has already started runs to completion and its result is recorded. Suspension is a promise not to start things, not an interruption.
- **Deadlines are paused.** Francis records how much of the instance timeout, the current step's timeout, and any event timeout was left, and restores them on resume, so a two-day suspension does not eat a thirty-minute timeout.
- **Events are accepted.** A `RaiseEvent` for the open `WaitForEvent` step is recorded, and the step completes on resume.
- **Cancel takes precedence.** `Cancel` on a suspended instance resumes it straight into `compensating`. Suspending during a rollback pauses it at the step it had reached.
- **Children are not affected.** A suspended parent's running children keep running, and their results wait for the resume. Suspend a child explicitly if that is not what you want: `GetStatus` on the parent lists every child's instance ID.

`GetStatus` reports the suspension, why, since when, and what the instance goes back to:

```go
status, err := svc.GetStatus(ctx, id)
if status.Suspended != nil {
	// status.Suspended.Reason, .At, .ResumeTo
}
```

`List(&workflow.ListOptions{Status: workflow.StatusSuspended})` finds everything currently paused.

## Which one to use

Use a **wait step** when the workflow's own logic requires something from outside: an approval, a callback, a third party confirming. It is part of the graph, it has its own timeout, and its payload feeds the steps after it.

Use **suspend** when something operational is wrong and you want the instance to stop touching it: a dependency under maintenance, a bad deployment, an incident. It is not part of the graph, any instance can be suspended at any point, and resuming picks up exactly where it left off.
