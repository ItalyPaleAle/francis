---
title: "Waiting and pausing"
weight: 50
description: "WaitForEvent, RaiseEvent, Suspend, and Resume"
---

Two different things stop a workflow: a step **waiting for something to happen**, and an operator **pausing the whole instance**.

## Waiting for an event

`workflow.WaitForEvent` parks the instance until an external event arrives or its timeout elapses. It has no task and no handler.

```go
workflow.Step("request-review", workflow.WithRun(openReviewTicket)),

// Parks the instance until a manager approves, or three days pass
workflow.WaitForEvent("approval",
	workflow.WithEventTimeout(72*time.Hour),
),

workflow.Step("approved", workflow.WithRun(readApproval)),
```

The event arrives from outside, as a single call:

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

- The event name **defaults to the step's name**, and `WithEventName` sets it explicitly. No two steps may listen for the same name.
- `RaiseEvent` returns `ErrNoSuchEvent` for a name nothing in the definition waits for.
- Only the **open** wait accepts its event. One raised before the step is reached, or after it completed, is discarded.
- Repeated calls with the same name coalesce, so a double-clicked approve button produces one event.
- If the timeout elapses first, the instance **rolls back**, with `event "approval" timed out` as the cause.

A rejection is not a failure. It is a value, and the step after the wait decides what to do with it:

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

Both are durable. Repeated calls coalesce, and a call on an instance already in the requested state does nothing.

While suspended:

- **Nothing new is started.** No next step, no next attempt, no compensation.
- **In-flight work finishes.** A task that has already started runs to completion and its result is recorded.
- **Deadlines are paused.** Francis restores whatever was left of the instance, step, and event timeouts on resume, so a two-day suspension does not eat a thirty-minute timeout.
- **Events are accepted.** A `RaiseEvent` for the open `WaitForEvent` step is recorded, and the step completes on resume.
- **Cancel takes precedence.** `Cancel` on a suspended instance resumes it straight into `compensating`. Suspending during a rollback pauses it at the step it had reached.
- **Children are not affected.** A suspended parent's children keep running, and their results wait for the resume. Suspend a child explicitly if that is not what you want: `GetStatus` on the parent lists every child's instance ID.

`GetStatus` reports the suspension, its reason, and what the instance goes back to:

```go
status, err := svc.GetStatus(ctx, id)
if status.Suspended != nil {
	// status.Suspended.Reason, .At, .ResumeTo
}
```

`List(&workflow.ListOptions{Status: workflow.StatusSuspended})` finds everything currently paused.

## Which one to use

Use a **wait step** when the workflow needs something from outside: an approval, a callback, a third party confirming. It is part of the graph, has its own timeout, and its payload feeds the steps after it.

Use **suspend** when something operational is wrong: a dependency under maintenance, a bad deployment, an incident. It is not part of the graph, and any instance can be suspended at any point.
