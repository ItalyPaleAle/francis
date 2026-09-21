---
title: "Waiting and pausing"
weight: 50
description: "WaitForEvent, RaiseEvent, Suspend, and Resume"
---

Workflows can be suspended waiting for something to happen, or the entire instance could be paused by an operator.

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

The event arrives from outside, as a single call.

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

Notes:

- The event name defaults to the step's name, and `WithEventName` sets it explicitly. No two steps may listen for the same name. `RaiseEvent` returns `ErrNoSuchEvent` for a name nothing in the definition waits for.
- Events raised before the step is reached, or after it completed, are discarded.
- If the timeout elapses first, the instance rolls back, with `event "approval" timed out` as the cause.

A rejection is not a failure. It is passed to the step after the wait as a value, which can decide what to do with it:

```go
workflow.Step("approved", workflow.WithRun(readApproval)),

// Skipped when the manager said no
workflow.Step("verify",
	workflow.WithRun(verifyTenant),
	workflow.WithSkipIf("approved", false),
),
```

Francis itself does not have an opinion on what causes an external event to be received. Common patterns that your application can implement include:

- Invoking `RaiseEvent` in an API handler, for example invoked by a user as approval ("human in the loop")
- Exposing a HTTP endpoint that can be invoked as webhook, triggered by an external event

## Suspending an instance

`Suspend` pauses an instance without losing its place, while `Resume` continues it.

```go
err := svc.Suspend(ctx, id, "downstream maintenance")
// ... later ...
err = svc.Resume(ctx, id)
```

Both are durable and a call on an instance already in the requested state (suspended or resumed) does nothing.

While suspended:

- Nothing new is started: no next step, attempt, or compensation.
- In-flight tasks continue until done: a task that has already started runs to completion and its result is recorded.
- Instance and event deadlines are paused. Francis restores whatever was left on resume, so a two-day suspension does not eat a thirty-minute timeout. An in-flight handler's `WithAttemptTimeout` is not paused because the handler keeps running.
- Events are accepted. A `RaiseEvent` for the open `WaitForEvent` step is recorded, and the step completes on resume. However, a `RaiseEvent` does not automatically cause the workflow to be resumed.
- Workflows can be canceled: `Cancel` on a suspended instance resumes it straight into `compensating` (but does not start the compensation). Suspending during a rollback pauses it at the step it had reached.
- Children are not affected. A suspended parent's children keep running, and their results wait for the resume. Suspend a child explicitly if that is not what you want: `GetStatus` on the parent lists every child's instance ID.

`GetStatus` reports the suspension, its reason, and what the instance goes back to:

```go
status, err := svc.GetStatus(ctx, id)
if status.Suspended != nil {
	// status.Suspended.Reason, .At, .ResumeTo
}
```

`List(&workflow.ListOptions{Status: workflow.StatusSuspended})` finds everything currently paused.

## Which one to use

- Use a wait step when the workflow needs something from outside: an approval, a callback, a third party confirming. It is part of the graph, has its own timeout, and its payload feeds the steps after it.
- Use suspend when something operational is wrong, e.g. a dependency under maintenance, a bad deployment, or an incident. It is not part of the graph, and any instance can be suspended at any point.
