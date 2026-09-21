---
title: "Parallel steps and fan-out"
weight: 30
description: "Parallel, ForEach, failure policies, capabilities, and capacity"
---

Two steps run more than one task at a time: a **parallel group**, whose members are fixed at definition time, and a **fan-out**, whose size is decided at runtime. Both spread their tasks across the cluster.

## Static parallel groups

`workflow.Parallel(name, steps...)` creates one task per member. The group completes when every member has reported.

```go
workflow.Parallel("notify",
	workflow.Step("email", workflow.WithRun(sendEmail)),
	workflow.Step("sms", workflow.WithRun(sendSMS)),
),
```

Members cannot read each other's output. `WithInputFrom` only names a top-level step that ran before the group. A later step reads the group's output as an object keyed by member name:

```go
var group map[string]string
err := t.DecodeOutput("notify", &group)
// group["email"], group["sms"]
```

A member may be a plain step or a [child step](/workflows/child-workflows). Every option a member declares applies to that member's task alone.

Options for the group as a whole go on its `With` method:

```go
workflow.
	Parallel("notify",
		workflow.Step("email", workflow.WithRun(sendEmail), workflow.WithMaxAttempts(5)),
		workflow.Step("sms", workflow.WithRun(sendSMS)),
	).
	With(
		workflow.WithFailurePolicy(workflow.TolerateFailures),
	),
```

## Dynamic fan-out

`workflow.ForEach` creates one task per element of an upstream step's output, which must decode to a JSON array:

```go
workflow.Step("plan", workflow.WithRun(planThumbnails)),

workflow.ForEach("generate",
	workflow.WithItemsFrom("plan"),
	workflow.WithRun(generateThumbnail),
	workflow.WithMaxParallel(8),
	workflow.WithFailurePolicy(workflow.TolerateFailures),
),
```

Each task reads its own element with `DecodeItem`:

```go
func generateThumbnail(ctx context.Context, t workflow.Task) (any, error) {
	var spec thumbnailSpec
	err := t.DecodeItem(&spec)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}
	// ... t.Index() is this item's position ...
}
```

The item list is fixed once the upstream step reports, so it never changes underneath a running fan-out.

The items are always a step's whole output: there is no selector for one field of something larger. Write a small step that returns the list, as `plan` does above.

### Bounding a fan-out

`WithMaxParallel(n)` bounds how many of a fan-out's tasks are in flight per instance, as a sliding window in index order.

`WithConcurrency` is separate: it limits how much work a host accepts across all instances. A fan-out of 500 with `WithMaxParallel(8)`, on four hosts each running `WithConcurrency(4)`, has at most 8 in flight for that instance and 16 across the cluster.

Asking for more in-flight tasks than the cluster can run does not fail the fan-out. The surplus waits, and the step takes longer. Queue wait does not consume a task's `WithAttemptTimeout`, only the workflow's overall `WithTimeout` continues to run.

## Failure policies

A group or a fan-out declares what one failing task costs the step:

| Policy | Behavior |
|--------|----------|
| `workflow.FailFast` *(default)* | The first failure fails the step. Tasks not yet dispatched are never dispatched, and pending jobs are cancelled. Tasks already running are not interrupted. |
| `workflow.CollectFailures` | Every task runs to completion, then the step fails if any of them failed. Use when the tasks are independent and partial progress is worth having before unwinding. |
| `workflow.TolerateFailures` | Every task runs to completion and the step succeeds regardless. Failures are visible in the step's output, and it is the next step's business what to do about them. |

A task that has already started is never interrupted under any policy. A handler that wants to bail out early should watch its context for host shutdown. If a task succeeds after its step has already failed, the result is still recorded and still compensated.

## Capabilities

A step can require a capability, and it is then only ever run on a host that advertises it.  
A step with no requirement runs anywhere.

```go
// On every host: the step declares what it needs
workflow.Step("ocr",
	workflow.WithRun(runOCR),
	workflow.WithRequiredCapability("gpu"),
)

// On a GPU host only: the host declares what it has
wf, err := workflow.New("documents",
	workflow.WithCapability("gpu"),
	workflow.WithSteps( /* ... */ ),
)
```

A step's compensation runs on a host with the same capability.

## Capacity

`WithConcurrency(n)` is the maximum number of tasks a host runs at once for this workflow. Compensations have a separate budget, `WithCompensateConcurrency`, so a slow rollback cannot starve forward work.

Two things to watch for:

- Every step shares one budget. A step that waits on a remote server holds a slot an image encode could have used. Give it a required capability to get a budget of its own.
- `WithConcurrency` is per host. The cluster's total is the sum across the hosts that registered the workflow.
