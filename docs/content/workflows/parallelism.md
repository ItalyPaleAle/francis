---
title: "Parallel steps and fan-out"
weight: 30
description: "Parallel, ForEach, failure policies, capabilities, and capacity"
---

Two steps run more than one task at a time: a **parallel group**, whose members are fixed at definition time, and a **fan-out**, whose size is decided at runtime. Both place their tasks independently across the cluster, so more hosts mean more of them run at once.

## Static parallel groups

`workflow.Parallel(name, steps...)` creates one task per member. The group completes when every member has reported.

```go
workflow.Parallel("notify",
	workflow.Step("email", workflow.WithRun(sendEmail)),
	workflow.Step("sms", workflow.WithRun(sendSMS)),
),
```

Members are independent: none can read another's output, since `WithInputFrom` only ever names a top-level step that ran before the group. A later step reads the group's output as an object keyed by member name:

```go
var group map[string]string
err := t.DecodeOutput("notify", &group)
// group["email"], group["sms"]
```

A member may be a plain step or a [child step](/workflows/child-workflows). Every option a member declares applies to that member's task alone: its own `WithCompensate`, its attempt budget and backoff, its `WithInputFrom` dependencies, and its required capability.

Options that apply to the group as a whole go on the returned spec's `With` method, since the members take the variadic slot:

```go
workflow.Parallel("notify",
	workflow.Step("email", workflow.WithRun(sendEmail), workflow.WithMaxAttempts(5)),
	workflow.Step("sms", workflow.WithRun(sendSMS)),
).With(workflow.WithFailurePolicy(workflow.TolerateFailures)),
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

The item list is fixed when the upstream step reports, and recorded from then on, so it never changes underneath a fan-out that is already running.

The items are always a step's **whole** output: there is no selector for fanning out over one field of something larger. When the list needs deriving, write a small step that returns it, as `plan` does above. It is an ordinary step, so it gets its own retries, timeout, and trace like any other.

### Bounding a fan-out

`WithMaxParallel(n)` bounds how many of a fan-out's tasks are in flight **per instance**. It is a sliding window over the tasks in index order: the first `n` that are not yet done are dispatched, and as results arrive the window slides.

This is separate from `WithConcurrency`, which limits how much work a host accepts across **all** instances. A fan-out of 500 with `WithMaxParallel(8)`, on four hosts each running `WithConcurrency(4)`, has at most 8 in flight for that instance and at most 16 running across the cluster.

`WithMaxParallel` knows nothing about cluster capacity. Asking for more in-flight tasks than the cluster can run does not fail the fan-out: the surplus simply waits its turn, so the step paces itself and takes longer.

## Failure policies

A group or a fan-out declares what one failing task costs the step:

| Policy | Behavior |
|--------|----------|
| `workflow.FailFast` *(default)* | The first failure fails the step. Tasks not yet dispatched are never dispatched, and pending jobs are cancelled. Tasks already running are not interrupted. |
| `workflow.CollectFailures` | Every task runs to completion, then the step fails if any of them failed. Use when the tasks are independent and partial progress is worth having before unwinding. |
| `workflow.TolerateFailures` | Every task runs to completion and the step succeeds regardless. Failures are visible in the step's output, and it is the next step's business what to do about them. |

`TolerateFailures` is the thumbnail case: a thumbnail that cannot be encoded is recorded as failed in the manifest and does not stop the run.

```go
var results []json.RawMessage
err := t.DecodeOutput("generate", &results)
// A failed slot carries {"error": "..."} in place of the value
```

A task that has already started is **never interrupted**, under any policy: Francis does not cancel a running handler's context to stop it early. A task that wants to bail out should watch its context for host shutdown and otherwise run to the end. If it succeeds after the step has already failed, its result is still recorded and still compensated, because the work really happened.

## Capabilities

A step can require a capability, and it is then only ever run on a host that advertises it:

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

A step with no requirement runs anywhere. A step's **compensation runs on a host with the same capability**, since undoing usually needs whatever the forward task needed: the same GPU, the same region.

Throughput then scales by adding hosts that advertise the capability, with no change to the definition.

## Capacity

`WithConcurrency(n)` is the strict maximum number of tasks a host runs at once for this workflow. Compensations have a separate budget, set by `WithCompensateConcurrency` and defaulting to the same number, so a slow rollback cannot starve forward work or the other way around.

Two things to watch for:

- **Every step of a workflow shares one budget.** A step that spends its time waiting on a remote server holds a slot sized for an image encode. When that matters, give the step a required capability, which gives it a budget of its own.
- **`WithConcurrency` is per host.** Raising it on one host does not raise it anywhere else, and the cluster's total is the sum across the hosts that registered the workflow.
