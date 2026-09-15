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

The size is decided once, when the upstream step reports, and is then **journaled**: a retried turn re-reads the recorded items rather than re-deriving them against a journal that has since moved on.

**Deriving the list is a normal step that runs on a worker.** An expander callback invoked on the orchestrator would be simpler to write, but it would put an arbitrary user function on the instance's control plane, which is the exact thing the [orchestration boundary](/workflows/how-it-works#the-orchestration-boundary) exists to prevent. The cost is one extra durable round-trip; the benefit is that the expansion is itself retried, traced, bounded by a deadline, and recorded like any other step.

The list is always a step's **whole** output. There is no selector for fanning out over one field of the input, because that would mean an expression language to save a round-trip, and a small `plan` step is the honest price.

### Bounding a fan-out

`WithMaxParallel(n)` bounds how many of a fan-out's tasks are in flight **per instance**. It is a sliding window over the tasks in index order: the first `n` that are not yet done are dispatched, and as results arrive the window slides.

This is separate from `WithConcurrency`, which limits how much work a host accepts across **all** instances. A fan-out of 500 with `WithMaxParallel(8)`, on four hosts each running `WithConcurrency(4)`, has at most 8 in flight for that instance and at most 16 running across the cluster.

`WithMaxParallel` knows nothing about cluster capacity. When the in-flight tasks exceed the cluster's total budget, each surplus occurrence is released and re-fetched an alarm poll interval later, so an over-subscribed fan-out degrades to poll-interval pacing rather than failing.

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

Cancelling a pending job does not interrupt one that is already executing, and Francis does not cancel the handler's context. A task that wants to stop early should watch its context for host shutdown and otherwise finish; the engine records its late report as a result. If it **succeeded** after the step was already decided, the engine still records it and still compensates it, because the work really happened.

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

A step with no requirement runs anywhere. A step's **compensation is routed to the undo queue of the same capability**, since undoing almost always needs the placement the forward task had: the same GPU, the same region.

That makes "the OCR step only runs on hosts with a GPU" a one-line property of the definition, and it means throughput scales by adding hosts with no change to the definition at all.

## Capacity

`WithConcurrency(n)` puts every worker queue of the workflow into **one capacity group** with a strict, in-process per-host budget, and mirrors it as a cluster-wide placement hint so hosts are rarely handed more than they can run. The undo queues form a second group, sized by `WithCompensateConcurrency` (defaulting to the same number), so a slow unwind cannot starve forward work or the other way around.

Two things worth knowing:

- **Every step of a workflow shares one worker budget.** A slow step occupies a slot an expensive one could have used — a callback waiting on a remote server holds a slot sized for an image encode. When that matters, give the step its own budget with a required capability.
- **There is no per-step capacity group.** Capability queues already give a step its own budget when it needs one, at no extra concept.
