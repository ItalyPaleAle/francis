---
title: "Steps and data flow"
weight: 20
description: "The step kinds, what a task receives, and what a failure costs"
---

A **step** is a named node in the graph. A **task** is one execution unit of a step: a plain step has one, a parallel group has one per member, a fan-out has one per item, and a child step's task is a whole child instance. An **attempt** is one run of a task's handler.

Steps are identified by name, not by position. Renaming one is a change of graph, so it needs [a new version](/workflows/deploying).

## Step kinds

| Kind | Constructor | Tasks | Notes |
|------|-------------|-------|-------|
| Plain | `workflow.Step(name, opts...)` | 1 | The common case |
| Parallel group | `workflow.Parallel(name, steps...)` | one per member | Members run concurrently |
| Fan-out | `workflow.ForEach(name, opts...)` | one per item, sized at runtime | Items come from an upstream step's output |
| Child workflow | `workflow.Child(name, opts...)` | 1 (a child instance) | Runs another registered definition |
| Wait for event | `workflow.WaitForEvent(name, opts...)` | 0 | Parks the instance until `RaiseEvent` or a deadline |
| Loop | `workflow.Loop(name, steps...)` | 0 | Repeats its body until a condition holds |

## The handler contract

```go
// RunFunc performs one attempt of a task and returns the output recorded for it
type RunFunc func(ctx context.Context, t workflow.Task) (output any, err error)

// CompensateFunc undoes the effect of one task that had completed successfully
type CompensateFunc func(ctx context.Context, c workflow.Compensation) error
```

A handler is a plain function. It may call the clock, do I/O, use randomness, and start goroutines. It must be **idempotent**: delivery is at-least-once, so a handler can run twice.

```go
type Task interface {
	// Identity, which is also what every log line and span is tagged with
	InstanceID() string
	Workflow() string
	Step() string
	// Index is the position within a parallel group or fan-out, and -1 for a plain step
	Index() int
	// Attempt is 1 on the first execution and increases with each retry
	Attempt() int

	// DecodeInput reads the workflow input, as given to Start
	DecodeInput(into any) error
	// DecodeItem reads this task's fan-out item, and is a no-op otherwise
	DecodeItem(into any) error
	// DecodeOutput reads the output of an upstream step
	// It returns ErrStepSkipped when that step was skipped
	DecodeOutput(step string, into any) error
}
```

A compensation gets everything a task does, plus two more:

```go
type Compensation interface {
	workflow.Task
	// DecodeResult reads the output this task produced when it succeeded
	DecodeResult(into any) error
	// Cause is the error that caused the workflow to unwind, or the cancellation reason
	Cause() string
}
```

## What a task receives

Each task receives only what its step declared it needs:

- the **workflow input**, as given to `Start`;
- the **output of the immediately preceding step**;
- the outputs of any steps named with `WithInputFrom("a", "b")`;
- for a fan-out task, its **item**.

```go
workflow.Step("verify",
	workflow.WithRun(verifyTenant),
	// Read the database step's output too, not only the preceding step's
	workflow.WithInputFrom("provision-database"),
)
```

`DecodeOutput` returns `ErrStepNotFound` for any other step.

## What a step outputs

A handler returns `(any, error)`. The value is JSON-encoded and recorded, subject to `WithMaxOutputSize`. What later steps see depends on the kind:

| Kind | Output seen by later steps |
|------|----------------------------|
| Plain | the handler's return value |
| Parallel group | an object keyed by member name, each member's output as its value |
| Fan-out | an array of the tasks' outputs, ordered by item index; a failed item carries `{"error": "…"}` under `TolerateFailures` |
| Child workflow | the child instance's output |
| Wait for event | the event's payload |
| Skipped | absent; `DecodeOutput` returns `ErrStepSkipped` |

Outputs are for **control flow and small results**. Keep large blobs in an object store and return a reference:

```go
func generateThumbnail(ctx context.Context, t workflow.Task) (any, error) {
	// ... encode, then write to the store ...

	// Return the handle, never the bytes
	return thumbnailResult{Key: key, Width: img.Width, Height: img.Height}, nil
}
```

Every task's output is recorded in the instance, capped in total by `WithMaxJournalSize` (1 MiB by default). Keep **the number of tasks multiplied by a typical output** well under that cap: a few hundred tasks returning under a kilobyte each is comfortable. For a wider fan-out, use a [child workflow](/workflows/child-workflows) per batch.

## Retries, and the three ways a handler can fail

`WithMaxAttempts` and `WithRetryBackoff` decide how many attempts a task gets and how long to wait between them. Compensations get a budget of their own. `GetStatus` shows the attempts each task has spent and why the last one failed.

| Return | What happens |
|--------|--------------|
| `nil` error | The output is recorded and the task completes. |
| An ordinary error | A failed attempt is recorded and retried per `WithMaxAttempts`, after the `WithRetryBackoff` delay. |
| `actor.ErrJobPermanentFailure` | The task fails at once, without further attempts. |
| `actor.ErrJobRejected` | This host declines the task so another runs it, **without counting an attempt**. |

Deciding between the second and third is the handler's job. Return an error as-is when a retry could succeed, and join `ErrJobPermanentFailure` when it cannot:

```go
src, err := store.ReadOriginal(ctx, in.SourceKey)
if errors.Is(err, store.ErrNotFound) {
	// This will never succeed, so do not spend the remaining attempts
	return nil, errors.Join(actor.ErrJobPermanentFailure, err)
} else if err != nil {
	// The store may recover, so this is worth retrying
	return nil, err
}
```

## Per-step options

| Option | Description |
|--------|-------------|
| `WithRun(fn)` | The handler. Required for a plain step and for a fan-out that is not a child fan-out. |
| `WithCompensate(fn)` | The function that undoes a completed task of this step. |
| `WithMaxAttempts(n)` | How many attempts a task gets before it is failed. Defaults to `3`. |
| `WithRetryBackoff(initial, max)` | The delay before the second attempt, doubling to the cap. Defaults to 2s and 1 minute. |
| `WithCompensateMaxAttempts(n)` | The same for the compensation. Defaults to `10`. |
| `WithCompensateBackoff(initial, max)` | The same for the compensation. Defaults to 10s and 10 minutes. |
| `WithStepTimeout(d)` | How long this step may take before its outstanding attempts are failed. |
| `WithOptional()` | This step's failure does not fail the instance. |
| `WithSkipOnFailure(steps...)` | Steps to skip when this one fails. |
| `WithSkipIf(step, value)` | Skip this step when the named upstream step's output equals `value`. |
| `WithInputFrom(steps...)` | Extra upstream outputs this step's tasks receive. |
| `WithRequiredCapability(cap)` | Run this step's tasks only on hosts advertising the capability. |
| `WithCompensateOnFailure()` | Compensate this step even when it failed. |
| `WithItemsFrom(step)` | *(fan-out)* the step whose output supplies the items. |
| `WithMaxParallel(n)` | *(fan-out)* how many tasks are in flight per instance. |
| `WithFailurePolicy(p)` | *(group or fan-out)* what a failing task costs the step. On a group, set it with `.With(...)`. |
| `WithChild(wf)` / `WithDefinition(wf)` | *(fan-out / child step)* the definition to run. |
| `WithEventTimeout(d)` / `WithEventName(n)` | *(wait step)* how long to wait, and for what. |

## What a step's failure costs the workflow

Two independent options say what a step's failure means:

| Declared | After the step fails | Named dependents | Terminal status |
|----------|----------------------|------------------|-----------------|
| *(default)* | the workflow **unwinds** | — | `failed` |
| `WithSkipOnFailure("a", "b")` | the workflow **continues** | recorded as `skipped`, never run | `failed` |
| `WithOptional()` | the workflow continues | — | `completed` |
| both | the workflow continues | recorded as `skipped` | `completed` |

`WithOptional` decides the terminal status, and `WithSkipOnFailure` decides which downstream steps are skipped. Neither rolls the workflow back: only the default case does that.

```go
// The run is a failure without the manifest, but there is nothing to undo
workflow.Step("manifest",
	workflow.WithRun(writeManifest),
	workflow.WithSkipOnFailure("notify"),
),

// Losing the notification does not fail the run
workflow.Step("notify",
	workflow.WithRun(deliverNotification),
	workflow.WithOptional(),
),
```

Skipping is **not transitive**: a skipped step is not a failed one, so its own `WithSkipOnFailure` list is not applied. A skipped step never enters the compensation stack.

## Conditional steps

A condition is a **step that returns a value**. `WithSkipIf` compares that step's output against the value you name:

```go
workflow.Step("approved", workflow.WithRun(readApproval)),

// Skipped when "approved" recorded false
workflow.Step("verify",
	workflow.WithRun(verifyTenant),
	workflow.WithSkipIf("approved", false),
),
```

A step whose condition recorded nothing, because it failed or was skipped itself, is **not** skipped.

## Loops

`workflow.Loop` repeats a body until a condition holds. The condition is a body step that returns a boolean, exactly as `WithSkipIf` is:

```go
workflow.Loop("poll",
	workflow.Step("check", workflow.WithRun(checkReady)),
	workflow.Step("pause", workflow.WithRun(waitABit)),
).With(
	workflow.WithUntil("check", true),
	workflow.WithMaxIterations(20),
)
```

The body steps are ordinary steps of the workflow. They appear in a status query under their own names, and the step after the loop can read what they produced.

A few things to know:

- The condition is read **after** the body, so the body always runs at least once.
- The **whole body** runs every iteration. To skip part of it on the round that ends the loop, put `WithSkipIf` on that step, as in `WithSkipIf("check", true)` on `pause` above.
- A loop reports the output of the step its condition named, so the step after a loop reads the loop itself rather than a body step.
- `WithMaxIterations` defaults to 100. A loop whose condition has not held by the last iteration **fails**, which is what keeps a condition that never becomes true from running the instance to its timeout. `WithOptional` and `WithSkipOnFailure` decide what that costs, exactly as for any other step.
- Every iteration's work is **compensated**. A body step that ran five times and succeeded each time has five effects to undo, and the unwind undoes all of them.
- A body holds plain, child, and wait steps, because a loop runs one task at a time. For a parallel group or a fan-out inside a loop, put it in a [child workflow](/workflows/child-workflows) the body starts.
- A body step reads the outputs of the steps before it **in the graph**, not from the previous iteration. Carry state across iterations through the instance input or your own store.
