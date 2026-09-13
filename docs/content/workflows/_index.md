---
title: "Workflows"
nav_title: "Overview"
weight: 29
description: "Durable multi-step processes, with parallelism, compensation, and child workflows"
---

A workflow is a **durable multi-step process**: a declared graph of named steps, plus the plain Go functions that implement them. Francis runs it as a built-in actor, so it survives restarts, host loss, and rebalancing, and its steps are spread across the cluster.

```go
import "github.com/italypaleale/francis/builtin/workflow"

checkout, err := workflow.New("checkout",
	workflow.WithSteps(
		workflow.Step("reserve-inventory",
			workflow.WithRun(reserveInventory),
			workflow.WithCompensate(releaseInventory),
		),
		workflow.Step("charge",
			workflow.WithRun(chargeCard),
			workflow.WithCompensate(refundCharge),
		),
		workflow.Step("create-shipment", workflow.WithRun(createShipment)),
	),
)
if err != nil {
	return err
}

// Register it before the host starts, on every host that should run its steps
err = host.RegisterBuiltInActor(checkout)
```

```go
// Start an instance, and read its status whenever you like
svc := checkout.Service(host.Service())

id, created, err := svc.Start(ctx, OrderInput{OrderID: "A-91", Total: 4999})
status, err := svc.GetStatus(ctx, id)
```

If `create-shipment` fails, the engine runs `refundCharge` and then `releaseInventory`, in that order, and the instance terminates as `failed` with its rollback recorded. That is the whole idea: **declare the steps and what undoes them, and the engine handles the rest.**

## What you get

- **Sequences, parallel groups, and dynamic fan-out** over a list sized at runtime.
- **Compensation**: a per-step callback that undoes a step that succeeded, run in reverse order when the workflow fails or is cancelled.
- **Child workflows**, each with its own journal, whose result alone enters the parent's.
- **Waiting on external events**, so a step can park until a human approves or another system calls in.
- **Suspend and resume**, with the instance's deadlines paused while it is parked.
- **Per-step retry policies**, recorded in the journal and visible in a status query.
- **Listing, purging, metrics, and traces**, and a journal an operator can read.

## The orchestration boundary

One rule shapes everything else:

> The `Workflow` actor orchestrates. It never performs a step.

It reads and writes its own state, arms and drops its timers, and dispatches jobs. **Every unit of work runs on a separate worker actor** — every call to a database, an object store, or an HTTP API, and every CPU-bound encode. That is why your handlers can do whatever they like, including calling `time.Now()`, using randomness, and starting goroutines.

You do not have to keep this rule yourself, because the engine gives your code nowhere else to run: `WithRun` and `WithCompensate` are the only places a handler appears, and both are invoked exclusively by a worker. The [How it works](/workflows/how-it-works) page explains what that buys and how it is enforced.

## When to reach for one

Use a workflow when a process has **several steps that must all happen**, some of which have side effects you would have to undo. Booking a trip, provisioning a tenant, running a media pipeline, fulfilling an order: anything where "step three failed and steps one and two already took money" is a real problem.

Do **not** use one when:

- The work is a single unit that either happens or does not. That is a durable [job](/docs/jobs).
- You want a pool of independent long-running tasks with no ordering between them and no result. That is a [task pool](/builtin-actors/task-pool).
- You need arbitrary control flow — loops over a changing condition, dynamic graph rewriting, "retry the whole thing with a different parameter". A workflow is a declared graph; express that outside it, or with a child workflow per attempt.

Francis workflows are deliberately **not** code-as-workflow: there is no replay of a Go function, and therefore no determinism rules on your code. The cost is expressiveness; the benefit is that nothing is hidden, and the journal is a document you can read.

## Where to go next

| Page | What it covers |
|------|----------------|
| [Defining a workflow](/workflows/defining) | `workflow.New`, the options, registering, and versions |
| [Steps and data flow](/workflows/steps) | The step kinds, what each task receives, and what a failure costs |
| [Parallel steps and fan-out](/workflows/parallelism) | `Parallel`, `ForEach`, and the three failure policies |
| [Compensation](/workflows/compensation) | The stack, ordering, and writing an undo that is safe to run twice |
| [Waiting and pausing](/workflows/events-and-suspension) | `WaitForEvent`, `RaiseEvent`, `Suspend`, and `Resume` |
| [Child workflows](/workflows/child-workflows) | `Child`, `WithChild`, and what crosses between journals |
| [Running and observing](/workflows/running) | Starting, status, listing, cancelling, retention, and purging |
| [Deploying and versioning](/workflows/deploying) | The registry, what needs a version bump, and rolling deployments |
| [Metrics and tracing](/workflows/observability) | Every instrument, and the two that matter most |
| [How it works](/workflows/how-it-works) | The actors, the turn, the journal, and the invariants |
| [Examples](/workflows/examples) | Three complete workflows, with every handler and what happens when they go wrong |
