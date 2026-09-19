---
title: "Workflows"
nav_title: "Overview"
weight: 29
description: "Durable multi-step processes, with parallelism, compensation, and child workflows"
---

A workflow is a durable multi-step process: a declared graph of named steps, plus the Go functions that implement them. It runs as a built-in actor, so it survives restarts and host loss, and its steps are spread across the cluster. Steps that fail with transient errors can be retried automatically.

Define a workflow:

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

Invoke a workflow:

```go
// Start an instance, and read its status whenever you like
svc := checkout.Service(host.Service())

id, created, err := svc.Start(ctx, OrderInput{OrderID: "A-91", Total: 4999})
status, err := svc.GetStatus(ctx, id)
```

If `create-shipment` fails, Francis runs `refundCharge` and then `releaseInventory`, in that order, and the instance terminates as `failed`.

## What you get

- **Sequences, parallel groups, and dynamic fan-out** over a list sized at runtime.
- **Loops**, repeating a body until a condition holds.
- **Compensation**: a per-step callback that undoes a step that succeeded, run in reverse order when the workflow fails or is cancelled.
- **Child workflows**, each with its own history.
- **Waiting on external events**, such as a human approval (_human in the loop_).
- **Suspend and resume**, with the instance's deadlines paused while it is parked.
- **Per-step retry policies**, visible in a status query.
- **Listing, purging, metrics, and traces.**

## Your handlers are plain Go

A step's handler is an ordinary function. Francis does not replay your code, so there are no determinism rules: a handler may read the clock, do I/O, use randomness, and start goroutines.

**Handlers must be idempotent**. Delivery is at-least-once, so a handler could run twice.

## When to reach for one

Use a workflow when a process has several steps that must all happen, some of which have side effects you would have to undo: booking a trip, provisioning a tenant, fulfilling an order.

Do **not** use one when:

- The work is a single unit that either happens or does not. Use a durable [job](/docs/jobs).
- You want a pool of independent long-running tasks with no ordering and no result. Use a [task pool](/builtin-actors/task-pool).
- You need arbitrary control flow beyond what is provided.

## Where to go next

| Page | What it covers |
|------|----------------|
| [Defining a workflow](/workflows/defining) | `workflow.New`, the options, registering, and versions |
| [Steps and data flow](/workflows/steps) | The step kinds, what each task receives, and what a failure costs |
| [Parallel steps and fan-out](/workflows/parallelism) | `Parallel`, `ForEach`, and the three failure policies |
| [Compensation](/workflows/compensation) | The stack, ordering, and writing an undo |
| [Waiting and pausing](/workflows/events-and-suspension) | `WaitForEvent`, `RaiseEvent`, `Suspend`, and `Resume` |
| [Child workflows](/workflows/child-workflows) | `Child`, `WithChild`, and what crosses between instances |
| [Running and observing](/workflows/running) | Starting, status, listing, cancelling, retention, and purging |
| [Deploying and versioning](/workflows/deploying) | Versions, rolling deployments, and draining old instances |
| [Metrics and tracing](/workflows/observability) | Every instrument, and what to alert on |
| [Examples](/workflows/examples) | Three complete workflows, with every handler |
