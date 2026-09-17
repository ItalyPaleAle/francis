---
title: "Child workflows"
weight: 60
description: "Running one workflow from another, and what crosses between them"
---

A **child workflow** is a step whose task is a whole instance of another registered definition. The child has its own status, its own compensation stack, its own timers, and its own attempts. Only its result reaches the parent.

```go
// A step that runs one child instance
workflow.Child("provision-database", workflow.WithDefinition(provisionDatabase)),

// A fan-out that runs one child instance per item
workflow.ForEach("ship",
	workflow.WithItemsFrom("plan-shipments"),
	workflow.WithChild(shipmentWorkflow),
	workflow.WithMaxParallel(8),
),
```

The child is registered on the hosts like any other workflow, and on the same hosts:

```go
err = host.RegisterBuiltInActor(provisionDatabase)
err = host.RegisterBuiltInActor(onboarding)
```

## Why reach for one

There are two reasons, and they are different:

**Composition.** A subsystem complex enough to be a workflow on its own, with its own steps, rollback, and timeouts, is a child, and the parent treats it as one step. That keeps the parent's graph readable and makes the child reusable.

**Keeping a wide fan-out small.** Every task's output is recorded in its instance, so a fan-out of several hundred large outputs runs into `WithMaxJournalSize` (see [Steps and data flow](/workflows/steps#what-a-step-outputs)). A child workflow per batch splits that across instances instead: a parent of a hundred children with a thousand steps each stays small at every level.

## What crosses between them

For each child task, the parent records only the child's instance ID and, once it terminates, its output, or its failure and compensation outcome. Nothing else crosses.

The child's input is what the task would have received:

- a child of a **fan-out** gets its **item**, so "one child per element" reads the way it looks;
- any other child gets the **preceding step's output**, falling back to the parent's own input when there is none.

```go
func shipOne(ctx context.Context, t workflow.Task) (any, error) {
	// In the child, the parent's item arrived as the workflow input
	var box shipmentSpec
	err := t.DecodeInput(&box)
	// ...
}
```

The child's output is the output of its last step, or of the step named by `WithOutput` on the **child's** definition:

```go
provisionDatabase, err := workflow.New("provision-database",
	// The parent reads this step's output as the child's result
	workflow.WithOutput("credentials"),
	workflow.WithSteps(
		workflow.Step("create-cluster", workflow.WithRun(createCluster), workflow.WithCompensate(deleteCluster)),
		workflow.Step("create-schema", workflow.WithRun(createSchema)),
		workflow.Step("credentials", workflow.WithRun(issueCredentials), workflow.WithCompensate(revokeCredentials)),
	),
)
```

```go
// In the parent, the child's step output is read like any other
var creds databaseCredentials
err := t.DecodeOutput("provision-database", &creds)
```

## Instance IDs

A child's instance ID is derived from its parent's:

```
<parentID>|<step>|<index>
```

The ID is derived rather than random, so retries can never start a second child for the same task. It also means the parent's ID is a prefix of the child's, which is handy when grepping logs.

`WithMaxDepth` (default 8) bounds how deep a chain of children may go, and is what stops a definition that references itself.

## Failure

A child that terminates `failed` or `cancelled` **fails the parent's task**. What that costs the parent is the parent's own step policy: the default unwinds, `WithOptional` does not, a fan-out's `TolerateFailures` records it and carries on.

A child's terminal `compensation: partial` is surfaced on the parent even when the parent carries on, so "the child rolled back, but not completely" is never lost.

## Unwinding a child

Compensating a child step means asking the child to undo itself:

- A child that is still **running** receives a cancel, unwinds its own stack, and reports back when it terminates.
- A child that already **completed** is asked to undo itself. It moves back to `compensating`, pops its stack in reverse exactly as a failure would, and reports its own compensation outcome. This is why a completed child is kept for as long as its parent is running.

```
parent:  provision ──────────────────────► ✗ verify
           ├─ child "database"  completed
           └─ child "storage"   completed

unwind:  unwind(database) ∥ unwind(storage)   then   close-review-ticket
           └─ revoke creds, delete cluster
```

A parent's `Cancel` cancels its running children through the same path, and a parent's step timeout cancels the child it was waiting for. A parent's `Suspend` does **not** propagate.

## Lifetime

`Purge` on a parent purges its children first, recursively, and is safe to repeat if it is interrupted.

The auto-purge sweep **skips any instance whose parent is still running**, so a child is never removed from under a parent that might still ask it to undo itself. That has one consequence worth planning for: a terminated child sticks around until its parent is purged, or until the sweep finds it after the parent is gone. **Give a child definition its own `WithAutoPurge`, or make sure its parents are purged**, or terminated children accumulate.

## Listing children

Every child records its parent, so you can list them:

```go
// Every child of one instance, whatever the child definition
page, err := childSvc.List(ctx, &workflow.ListOptions{Parent: tenantID})
```

A child's own status names its parent too:

```go
status, err := childSvc.GetStatus(ctx, childID)
// status.Parent.InstanceID, .Workflow, .Step, .Index, .Depth
```
