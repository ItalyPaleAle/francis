---
title: "Child workflows"
weight: 60
description: "Running one workflow from another, and what crosses between them"
---

A **child workflow** is a step whose task is a whole instance of another registered definition. The child has its own status, compensation stack, timers, and attempts. Only its result reaches the parent.

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

Register the child like any other workflow, on the same hosts:

```go
err = host.RegisterBuiltInActor(provisionDatabase)
err = host.RegisterBuiltInActor(onboarding)
```

## Why reach for one

There are two reasons, and they are different:

**Composition.** A subsystem complex enough to be a workflow on its own becomes a child, and the parent treats it as one step. The parent's graph stays readable and the child is reusable.

**Keeping a wide fan-out small.** Every task's output is recorded in its instance, so a fan-out of several hundred large outputs runs into `WithMaxJournalSize` (see [steps and data flow](/workflows/steps#what-a-step-outputs)). A child per batch splits that across instances.

## What crosses between them

For each child task, the parent records the child's instance ID and, once it terminates, its output or its failure and compensation outcome. Nothing else crosses.

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

The ID is derived rather than random, so a retry can never start a second child for the same task. The parent's ID is also a prefix of the child's, which helps when grepping logs.

`WithMaxDepth` (default 8) bounds how deep a chain of children may go.

## Failure

A child that terminates `failed` or `cancelled` **fails the parent's task**. The parent's own step policy decides what that costs: the default rolls back, `WithOptional` does not, and a fan-out's `TolerateFailures` records it and carries on.

A child's terminal `compensation: partial` is surfaced on the parent even when the parent carries on.

## Unwinding a child

Compensating a child step means asking the child to undo itself:

- A child that is still **running** receives a cancel, rolls back its own stack, and reports when it terminates.
- A child that already **completed** is asked to undo itself. It moves back to `compensating` and pops its stack in reverse. This is why a completed child is kept for as long as its parent is running.

```
parent:  provision ──────────────────────► ✗ verify
           ├─ child "database"  completed
           └─ child "storage"   completed

unwind:  unwind(database) ∥ unwind(storage)   then   close-review-ticket
           └─ revoke creds, delete cluster
```

A parent's `Cancel` cancels its running children, and a parent's step timeout cancels the child it was waiting for. A parent's `Suspend` does **not** propagate.

## Lifetime

`Purge` on a parent purges its children first, recursively, and is safe to repeat.

The auto-purge sweep **skips any instance whose parent is still running**, so a terminated child sticks around until its parent is purged. **Give a child definition its own `WithAutoPurge`, or make sure its parents are purged**, or terminated children accumulate.

## Listing children

Every child records its parent:

```go
// Every child of one instance, whatever the child definition
page, err := childSvc.List(ctx, &workflow.ListOptions{Parent: tenantID})
```

A child's own status names its parent too:

```go
status, err := childSvc.GetStatus(ctx, childID)
// status.Parent.InstanceID, .Workflow, .Step, .Index, .Depth
```
