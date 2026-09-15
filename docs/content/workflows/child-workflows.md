---
title: "Child workflows"
weight: 60
description: "Running one workflow from another, and what crosses between them"
---

A **child workflow** is a step whose task is a whole instance of another registered definition. The child has its own journal, its own compensation stack, its own timers, and its own attempts; only its result enters the parent's.

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

Two reasons, and they are different:

**Composition.** A subsystem complex enough to be a workflow on its own — with its own steps, its own rollback, its own timeouts — is a child, and the parent treats it as one step. That keeps the parent's graph readable and makes the child reusable.

**Journal width.** Every report rewrites the parent's whole journal, so a very wide fan-out is expensive (see [How it works](/workflows/how-it-works#size-and-write-amplification)). A child workflow per batch moves that width into journals that are rewritten independently. A parent of a hundred children with a thousand steps each is a journal of a hundred small records.

## What crosses between journals

The parent's journal records, per child task, only the child's instance ID and — once it terminates — its output, or its failure and compensation outcome. Nothing else crosses.

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

That is deterministic, so a retried turn finds the same child rather than starting a second. It also means a child is always locatable from its parent's journal, and the parent's ID is a prefix of the child's — useful when grepping logs.

`WithMaxDepth` (default 8) bounds the parent chain, which is the only thing that stops a definition that references itself.

## Failure

A child that terminates `failed` or `cancelled` **fails the parent's task**. What that costs the parent is the parent's own step policy: the default unwinds, `WithOptional` does not, a fan-out's `TolerateFailures` records it and carries on.

A child's terminal `compensation: partial` is surfaced in the parent's journal even when the parent continues, so "the child rolled back, but not completely" is never lost.

## Unwinding a child

Compensating a child step means asking the child to undo itself:

- A child that is still **running** receives a cancel, unwinds its own stack, and reports back when it terminates.
- A child that already **completed** receives an unwind — a verb only a parent may send. It moves back to `compensating`, pops its stack in reverse exactly as a failure would, and reports its own compensation outcome. That is why a completed child is kept, not purged, for as long as its parent is running.

```
parent:  provision ──────────────────────► ✗ verify
           ├─ child "database"  completed
           └─ child "storage"   completed

unwind:  unwind(database) ∥ unwind(storage)   then   close-review-ticket
           └─ revoke creds, delete cluster
```

A parent's `Cancel` cancels its running children through the same path, and a parent's step timeout cancels the child it was waiting for. A parent's `Suspend` does **not** propagate.

## Lifetime

A child's retention follows its parent's. `Purge` on a parent purges its children first, recursively, then its own jobs, then its journal — in that order, so an interrupted purge is safe to repeat.

The auto-purge sweep **skips instances whose parent is still running**, so a child is never purged from under a parent that might still unwind it. A terminated child's journal is written **without an expiry** for the same reason: a parent may ask it to undo itself for as long as the parent runs, and an expiry the parent cannot see would take that journal out from under it. What removes a child is its parent's purge, or the sweep once the parent's journal is gone — so a child definition wants either `WithAutoPurge` of its own or a parent that is purged.

## Listing children

Every child instance records its parent, and the listing index carries it:

```go
// Every child of one instance, whatever the child definition
page, err := childSvc.List(ctx, &workflow.ListOptions{Parent: tenantID})
```

A child's own status names its parent too:

```go
status, err := childSvc.GetStatus(ctx, childID)
// status.Parent.InstanceID, .Workflow, .Step, .Index, .Depth
```
