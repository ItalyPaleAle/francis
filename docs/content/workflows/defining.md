---
title: "Defining a workflow"
weight: 10
description: "workflow.New, the options, and registering on a host"
---

A workflow is built with `workflow.New`, which takes a unique name and a set of options, and returns a value you register on a host. The name is used to build the reserved actor types, so it must be unique within a cluster and must not contain `/`.

```go
import "github.com/italypaleale/francis/builtin/workflow"

orders, err := workflow.New("order-fulfillment",
	workflow.WithVersion(3),
	workflow.WithTimeout(30*time.Minute),
	workflow.WithConcurrency(4),
	workflow.WithRetention(workflow.RetentionPolicy{
		Completed: 24 * time.Hour,
		Failed:    7 * 24 * time.Hour,
		Cancelled: 7 * 24 * time.Hour,
	}),
	// Sweep terminated instances past their retention every night, cluster-wide on one host
	workflow.WithAutoPurge("0 3 * * *"),
	workflow.WithLogger(log),

	workflow.WithSteps(
		workflow.Step("validate", workflow.WithRun(validateOrder)),
		workflow.Step("charge",
			workflow.WithRun(chargeCard),
			workflow.WithCompensate(refundCharge),
			workflow.WithMaxAttempts(5),
			workflow.WithRetryBackoff(2*time.Second, time.Minute),
			workflow.WithStepTimeout(30*time.Second),
		),
		workflow.Step("ship", workflow.WithRun(createShipment)),
	),
)
if err != nil {
	return err
}
```

## Registering

Register the workflow on the host before it starts, the same way as any other built-in actor:

```go
host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Before host.Run
err = host.RegisterBuiltInActor(orders)
```

**Register the same workflow on every host that should run its steps**, with the same name and the same graph. A host that has the workflow registered can orchestrate its instances and run its handlers; a host that does not will never be handed either. Any [child workflow](/workflows/child-workflows) is registered the same way, on the same hosts.

Each host passes its **own** capabilities and its **own** concurrency, so hosts can differ in what they can run and how much:

```go
// On a GPU host
wf, err := workflow.New("thumbnails",
	workflow.WithCapability("gpu"),
	workflow.WithConcurrency(runtime.NumCPU()),
	workflow.WithSteps( /* ... the same steps ... */ ),
)
```

One call registers everything the workflow needs: the orchestrator, the worker and undo queues, the definition registry, and the auto-purge cron job when `WithAutoPurge` is set.

## What `New` validates

`New` builds the graph and checks it, so a mistake is a startup error rather than an instance that stalls halfway through:

- Step names are unique, non-empty, and contain neither `/` nor `|`.
- No two `WaitForEvent` steps listen for the same event name.
- A plain step has a `WithRun` handler; a child step has a definition; a wait step has no handler.
- `WithInputFrom`, `WithSkipIf`, and `WithItemsFrom` name steps that exist and run **before** the step referring to them.
- `WithSkipOnFailure` names steps that exist and run **after** it.
- A `Parallel` group has at least one member, and its members are plain or child steps.
- `WithOutput` names a step that exists.
- A child definition is itself valid, because it was built by its own `New`.

## Workflow options

| Option | Description |
|--------|-------------|
| `WithSteps(...)` | The graph, in the order the steps run. **Required.** |
| `WithVersion(n)` | The definition's version, stamped on every instance it starts. Defaults to `1`. See [Deploying and versioning](/workflows/deploying). |
| `WithTimeout(d)` | How long an instance may run before it is failed and unwound. Defaults to 1 hour. |
| `WithConcurrency(n)` | The strict maximum number of tasks this host runs at once, across every worker queue of the workflow. Defaults to `1`. |
| `WithCompensateConcurrency(n)` | The same budget for the undo queues, which form their own capacity group. Defaults to `WithConcurrency`. |
| `WithCapability(cap)` | Advertise a capability on this host, so steps that require it can run here. Repeatable. |
| `WithOutput(step)` | The step whose output becomes the instance's output. Defaults to the last step that produced one. |
| `WithRetention(policy)` | How long a terminated instance's journal is kept, per terminal status. Defaults to 24 hours each. |
| `WithAutoPurge(cron)` | Register a cron job that sweeps terminated instances past their retention on this schedule. |
| `WithCompensationFailurePolicy(p)` | What a failing compensation costs the rest of the unwind. Defaults to `ContinueUnwinding`. |
| `WithUnknownVersionPolicy(p)` | What to do with an instance no host can serve. Defaults to `ParkUnknownVersion`. |
| `WithMaxDepth(n)` | How deep a chain of child instances may go. Defaults to `8`. |
| `WithMaxInputSize(n)` | Cap on the encoded workflow input, checked at `Start`. Defaults to 64 KiB. |
| `WithMaxOutputSize(n)` | Cap on a single task's encoded output, checked on the worker. Defaults to 16 KiB. |
| `WithMaxJournalSize(n)` | Cap on the encoded journal, checked before every write. Defaults to 1 MiB. |
| `WithLogger(l)` | A logger for instance and task lifecycle events. |
| `WithMeter(m)` | The OpenTelemetry meter the engine records on. Without one the instruments are no-ops. |

The three size caps are not arbitrary. Every report rewrites the whole journal, so an N-task fan-out writes on the order of N² bytes through the state store; [How it works](/workflows/how-it-works#size-and-write-amplification) has the arithmetic. Keep outputs small — a key or an ID, not the bytes they refer to.

## Versions

`WithVersion` is the definition's version, and it is stamped on every instance the workflow starts. **Bump it for any change to the graph**: a step added, removed, renamed, reordered, or with a changed policy. A cluster-wide registry records the fingerprint of each version and refuses a second, different graph under the same number, so a forgotten bump is loud rather than silent.

A change to a **handler's body** alone changes no fingerprint and needs no new version. [Deploying and versioning](/workflows/deploying) covers what that means in practice, and when to bump anyway.
