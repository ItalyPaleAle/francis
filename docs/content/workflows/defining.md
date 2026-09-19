---
title: "Defining a workflow"
weight: 10
description: "workflow.New, the options, and registering on a host"
---

A workflow is built with `workflow.New`, which takes a name and a set of options and returns a value you register on a host. The name must be unique within the cluster and must not contain `/`.

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
	workflow.WithAutoPurgeCron("0 3 * * *"),
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

**Register the same workflow on every host that should run its steps**, with the same name and the same graph. A host that does not have it registered is never handed one of its instances or tasks. Register any [child workflow](/workflows/child-workflows) on the same hosts.

Each host passes its **own** capabilities and concurrency:

```go
// On a GPU host
wf, err := workflow.New("thumbnails",
	workflow.WithCapability("gpu"),
	workflow.WithConcurrency(runtime.NumCPU()),
	workflow.WithSteps( /* ... the same steps ... */ ),
)
```

One call registers everything the workflow needs, including the actors that run its steps and compensations.

## What `New` validates

`New` checks the graph, so a mistake is a startup error:

- Step names are unique, non-empty, and contain neither `/` nor `|`.
- No two `WaitForEvent` steps listen for the same event name.
- A plain step has a `WithRun` handler; a child step has a definition; a wait step has no handler.
- `WithInputFrom`, `WithSkipIf`, and `WithItemsFrom` name steps that exist and run **before** the step referring to them.
- `WithSkipOnFailure` names steps that exist and run **after** it.
- A `Parallel` group has at least one member, and its members are plain or child steps.
- `WithOutput` names a step that exists.
- A child definition is itself valid.

## Workflow options

| Option | Description |
|--------|-------------|
| `WithSteps(...)` | The graph, in the order the steps run. **Required.** |
| `WithVersion(n)` | The definition's version, stamped on every instance it starts. Defaults to `1`. See [Deploying and versioning](/workflows/deploying). |
| `WithTimeout(d)` | How long an instance may run before it is failed and unwound. Defaults to 1 hour. |
| `WithConcurrency(n)` | The maximum number of tasks this host runs at once. Defaults to `1`. |
| `WithCompensateConcurrency(n)` | The same, for compensations. Defaults to `WithConcurrency`. |
| `WithCapability(cap)` | Advertise a capability on this host. Repeatable. |
| `WithOutput(step)` | The step whose output becomes the instance's output. Defaults to the last step that produced one. |
| `WithRetention(policy)` | How long a terminated instance is kept, per terminal status. Defaults to 24 hours each. |
| `WithAutoPurgeInterval(d)` | Purge terminated instances past their retention at this interval. Defaults to 12 hours. |
| `WithAutoPurgeCron(cron)` | Purge terminated instances past their retention on this cron schedule instead. |
| `WithCompensationFailurePolicy(p)` | What a failing compensation costs the rest of the rollback. Defaults to `ContinueUnwinding`. |
| `WithUnknownVersionPolicy(p)` | What to do with an instance no host can serve. Defaults to `ParkUnknownVersion`. |
| `WithMaxDepth(n)` | How deep a chain of child instances may go. Defaults to `8`. |
| `WithMaxInputSize(n)` | Cap on the workflow input, checked at `Start`. Defaults to 64 KiB. |
| `WithMaxOutputSize(n)` | Cap on a single task's output. Defaults to 16 KiB. |
| `WithMaxJournalSize(n)` | Cap on everything one instance records. Defaults to 1 MiB. |
| `WithLogger(l)` | A logger for instance and task lifecycle events. |
| `WithMeter(m)` | The OpenTelemetry meter to record on. Without one the instruments are no-ops. |

Keep step outputs small: a key or an ID, not the bytes it refers to. See [steps and data flow](/workflows/steps#what-a-step-outputs) for the sizing rule.

## Versions

`WithVersion` is stamped on every instance the workflow starts. **Bump it whenever you change the graph or a setting that governs how a step runs.** Changing only a handler's body needs no new version.

Francis refuses a second, different definition under the same version number. See [deploying and versioning](/workflows/deploying) for the full list and for rolling deployments.
