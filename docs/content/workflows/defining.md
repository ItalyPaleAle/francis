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

Each host passes **its own capabilities** and concurrency:

```go
// On a GPU host
wf, err := workflow.New("thumbnails",
	workflow.WithCapability("gpu"),
	workflow.WithConcurrency(runtime.NumCPU()),
	workflow.WithSteps( /* ... the same steps ... */ ),
)
```

One call registers everything the workflow needs, including the actors that run its steps and compensations.

## Workflow options

| Option | Description |
|--------|-------------|
| `WithSteps(...)` | The graph, in the order the steps run (required) |
| `WithVersion(n)` | The definition's version, stamped on every instance it starts (defaults to `1`) - see [Deploying and versioning](/workflows/deploying) |
| `WithTimeout(d)` | How long an instance may run before it is failed and unwound (defaults to 1 hour) |
| `WithConcurrency(n)` | The maximum number of tasks this host runs at once (defaults to `1`) |
| `WithCompensateConcurrency(n)` | The same, for compensations (defaults to `WithConcurrency`) |
| `WithCapability(cap)` | Advertise a capability on this host (repeatable) |
| `WithOutput(step)` | The step whose output becomes the instance's output (defaults to the last step that produced one) |
| `WithRetention(policy)` | How long a terminated instance is kept, per terminal status (defaults to 24 hours each) |
| `WithAutoPurgeInterval(d)` | Purge terminated instances past their retention at this interval (defaults to 12 hours) |
| `WithAutoPurgeCron(cron)` | Purge terminated instances past their retention on this cron schedule instead (alternative to `WithAutoPurgeInterval`) |
| `WithCompensationFailurePolicy(p)` | Behavior after a failed compensation step (defaults to `ContinueUnwinding`) |
| `WithUnknownVersionPolicy(p)` | What to do with an instance no host can serve (defaults to `ParkUnknownVersion`) |
| `WithMaxDepth(n)` | How deep a chain of child instances may go (defaults to `8`) |
| `WithMaxInputSize(n)` | Cap on the workflow input (defaults to 64 KiB) |
| `WithMaxOutputSize(n)` | Cap on a single task's output (defaults to 16 KiB) |
| `WithMaxJournalSize(n)` | Cap on everything one instance records (defaults to 1 MiB) |
| `WithLogger(l)` | A logger for instance and task lifecycle events |
| `WithMeter(m)` | The OpenTelemetry meter to record on. Without one the instruments are no-ops |

Keep step outputs small: a key or an ID, not the bytes it refers to. See [steps and data flow](/workflows/steps#what-a-step-outputs) for the sizing rule.

## Versions

`WithVersion` is stamped on every instance the workflow starts and is used to identify one canonical definition. It does not identify one build of your application.

Francis fingerprints the definition the first time it sees a version. It refuses a different fingerprint under the same number.

**Bump the version when the graph or its declared behavior changes.** Examples include:

- Adding, removing, renaming, or reordering a step
- Changing a retry budget, backoff, timeout, or failure policy
- Changing data flow (e.g. with `WithInputFrom` or `WithItemsFrom`)
- Changing a condition, loop, fan-out limit, or required capability
- Adding or removing a compensation
- Selecting a different child workflow or child version
- Changing `WithOutput`, `WithRetention`, a size cap, or `WithMaxDepth`
- Changing the unknown-version or compensation-failure policy

For example, updating the previous sample to this definition must use a new version:

```go
orders, err := workflow.New("order-fulfillment",
	workflow.WithVersion(4), // Was version 3
	workflow.WithSteps(
		workflow.Step("validate", workflow.WithRun(validateOrder)),
		workflow.Step("charge",
			workflow.WithRun(chargeCard),
			workflow.WithMaxAttempts(5), // Was 3
		),
		workflow.Step("notify", workflow.WithRun(sendReceipt)), // New step
	),
)
```

Both changes alter the canonical definition. Reusing version `3` would cause a definition conflict, and Francis refuses to start up.

A version bump is not required for host-local settings or an internal handler change. Examples include:

- Fixing a bug inside `chargeCard`
- Changing an API client used by a handler
- Changing `WithConcurrency` or `WithCompensateConcurrency`
- Changing the capabilities advertised by a host
- Changing the logger, meter, or auto-purge schedule

Francis fingerprints whether a step has a handler, but does not fingerprint the function value.

See [deploying and versioning](/workflows/deploying) for rolling deployments and draining old versions.
