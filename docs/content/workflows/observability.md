---
title: "Metrics and tracing"
weight: 90
description: "Every instrument, and the two that matter most"
---

Pass an OpenTelemetry meter with `WithMeter` and the engine records on it. Without one, the instruments are no-ops, so the engine records without nil checks either way.

```go
wf, err := workflow.New("order-fulfillment",
	workflow.WithMeter(meter),
	workflow.WithLogger(log),
	workflow.WithSteps( /* ... */ ),
)
```

## The two that matter most

**`francis.workflow.turn.duration`** is how long the engine spends deciding what an instance does next, once for each result it takes in. It should sit in **single-digit milliseconds**. If it climbs, every instance is slower to advance, and status reads start queueing behind it.

**`francis.workflow.turns.duplicate_events`** counts results the engine received more than once and had already recorded. A low, non-zero rate is healthy: it is how an instance recovers when a host dies partway through handing off work. A climbing rate means work is repeatedly failing to be handed off, so look at `task.transport_failures` and the provider next.

## Every instrument

| Instrument | Kind | Attributes |
|------------|------|------------|
| `francis.workflow.instances.started` | counter | `workflow` |
| `francis.workflow.instances.terminated` | counter | `workflow`, `status` |
| `francis.workflow.instances.running` | up-down counter | `workflow` |
| `francis.workflow.instance.duration` | histogram (s) | `workflow`, `status` |
| `francis.workflow.step.duration` | histogram (s) | `workflow`, `step`, `outcome` |
| `francis.workflow.task.attempts` | counter | `workflow`, `step`, `failed` |
| `francis.workflow.task.transport_failures` | counter | `workflow`, `step` or `method` |
| `francis.workflow.compensations.run` | counter | `workflow`, `step` |
| `francis.workflow.compensations.failed` | counter | `workflow`, `step` |
| `francis.workflow.instances.suspended` | counter | `workflow` |
| `francis.workflow.children.started` | counter | `workflow`, `child` |
| `francis.workflow.instances.purged` | counter | `workflow` |
| `francis.workflow.turn.duration` | histogram (s) | `workflow` |
| `francis.workflow.turns.duplicate_events` | counter | `workflow`, `event` |
| `francis.workflow.definition.conflicts` | counter | `workflow`, `version` |

`task.transport_failures` is worth watching separately from ordinary attempt failures: it counts attempts that failed because the **result could not be delivered**, not because the handler failed. A task that ran and could not report is a different problem from a task that ran and failed, because the work probably did happen and is about to happen again. This is the counter that says your handlers' idempotency is being exercised for real.

## What to alert on

- `instances.terminated{status="failed"}` climbing.
- A terminal `compensation: partial` or `compensation: failed` outcome. These are the "money may be stranded" cases, and they are the reason the compensation outcome is reported separately from the status. Watch `compensations.failed` per workflow, and read `GetStatus` for the instance itself.
- `definition.conflicts` above zero, at any rate: two hosts are serving different graphs under one version number. See [Deploying and versioning](/workflows/deploying#if-you-forget-to-bump).
- `turn.duration` regressing past a few milliseconds.

## Tracing

A workflow instance is long-lived and spread across hosts, so it is not one span. Instead:

- `Start` records the caller's trace context on the instance, and it is carried through everything the engine dispatches afterwards.
- There is **one span per attempt** and one for each time the engine advances the instance, tagged with instance ID, workflow, version, step, index, and attempt. Each is linked back to the instance's original trace context, so you can follow a run either from the request that started it or from any single step.
- A child instance's spans link to its parent's trace context as well as its own.

## Logs

`WithLogger` gets you instance and task lifecycle events, every line tagged with the instance ID and, for a task, the step, index, and attempt.

The actor IDs in those lines are readable rather than hashed, so they are what to grep for. A task's worker is `<instanceID>|<step>|<index>`, and a [child instance](/workflows/child-workflows#instance-ids) uses the same shape, which means a parent's ID is a prefix of everything underneath it.
