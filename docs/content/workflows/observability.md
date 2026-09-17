---
title: "Metrics and tracing"
weight: 90
description: "Every instrument, and the two that matter most"
---

Pass an OpenTelemetry meter with `WithMeter`. Without one, the instruments are no-ops.

```go
wf, err := workflow.New("order-fulfillment",
	workflow.WithMeter(meter),
	workflow.WithLogger(log),
	workflow.WithSteps( /* ... */ ),
)
```

## The two that matter most

**`francis.workflow.turn.duration`** is how long the engine spends deciding what an instance does next. It should sit in **single-digit milliseconds**. If it climbs, instances are slower to advance and status reads queue behind it.

**`francis.workflow.turns.duplicate_events`** counts results received more than once. A low, non-zero rate is healthy. A climbing rate means work is repeatedly failing to be handed off, so check `task.transport_failures` and the provider.

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

`task.transport_failures` counts attempts that failed because the **result could not be delivered**, not because the handler failed. The work probably did happen and is about to happen again, so this is the counter that says your handlers' idempotency is being exercised for real.

## What to alert on

- `instances.terminated{status="failed"}` climbing.
- A terminal `compensation: partial` or `compensation: failed` outcome. These are the "money may be stranded" cases. Watch `compensations.failed` per workflow, and read `GetStatus` for the instance itself.
- `definition.conflicts` above zero, at any rate: two hosts are serving different graphs under one version number. See [deploying and versioning](/workflows/deploying#if-you-forget-to-bump).
- `turn.duration` regressing past a few milliseconds.

## Tracing

A workflow instance is long-lived and spread across hosts, so it is not one span. Instead:

- `Start` records the caller's trace context on the instance, and it is carried through everything dispatched afterwards.
- There is **one span per attempt**, and one each time the engine advances the instance, tagged with instance ID, workflow, version, step, index, and attempt. Each links back to the original trace context, so you can follow a run from the request that started it or from any single step.
- A child instance's spans link to its parent's trace context as well as its own.

## Logs

`WithLogger` gets you instance and task lifecycle events, every line tagged with the instance ID and, for a task, the step, index, and attempt.

Actor IDs are readable rather than hashed. A task's worker is `<instanceID>|<step>|<index>`, and a [child instance](/workflows/child-workflows#instance-ids) uses the same shape, so a parent's ID is a prefix of everything underneath it.
