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

**`francis.workflow.turn.duration`** is a histogram of how long a `Workflow` turn took. It should sit in **single-digit milliseconds**, because a turn only reads and writes the journal, decides what comes next, and dispatches jobs. A regression is the signal that something has been inlined onto the orchestrator that should be a step — which is the one failure this design is most concerned with, since a blocking call on the orchestrator degrades every property the engine is supposed to provide, exactly when things are going worst.

**`francis.workflow.turns.duplicate_events`** counts turns that re-applied an event the journal already reflected. That is the direct measure of how often the second ordering invariant is doing its job: a turn that persisted a result and then failed to dispatch is recovered through the duplicate report. A non-zero, low rate is healthy. A rate that climbs means dispatches are failing.

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

`task.transport_failures` is worth separating from ordinary attempt failures: it counts attempts that failed because the **report could not be delivered**, not because the handler failed. A task that ran and could not say so is a different problem from a task that ran and failed — it means the work may well have happened and is about to happen again.

## What to alert on

- `instances.terminated{status="failed"}` climbing, obviously.
- A terminal `compensation: partial` or `compensation: failed` outcome. These are the "money may be stranded" cases, and the reason the compensation outcome is carried separately from the status rather than folded into it. They are visible in `GetStatus` and, per workflow, through the `compensations.failed` counter.
- `definition.conflicts` above zero, at any rate: two hosts are serving different graphs under one version number.
- `turn.duration` regressing past a few milliseconds.

## Tracing

A workflow instance is a long-lived, multi-host activity, so it cannot be one span. A Francis job does not carry its dispatcher's trace context either — only transport hops propagate it — so the engine carries it in the payloads it controls:

- `Start` records the caller's trace context in the journal, and every `start`, `run`, `compensate`, and report payload the engine builds carries the context of the turn or attempt that dispatched it.
- There is **one span per `Workflow` turn** and **one per attempt**, tagged with instance ID, workflow, version, step, index, and attempt, each a child of the span in its payload and **linked** to the instance's recorded trace context. So a trace can be followed either from the caller's original request or from any single turn.
- A child instance's spans link to the parent's trace context as well as their own.

## Logs

`WithLogger` gets you instance and task lifecycle events, every line tagged with the instance ID and, for a task, the step, index, and attempt. Worker actor IDs are readable on purpose — `<instanceID>|<step>|<index>` — because those are what an operator greps for.

The journal itself is a document you can read. Its shape is in [How it works](/workflows/how-it-works#the-journal).
