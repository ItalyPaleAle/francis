# 10. History, listing, and retention

The journal says where an instance stands. Three other mechanisms answer the questions around it: the event history says how it got there, the workflow labels make instances findable, and purge and retention decide when it all goes away. This document also lists the engine's metrics.

## The event history

Every instance has an append-only timeline of history events, unless its definition opted out with `WithoutEventHistory`. The events live in a separate store, not in the journal, so a wide fan-out does not fill the size-capped journal merely to support a timeline. They are written in the same `SetState` operation as the journal (`SetStateOpts.SetAppendEvents`), so a journal write and the events that describe it are committed together or not at all.

Each stored event is a `components.WorkflowEvent` with a sequence number, a time, a kind, and a MessagePack body (`eventData`) holding the step, task index, attempt, iteration, outcome, error, reason, event name, due time, and child link that apply. Event bodies never carry inputs or outputs. `DecodeEvent` in `management.go` decodes them for the management API and the dashboard.

### Events are derived from journal diffs

Turns do not emit events imperatively. `historyEntries` compares the journal about to be written with the committed one, and derives the events from what changed: a step that went from `pending` to `running` produced `step_started`, a task whose `DispatchedAttempt` advanced produced `task_dispatched`, and so on. Three kinds of information that the diff cannot see are added from elsewhere:

| Source | Contributes |
| --- | --- |
| `turnHistory`, built from the turn's event | The worker's own start and finish times of the attempt a report describes, and the reason of a cancel or an unwind request |
| `instanceState.stepHistory` | Step settlements a turn overwrote before writing, such as the iteration of a body step a loop rewound, or the settlement of a step whose compensation frame opened in the same turn, so the timeline shows both transitions |
| The activation's held dispatch markers | When each job was accepted, which becomes the dispatch event's time |

Deriving events from the diff keeps them consistent with the journal by construction, and makes them idempotent: the sequence numbers continue from the committed journal's `LastEventSeq`, so a turn retried after a failed write recomputes exactly the same events with exactly the same numbers. The first journal write of an instance starts numbering at 1, which is also how the provider drops the events of an earlier instance that used the same ID.

Within one write, events are ordered so they read causally: instance-level changes that precede steps (started, the request, reopened, resumed), then each step's forward transitions and its tasks' dispatches, worker activity, retries, and outcomes, then `compensation_started`, then each step's compensation transitions, then any worker events for tasks the journal no longer lists, and finally instance-level changes that summarize the steps (suspended, and the terminal event). `parent_notified` is written by `markReported`, together with the flag that says the report was sent.

### Time sources

Each event says how its time was obtained, because the times come from different clocks:

| Time source | The event's time is |
| --- | --- |
| `engine` | The time of the orchestrator turn that recorded it |
| `dispatch` | When the orchestrator's dispatch of the job was accepted |
| `report` | The time of the turn that processed the task report |
| `worker` | The worker's own clock, sent with the report, for `worker_started` and `worker_finished` |

### Event kinds

| Group | Kinds |
| --- | --- |
| Instance | `instance_started`, `instance_suspended`, `instance_resumed`, `instance_reopened`, `instance_completed`, `instance_failed`, `instance_cancelled` |
| Requests | `cancel_requested`, `unwind_requested`, `compensation_started`, `parent_notified` |
| Steps | `step_started`, `step_completed`, `step_failed`, `step_skipped`, `wait_started`, `event_received`, `step_compensating`, `step_compensated`, `step_compensation_failed` |
| Tasks | `task_dispatched`, `child_started`, `worker_started`, `worker_finished`, `task_retry_scheduled`, `task_completed`, `task_failed`, `task_abandoned` |
| Compensations | `compensation_dispatched`, `child_unwind_requested`, `compensation_retry_scheduled`, `compensation_completed`, `compensation_failed` |

## Listing instances

Every journal write also writes the instance's workflow labels: its status, version, parent instance ID, and creation time. The provider indexes them, so `List` filters with an equality on an indexed column rather than walking every retained journal: every running instance, every instance of version 2, or every child of one parent. The pending placeholder carries the same labels, so pending instances, including children whose start has not run, are listed too.

Pages are ordered by instance ID. Generated IDs are UUIDv7, which sort by creation time, so the default listing is in creation order. A filter on a range, such as "completed more than a day ago", is not something the labels can express, so the purge sweep filters on status with labels and then checks `CompletedAt` on each decoded journal.

## Retention and purge

### Purging one instance

`Purge` invokes the orchestrator under its exclusive lock, so removal is serialized with the instance's own turns. It removes, in this order:

1. **Its children, recursively and concurrently**, at most 16 at a time. Each child is told which parent is purging it, so it can skip re-checking its ancestry.
2. **Its jobs**: the orchestrator's own, then the forward and undo jobs of every task, live or retained, found through the actor types the journal recorded for each task.
3. **Its journal.**

That order is what makes an interrupted purge safe to repeat: the journal that names the children and task actors is the last thing to go. `Purge` refuses an instance that is not terminal with `ErrInstanceActive`, and also refuses a terminated child while any of its ancestors is still running, by walking the ancestors' journals, because a running ancestor may still ask it to undo itself.

### Automatic cleanup

| Mechanism | What it removes | When |
| --- | --- | --- |
| The auto-purge cron job `<name>.purge` | Every terminated instance past its retention, via `PurgeTerminated`, which pages through each terminal status 100 instances at a time | Every 12 hours by default, or per `WithAutoPurgeInterval` or `WithAutoPurgeCron` |
| The journal TTL | A terminated top-level journal, at twice its retention | If the sweep never runs, as a backstop |
| Job retention | The engine's completed and dead-lettered job records, at twice the longest retention | With the instance's own records |

A terminated child's journal has no TTL, since an expiry its parent cannot see would take the journal out from under a parent that may still unwind it. The parent's purge removes it, and the sweep removes orphaned children once their parent's journal is gone.

## Metrics

The engine records these OpenTelemetry instruments on the meter set with `WithMeter`, and on a no-op meter otherwise.

| Instrument | Kind | Meaning |
| --- | --- | --- |
| `francis.workflow.instances.started` | Counter | Instances whose start turn ran |
| `francis.workflow.instances.running` | Up-down counter | Instances that are active, including reopened ones |
| `francis.workflow.instances.terminated` | Counter | Terminations by status, counting a reopened instance once |
| `francis.workflow.instances.suspended` | Counter | Suspensions |
| `francis.workflow.instances.purged` | Counter | Purged instances |
| `francis.workflow.instance.duration` | Histogram | From start to termination |
| `francis.workflow.step.duration` | Histogram | From a step opening to it settling, by outcome |
| `francis.workflow.task.attempts` | Counter | Attempts run by workers, by outcome |
| `francis.workflow.task.transport_failures` | Counter | Orchestrator jobs that were dead-lettered |
| `francis.workflow.compensations.run` | Counter | Compensation attempts dispatched |
| `francis.workflow.compensations.failed` | Counter | Frames that failed to compensate |
| `francis.workflow.children.started` | Counter | Child starts dispatched |
| `francis.workflow.turn.duration` | Histogram | Time spent in a turn |
| `francis.workflow.turns.duplicate_events` | Counter | Turns whose event changed nothing, which is journal-based deduplication at work |
| `francis.workflow.definition.conflicts` | Counter | Times a host found its definition disagreeing with the registry for its version |
