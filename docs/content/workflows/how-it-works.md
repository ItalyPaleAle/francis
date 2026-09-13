---
title: "How it works"
weight: 100
description: "The actors, the turn, the journal, and the invariants, for operators and contributors"
---

This page is for operators and contributors. Everything above it is enough to use workflows; this is what is underneath.

## The actors

Per registered workflow, the engine registers these reserved types:

| Actor | Actor type | Instances | Responsibility |
|-------|-----------|-----------|----------------|
| `Workflow` | `francis.builtin.workflow.<name>` | one per instance, actor ID is the instance ID | **Orchestrator.** Owns the journal. Decides what happens next, records what came back, and terminates. State, timers, and dispatch — nothing else. |
| Worker | `francis.builtin.workflow.<name>.worker`, plus `.worker.<cap>` per capability | one per task | Performs one attempt of one task and reports the outcome back. Every `WithRun` and every external call happens here. |
| Undo worker | `francis.builtin.workflow.<name>.undo`, plus `.undo.<cap>` | one per compensated task | The same shape, for `WithCompensate`. A separate type so compensations have their own per-host budget and cannot be starved by forward work. |
| Registry | `francis.builtin.workflow.<name>.registry` | the cluster-wide singleton | Records each version's definition fingerprint, and answers the once-per-host consistency check. |

A worker is stateless: the task arrives in its job payload and the result leaves in another job, so it **halts itself as soon as it has reported** rather than lingering to its idle timeout. That matters for a wide fan-out, which would otherwise hold one activation per task. It does keep one thing for the life of its activation — the result of the attempt it just ran — so that if reporting fails and Francis retries the report, the handler is not run again.

Splitting orchestrator from worker is what buys parallelism, since Francis places workers independently across the cluster. It is also what keeps the orchestration boundary structural rather than aspirational.

## The orchestration boundary

> The `Workflow` actor orchestrates. It never performs a step.

A `Workflow` actor holds its turn lock for the duration of a turn. While a turn runs, that instance cannot accept a result from any other worker, accept a cancellation or a suspend, handle its own deadline alarm, or serve a status read. A single blocking call on the orchestrator therefore degrades **every** property the engine is supposed to provide, and it does so exactly when things are going worst: a store that has become slow, a callback endpoint that has started hanging.

It also breaks the failure model. A step that runs on a worker gets its own attempts, its own backoff, and its own deadline, and a failure is *data* the journal records. The same work inlined on the orchestrator gets the orchestrator's retry policy, and its failure re-runs the whole turn — including the parts that already succeeded.

| On the `Workflow` actor | On a worker |
|-------------------------|-------------|
| Decode a report from a job payload | Any call to a database, object store, HTTP API, or queue |
| Mutate the in-memory journal | Any user handler (`WithRun`, `WithCompensate`) |
| Decide what comes next, as a pure function of the journal | Any CPU-bound work: encoding, rendering, compression |
| Build a job payload from the journal, in memory | Anything with a timeout of its own |
| State, alarm, and job operations | Anything whose failure should be retried independently |
| Halt, log, emit a metric | Anything that should appear as its own attempt in the journal |

The operations on the left are not free — a state write and a dispatch are database writes and can fail. The distinction is that they are the framework's own bounded, fast, retried operations, and the whole turn is retried as a unit if one of them fails. The rule is about **external and user work**, which is neither bounded nor the framework's to retry.

Two cases look like exceptions and are not:

- **Deriving a payload is orchestration; writing it is a step.** The thumbnail example builds its manifest on the orchestrator, from persisted state, and hands the finished bytes to a worker to store. Building is a pure in-memory function of the journal, and doing it there is what makes a retried write store *identical* bytes rather than re-deriving them against a journal that has since moved on.
- **The orchestrator never calls another actor on behalf of user logic.** A synchronous invocation couples this instance's turn lock to another actor's availability and queue depth. Talking to another actor is a step. The engine makes exactly one exception for itself: the once-per-host, cached definition-registry check, which is framework-owned, bounded to a few milliseconds, and happens at most once per version for the life of the process.

**How it is enforced.** The definition exposes no hook that runs on the `Workflow` actor: `WithRun` and `WithCompensate` are the only places user code appears, and both are invoked exclusively by a worker. There is no before-step hook, no orchestrator-side predicate, no expander callback — which is why a fan-out's size and a step's condition are both outputs of steps. The function that decides what happens next is pure: no I/O, no `context.Context`, no user code, and it cannot block. Size caps are checked on the worker before a report is dispatched, so the orchestrator never spends a turn serializing something unbounded. The engine's own tests drive a turn against a transport that panics on anything but state, alarm, and job operations.

## The turn

Every turn — whether triggered by the start job, a task report, an event, a cancel, a suspend or resume, the watchdog, or the deadline alarm — runs the same four phases:

1. **Read the journal.** It is the source of truth, and a terminated instance ignores everything but a purge and the unwind a parent may send.
2. **Fold the event in.** A duplicate, or a report for a task the journal already has an outcome for, records nothing. That includes this very turn being retried after its state write succeeded and its dispatch failed.
3. **Advance.** Decide what comes next, as a pure function of the journal and the definition: a completed step opens the next one, a failed attempt schedules the next, a failed step opens the unwind, a fully unwound instance terminates. This is unit-testable from a serialized journal alone, and it cannot block.
4. **Persist, then reconcile.** The journal is durable before anything is scheduled. Then everything the journal says should be running is dispatched, idempotently.

**Recovery is not a special code path.** Re-delivering any event, or firing the watchdog, re-runs advance and reconcile and converges on the same journal.

## The three ordering invariants

Everything durable here rests on three rules.

**1. Persist before dispatching.** The journal records a task as *scheduled* before the job that runs it exists. If the process dies between the two, the triggering job is retried, the turn re-runs, and reconcile dispatches it. The inverse order would allow a task to report back to a journal that does not know it exists, and that result would be dropped.

**2. A turn that records nothing still schedules.** Advance and reconcile run on every turn, including one whose event was a duplicate. This is not an optimization to skip — it is load-bearing. Consider a turn that persists a result and then fails to dispatch the next step: the job is retried, the report arrives again, and it is now a *duplicate* that records nothing. If the duplicate branch returned early, the instance would wait forever for a step nobody scheduled. The same rule covers the turn's own retry. It stays correct only because the guard is "is this outcome already recorded", never "have I seen this delivery before".

**3. The journal decides what happened; the idempotency key only prevents duplicate in-flight work.** Francis deduplicates an idempotency key against *live* rows only, so a key is reusable once its job completes **or dead-letters**. That is exactly right for the first case: the key stops reconcile from queueing a second copy of a task that is already pending or running, and the journal's outcome stops a duplicate *report* from being counted twice. The second case is why reconcile must never redispatch a scheduled-not-done task without first asking whether its previous job dead-lettered — a freed key would otherwise let a task that could not report be run again on every turn.

## The journal

One state document per instance, as a single actor state value. It records the instance's status, its input, the compensation stack, the cause of any unwind, and one record per step and task.

**Every step of the definition is recorded at `Start`**, with the ones not yet reached marked pending. That costs a few hundred bytes and makes status, the unknown-version path, and operator tooling answerable from the journal alone, without the definition on hand.

There is deliberately **no phase field**. What the instance does next is derived from the step records. A cursor naming the current step is written as a by-product, purely so status reads and log lines do not have to walk the list — nothing reads it, so if it ever disagreed with the records, the records would win. Deriving the phase rather than storing it is what makes duplicate reports safe.

### Size and write amplification

Francis state is read and written as one value per actor, so the journal has to stay small — and every report rewrites the whole document, so it has to stay small **in proportion to how often it is rewritten**. An N-task fan-out writes on the order of N² bytes: a journal that grows to 1 MiB over 500 reports has written about 250 MiB through the state store by the time it is done. That, not the cap, is the reason for the guidance.

Three caps compose as one constraint, not three:

- `WithMaxOutputSize` (16 KiB) caps a single task's output, enforced on the worker before it reports. Exceeding it fails the attempt permanently.
- `WithMaxInputSize` (64 KiB) caps the workflow input, enforced at `Start`, because the input is shipped in every task's payload.
- `WithMaxJournalSize` (1 MiB) caps the encoded journal, checked before every write. Exceeding it fails the instance, which is a much better outcome than an instance that can no longer persist and therefore can no longer progress.

**`tasks × typical output size` should stay well under the journal cap**, and comfortably under 256 KiB for anything that reports quickly. A 300-task fan-out gets under 1 KiB per task in practice; the 16 KiB cap is a ceiling against a misbehaving task, not a per-task budget. For wide fan-outs, return a handle: write the result to the object store and return its key. For fan-outs wider than a few hundred, use a [child workflow](/workflows/child-workflows) per batch, which moves the width into journals that are rewritten independently.

The flip side of rewriting the whole document is the property that buys: **a step transition is a single atomic state write**, so there is no partially-applied journal to reason about.

## Timers

The instance keeps two durable timers, chosen for different reasons.

**The deadline alarm** is recomputed every turn to the earliest of the instance timeout, the current step's `WithStepTimeout`, and a wait step's `WithEventTimeout`. It is an **alarm** because alarms are replaceable by name, so recomputing is one write — and the turn skips the write when the newly computed time is unchanged, which keeps a wide fan-out's reports from each costing a second write on the same row. Re-arming from inside its own handler is safe because Francis completes alarms by lease, not by name.

When it fires, the actor determines from the journal which deadline actually elapsed and applies it: fail the outstanding attempts of a timed-out step, or fail the instance on an instance timeout. What a timeout *costs* depends on the step it hit, which the per-step policies decide.

**The watchdog** is a repeating job the instance dispatches to itself at start and cancels at termination, on by default at a ten-minute interval. It is a **job** because a repeating job survives a failed occurrence — one that dead-letters is recorded while the recurrence continues — whereas an alarm whose handler fails its attempts is deleted, repeating or not, and the watchdog's whole purpose is to survive the conditions that make handlers fail. Ten thousand running instances cost seventeen ticks a second.

A tick does what no ordinary turn can afford to do on every report:

1. re-run advance and reconcile, which recovers a lost dispatch;
2. re-arm the deadline unconditionally, which restores one that was deleted;
3. scan the workers of every scheduled-not-done task for a dead-lettered job, and the instance's own dead-letters for a lost report;
4. re-assert the status labels, in case a state write raced a purge.

The `Workflow` type itself is registered with 20 attempts and a 5-second initial retry delay, far above the framework defaults. Its turns are idempotent, so retrying is always safe, and a generous policy is what keeps a ten-second database blip from dead-lettering a report or deleting a deadline in the first place. The watchdog is the backstop for the cases that still get through.

## Attempts and dead-letters

The engine owns retries. Francis' own retry mechanism is a property of the actor **type**, not of the job, so it cannot express "five attempts for this step, twenty for its compensation" — and a Francis retry is invisible to the journal. So the worker never lets a handler error reach Francis:

1. **Success** — the worker reports the output and halts.
2. **Retryable failure** — the worker reports the error as retryable and halts. The orchestrator records the attempt and, if the policy allows another, schedules it after the step's backoff.
3. **Permanent failure** — the handler returned `actor.ErrJobPermanentFailure`. The task is failed without further attempts.
4. **Rejection** — the handler returned `actor.ErrJobRejected`. The worker returns it to Francis, which re-routes the occurrence to another host without counting an attempt.

Each attempt costs a round trip through the orchestrator — one report turn and one dispatch — where a Francis-level retry would have re-run the handler in place. That is the price of attempts being durable, per-step, observable, and independent of the actor type's configuration, and it is a price only failing tasks pay.

A worker's `Job` handler returns an error to Francis in exactly one situation: **the report dispatch itself failed**. Francis retries the job in place; the worker still holds the result of the attempt in memory, so the retried job re-sends the report rather than re-running the handler. The worker types are registered with 5 attempts to cover this case only.

**Dead-letters.** A worker's job dead-letters only when it could not report — five failed report dispatches, or a host that died mid-attempt more than five times. Two mechanisms cover it:

- The worker's dead-letter hook reports the failure to the orchestrator, so in the common case it learns within a turn. The hook is best-effort, so it is the fast path, not the guarantee.
- **The watchdog scans.** For every scheduled-not-done task it lists the worker's jobs; a dead-lettered run or compensate is folded into the journal as one failed attempt of transport kind and its record removed, and reconcile dispatches the next attempt under its own key in the same turn. The same dead job seen twice is not counted twice. It also lists the instance's own jobs and **retries** any dead-lettered report or event it finds — those are re-dispatched rather than re-derived, because nothing else can reconstruct a lost report.

The invariant that falls out: **no dead-letter record outlives the journal entry that accounts for it.** By the time an instance terminates, every dead-letter it produced has been retried or deleted, which is what lets `Purge` be a state delete plus a bounded sweep rather than a search.

## Job methods and idempotency keys

| Method | Target | Dispatched by | Idempotency key |
|--------|--------|---------------|-----------------|
| `start` | `Workflow` | `Service.Start`, or a parent | `start` |
| `done` | `Workflow` | worker, or a child instance | `done\|<step>\|<index>\|<attempt>` |
| `compensated` | `Workflow` | undo worker, or a child instance | `comp\|<step>\|<index>\|<attempt>` |
| `event` | `Workflow` | `Service.RaiseEvent` | `event\|<name>` |
| `cancel` | `Workflow` | `Service.Cancel`, or a parent | `cancel`, or `cancel\|<attempt>` from a parent |
| `unwind` | `Workflow` (a completed child) | a parent instance | `unwind\|<attempt>` |
| `suspend` / `resume` | `Workflow` | `Service.Suspend` / `Resume` | `suspend` / `resume` |
| `tick` | `Workflow` | itself, as a repeating job | `watchdog` |
| `run` | worker | `Workflow` | `run\|<attempt>` |
| `compensate` | undo worker | `Workflow` | `compensate\|<attempt>` |

Report keys carry the attempt number so a late report from attempt 1 can never be mistaken for attempt 2's. One report method keyed by step, index, and attempt is enough for every kind of step.

A worker's actor ID is deterministic — `<instanceID>|<step>|<index>` — so a re-run of reconcile addresses the same actor and its idempotency key applies. `|` is the delimiter, so step names are rejected at definition time if they contain it, and instance IDs are rejected at `Start`. A hash of the three components would avoid constraining names at the cost of unreadable actor IDs in logs and traces; readability wins, since these are what an operator greps for.

## Listing

`List` is built on **state labels**: the orchestrator writes `status`, `version`, `parent`, and `terminatedAt` with every journal write, in the same operation as the state, so the index can never disagree with the journal. The labels are matched by equality on an indexed column, which makes a filtered listing a range scan rather than a walk of every retained journal.

A secondary index actor would have needed no framework change, but it would have been a second write on a second actor, updated *after* the journal write and therefore able to lag or dangle. Labels are also useful to every actor application, not only this one.
