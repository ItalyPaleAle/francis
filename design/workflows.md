# Design: generic workflows as a built-in actor

- **Status**: draft, for discussion
- **Package**: `builtin/workflow` (new)
- **Reserved actor types**: `francis.builtin.workflow.<name>`, `francis.builtin.workflow.<name>.task`
- **Prior art in the wild**: [Pixel's `imageoptim` service](https://github.com/ItalyPaleAle/pixel/tree/devel/v1/services/imageoptim/pkg/actors), which hand-rolls this pattern on top of Francis actors, jobs, and alarms

## 1. Summary

Francis already has every primitive a durable workflow engine needs: single-activation actors with turn-based concurrency, durable per-actor state, durable jobs with retries and dead-lettering, named replaceable alarms, and placement that spreads actors across a cluster. What it does not have is the *pattern* that assembles them, so every application that wants "run these steps, in this order, some of them in parallel, and undo them if something fails" writes the same orchestrator by hand.

This document generalizes the pattern that `imageoptim` proved out into a **built-in actor** that runs arbitrary workflows: sequences of steps, parallel groups, dynamic fan-out, and **compensations** that roll back what already succeeded.

The design deliberately does **not** introduce an SDK that abstracts the underlying actors the way Dapr Workflow does. There is no code-as-workflow, no replay, no determinism requirement, and no hidden control flow. A workflow is a **declared graph of named steps** plus plain Go handler functions, and the engine is a state machine over a durable journal. Everything the engine does is expressible in terms of the public Francis API, and everything a user writes is an ordinary Go function.

## 2. Motivation

### 2.1 What `imageoptim` does today

`imageoptim` turns one uploaded image into N thumbnails. Its actors implement a workflow by hand:

```text
POST /v1/thumbnail
      │  job "start"
      ▼
imageworkflow/<workflowID>                    ← orchestrator, one per image, holds the durable journal
      │  job "generate", one per thumbnail
      ├──────────► thumbnailgen/<workflowID>-0  ┐
      ├──────────► thumbnailgen/<workflowID>-1  │  each encodes its thumbnail and writes it to the store
      └──────────► thumbnailgen/<workflowID>-2  ┘
      ◄────────── job "thumbnail-done" ─────────┘
      │
      ▼
  all reported → write manifest → mark completed → halt
```

The properties that make it work are all general, and none of them are about images:

1. **Executable steps are durable jobs; the deadline is an alarm.** A job is dispatched per unit of work, is retried, and is dead-lettered rather than dropped. The deadline is an alarm because it must be replaceable and cancellable by name.
2. **The orchestrator's persisted state is the source of truth.** Scheduling is derived from it, so a step that was scheduled but never dispatched can be re-derived.
3. **Every step is idempotent**, because jobs and alarms are delivered at least once: a repeated `start` re-schedules only what has not reported, a repeated result is ignored, a re-run thumbnail overwrites the same object.
4. **One actor per unit of work** is what buys parallelism: Francis places them across hosts and bounds them with `WithConcurrencyLimit`.
5. **Failure has two paths**: a retryable error returns and is retried by the job engine; a permanent one reports a failed result immediately, and the dead-letter hook (`ActorJobFailed`) covers the case where retries are exhausted.

### 2.2 Why generalize it

Everything above is boilerplate that any durable multi-step process needs, and it is subtle boilerplate: the ordering of "persist, then dispatch", the idempotency keys, the dead-letter hook, the timeout that guarantees termination. Getting one of them wrong produces a workflow that stalls forever or double-charges a credit card.

Two things `imageoptim` did **not** need, and which a general engine must have:

- **Sequencing.** `imageoptim` has exactly one fan-out and no ordered steps. Most workflows are "do A, then B, then C in parallel, then D".
- **Compensation.** `imageoptim` writes to an object store, where a partial result is harmless and a manifest records what failed. A workflow that charges a card, reserves stock, and books a courier must be able to unwind.

## 3. Goals and non-goals

### Goals

- Sequential steps, static parallel groups, and dynamic fan-out over a runtime-sized list.
- Per-step **compensation callbacks**, run in reverse order when the workflow fails or is cancelled.
- Durable and resumable across restarts, host loss, and rebalancing, with at-least-once execution.
- Steps run **anywhere in the cluster**, with per-host concurrency bounds and optional capability requirements.
- Observable: status query, listing, metrics, traces, and a journal an operator can read.
- Registered and driven exactly like the other built-in actors (`taskpool`, `cronjob`, `signal`, `ratelimit`).
- No user code ever runs on the orchestrator's turn.

### Non-goals

- **No code-as-workflow SDK.** There is no `ctx.CallActivity(...).Await()`, no replay of a Go function, and therefore no determinism constraints on user code. This is the explicit design choice that separates this from Dapr Workflow and Durable Task.
- **No arbitrary control flow.** No `goto`, no unbounded loops, no dynamic graph rewriting. Conditional skipping is supported; anything more expressive belongs in a child workflow or a fresh instance.
- **Not a queue and not a saga coordinator across clusters.** A workflow instance is an actor: it lives in one cluster and one database.
- **No exactly-once.** At-least-once with idempotent handlers, like everything else in Francis.

## 4. Relationship to Dapr Workflow

Dapr Workflow (and the Durable Task Framework it is built on) expresses a workflow as a Go/C#/Java function that calls activities, and makes it durable by **replaying** that function from an event history on every resumption. The function must therefore be deterministic: no clocks, no random numbers, no I/O, no map iteration order.

That is a powerful model, and it is also the source of nearly all of its sharp edges: the determinism rules, the versioning problem when the code changes under a running instance, the difficulty of reasoning about what the runtime is doing, and the size of the SDK required to hide it.

This design keeps the parts of Dapr's model that are unambiguously good and drops the replay:

| | Dapr Workflow | This design |
|---|---|---|
| Workflow shape | imperative function, replayed | declared graph of named steps |
| Determinism required | yes, strictly | no |
| History used for | replaying the orchestrator function | driving a state machine, and audit |
| Activity dispatch | queue + work items | Francis durable job per task |
| Orchestration state | event-sourced history | one journal document per instance |
| Compensation | hand-written `defer`/`try` inside the orchestrator | declared per step, engine-driven |
| Dynamic fan-out | `for` loop over `CallActivity` | `ForEach` step over a list from an upstream step |
| Conditionals, loops | arbitrary Go | conditions only; loops out of scope |

The cost of dropping replay is expressiveness: a graph cannot say "retry the whole subworkflow with a different parameter until it works". The benefit is that the engine is a few hundred lines of state machine with no hidden rules, the journal is directly readable, and a user's handler is just a function that can do whatever it likes — including calling `time.Now()`.

## 5. Model

### 5.1 Vocabulary

- **Definition** — a named, versioned graph of steps, registered on a host at startup, together with the Go functions that implement them. Registered identically on every host that should run the workflow's steps.
- **Instance** — one execution of a definition, identified by an **instance ID**. One orchestrator actor instance per workflow instance.
- **Step** — a named node in the definition. A step is one of the kinds below.
- **Task** — one execution unit of a step. A plain step has one task; a parallel group has one per member; a fan-out has one per item. A task is one durable job delivered to one task actor.
- **Journal** — the orchestrator's durable state: the instance's status, its input, and one record per step and task. It is the single source of truth.
- **Compensation** — a per-step callback that undoes the effect of a task that completed successfully.

### 5.2 Step kinds

| Kind | Constructor | Tasks | Notes |
|---|---|---|---|
| Plain | `workflow.Step(name, opts...)` | 1 | The common case |
| Parallel group | `workflow.Parallel(name, steps...)` | one per member | Members are plain steps; they run concurrently |
| Fan-out | `workflow.ForEach(name, opts...)` | one per item, sized at runtime | Items come from an upstream step's output |
| Wait for event | `workflow.WaitForEvent(name, opts...)` | 0 | Parks the instance until `RaiseEvent` or a deadline |

Steps are addressed **by name**, never by position, which is what makes the journal survive a definition change (§12).

### 5.3 Data flow between steps

Each task receives, in its job payload:

- the **workflow input**, as given to `Start`;
- the **output of the immediately preceding step**;
- the outputs of any steps named with `WithInputFrom("a", "b")`;
- for a fan-out task, its **item**.

The engine never ships the whole journal to a task. This keeps the payload bounded and makes the data dependencies of a step explicit and auditable from the definition alone.

A task returns `(any, error)`. The output is JSON-encoded into the journal, subject to `WithMaxOutputSize` (default 64 KiB per task, mirroring `signal`'s payload cap). A fan-out step's output, seen by later steps, is the array of its tasks' outputs, ordered by item index.

Outputs are for **control flow and small results**, not for payloads. The guidance is the same as for actor state: keep large blobs in an object store and put a reference in the output.

## 6. Public API

### 6.1 Defining and registering

```go
import "github.com/italypaleale/francis/builtin/workflow"

wf, err := workflow.New("order-fulfillment",
    workflow.WithVersion(3),
    workflow.WithTimeout(30*time.Minute),
    workflow.WithRetention(24*time.Hour),
    workflow.WithConcurrency(4),
    workflow.WithLogger(log),

    workflow.WithSteps(
        // A plain step, with the compensation that undoes it
        workflow.Step("charge-card",
            workflow.WithRun(chargeCard),
            workflow.WithCompensate(refundCard),
            workflow.WithMaxAttempts(5),
            workflow.WithStepTimeout(30*time.Second),
        ),

        workflow.Step("reserve-stock",
            workflow.WithRun(reserveStock),
            workflow.WithCompensate(releaseStock),
        ),

        // A human (or another system) has to approve before the order ships
        workflow.WaitForEvent("approval",
            workflow.WithEventTimeout(48*time.Hour),
        ),

        // Two independent notifications, run at the same time
        workflow.Parallel("notify",
            workflow.Step("email", workflow.WithRun(sendEmail)),
            workflow.Step("sms", workflow.WithRun(sendSMS)),
        ),

        // One task per element of the "plan-shipments" output, run concurrently
        workflow.ForEach("ship",
            workflow.WithItemsFrom("plan-shipments"),
            workflow.WithRun(bookCourier),
            workflow.WithCompensate(cancelCourier),
            workflow.WithMaxParallel(8),
            workflow.WithFailurePolicy(workflow.CollectFailures),
            workflow.WithRequiredCapability("eu-region"),
        ),
    ),
)
if err != nil {
    return err
}

// Register before the host starts, on every host that should run this workflow's steps
err = host.RegisterBuiltInActor(wf)
```

`New` follows the conventions of the other built-ins exactly: a unique name validated with `ref.ValidateComponents`, functional options, a single returned value registered with `RegisterBuiltInActor`, and a `Service` method that binds it to an `actor.Service`.

### 6.2 Handler contract

```go
// RunFunc executes one task of a step and returns the output recorded in the journal
// Returning an error retries the task; returning actor.ErrJobPermanentFailure fails it immediately; returning actor.ErrJobRejected declines it so another host runs it
type RunFunc func(ctx context.Context, t Task) (output any, err error)

// CompensateFunc undoes the effect of one task that had completed successfully
type CompensateFunc func(ctx context.Context, c Compensation) error

type Task interface {
    // Identity of the task, which is also what every log line and span is tagged with
    InstanceID() string
    Workflow() string
    Step() string
    // Index is the position within a parallel group or fan-out, and -1 for a plain step
    Index() int
    // Attempt is 1 on the first execution and increases with each retry
    Attempt() int

    // DecodeInput reads the workflow input, as given to Start
    DecodeInput(into any) error
    // DecodeItem reads this task's fan-out item, and is a no-op for a step that is not a fan-out
    DecodeItem(into any) error
    // DecodeOutput reads the output of an upstream step, which must be the preceding step or one named with WithInputFrom
    DecodeOutput(step string, into any) error
}

type Compensation interface {
    Task
    // DecodeResult reads the output this task produced when it succeeded, which is usually what identifies the effect to undo
    DecodeResult(into any) error
    // Cause is the error that caused the workflow to unwind, or the cancellation reason
    Cause() string
}
```

A handler is a plain function. It may call the clock, do I/O, use randomness, and start goroutines. The only contract is **idempotency**, because at-least-once delivery means it can run twice.

### 6.3 Driving workflows

```go
svc := wf.Service(host.Service())

// Start an instance
// Without WithInstanceID, the engine mints a UUIDv7, which sorts by creation time
id, err := svc.Start(ctx, OrderInput{OrderID: "A-91", Total: 4999})

// Use a natural key to make starting idempotent: a second Start with the same ID is a no-op
id, err = svc.Start(ctx, input, workflow.WithInstanceID("order-A-91"))

// Read the current status without taking the orchestrator's exclusive turn
status, err := svc.GetStatus(ctx, id)

// List instances, paginated, built on Service.ListStates
page, err := svc.List(ctx, &workflow.ListOptions{Status: workflow.StatusRunning, Limit: 50})

// Deliver an external event to a WaitForEvent step
err = svc.RaiseEvent(ctx, id, "approval", ApprovalPayload{By: "ops"})

// Ask a running instance to stop and unwind
err = svc.Cancel(ctx, id, "customer cancelled the order")

// Drop the journal of a terminated instance before its retention elapses
err = svc.Purge(ctx, id)
```

`Start` returns as soon as the start job is durable, which is the same guarantee `imageoptim`'s handler gives its callers: from that point on the work survives a restart of the process.

## 7. Execution model

### 7.1 Actors

Two reserved actor types per registered workflow:

- **`francis.builtin.workflow.<name>`** — the orchestrator. One instance per workflow instance, whose actor ID is the instance ID. It owns the journal and does nothing but read it, write it, and schedule.
- **`francis.builtin.workflow.<name>.task`** — the task executor. One instance per task. Stateless: the task arrives in its job payload and the result leaves in another job. It halts itself when done.

Splitting them is not cosmetic. It follows the same reasoning as `cronjob`'s scheduler/runner split: a task can run for minutes and would hold the turn lock for its whole duration, so it must not run on the actor that also has to accept results, timeouts, cancellations, and status reads. It is also what buys parallelism, since Francis places task actors independently across the cluster.

**No user code runs on the orchestrator.** The orchestrator only decodes the journal, mutates it, and dispatches jobs. A user handler that hangs can never wedge an instance's control plane, and the orchestrator's turns stay in the millisecond range.

### 7.2 The orchestrator turn

Every orchestrator turn — whether triggered by the start job, a task result, an event, a cancellation, or the deadline alarm — runs the same four phases:

```go
func (o *orchestrator) turn(ctx context.Context, ev event) error {
    // The journal is the source of truth, and a terminated instance ignores everything
    st, err := o.client.GetState(ctx)
    if err != nil {
        return err
    }
    if st.Status.IsTerminal() {
        return nil
    }

    // Fold the event into the journal
    // Duplicates and results for unknown tasks are dropped here, which is what makes at-least-once delivery safe
    apply(&st, ev)

    // Advance the cursor as far as the journal allows, which is a pure function of the journal and the definition
    // This is where a completed step opens the next one, a failed step opens the unwind, and a fully unwound instance terminates
    advance(&st, o.def)

    // The journal is durable before anything is scheduled, so a lost dispatch is always recoverable and an orphan result never is
    err = o.client.SetState(ctx, st, o.stateOpts(st))
    if err != nil {
        return err
    }

    // Everything the journal says should be running is dispatched, idempotently
    // A dispatch that already exists is deduplicated by its key, so this is safe to run on every turn
    return o.reconcile(ctx, st)
}
```

`advance` is pure and total: given the journal and the definition, it produces the next journal. It never performs I/O and never runs user code, so it is trivially unit-testable, and a bug in it can be reproduced from a serialized journal alone.

`reconcile` derives the set of tasks that should be in flight from the journal and dispatches each one with a stable idempotency key. It is the *only* thing that schedules work, and it is safe to run at any time.

**Recovery is not a special code path.** Re-delivering any event, or firing the watchdog, re-runs `advance` + `reconcile` and converges on the same journal.

### 7.3 The two ordering invariants

Everything durable in this design rests on two rules:

1. **Persist before dispatching.** The journal records a task as *scheduled* before the job that runs it exists. If the process dies between the two, the triggering job is retried (jobs complete only after the handler returns), the turn re-runs, and `reconcile` dispatches it. The inverse order would allow a task to report back to a journal that does not know it exists, and that result would be dropped.
2. **The journal decides what happened; the idempotency key only prevents duplicate in-flight work.** Francis maps an idempotency key to the job's name and deduplicates against *live* rows, so a key is reusable once its job completes. That is exactly right here: the key stops `reconcile` from queueing a second copy of a task that is already pending or running, and the journal's `Done` flag stops a duplicate *result* from being counted twice — as `imageoptim` does when it ignores a repeated `thumbnail-done`.

### 7.4 Message flow

```mermaid
sequenceDiagram
    participant C as Caller
    participant O as orchestrator<br/>(one per instance)
    participant T as task actors<br/>(one per task)

    C->>O: job "start" (input)
    Note over O: journal: running, step 1 scheduled
    O->>T: job "run" (task 1)
    T-->>O: job "done" (output)
    Note over O: journal: step 1 completed,<br/>fan-out step 2 materialized
    O->>T: job "run" (task 2.0)
    O->>T: job "run" (task 2.1)
    T-->>O: job "done" (2.0)
    T-->>O: job "done" (2.1, error)
    Note over O: policy = fail fast →<br/>status: compensating
    O->>T: job "compensate" (task 1)
    T-->>O: job "compensated" (task 1)
    Note over O: journal: failed / compensated,<br/>state TTL set, actor halts
```

### 7.5 Job methods

| Method | Target | Dispatched by | Idempotency key |
|---|---|---|---|
| `start` | orchestrator | `Service.Start` | `start` |
| `done` | orchestrator | task actor | `done\|<step>\|<index>` |
| `compensated` | orchestrator | task actor | `comp\|<step>\|<index>` |
| `event` | orchestrator | `Service.RaiseEvent` | `event\|<name>` |
| `cancel` | orchestrator | `Service.Cancel` | `cancel` |
| `run` | task actor | orchestrator | `run` |
| `compensate` | task actor | orchestrator | `compensate` |

The task actor's keys are constant because each task has its own actor, so the key only has to be unique within it.

### 7.6 Task actor IDs

A task actor's ID must be **deterministic**, so that a re-run of `reconcile` addresses the same actor and its idempotency key applies:

```text
<instanceID>|<step>|<index>
```

`|` is the delimiter, so step names are rejected at definition time if they contain it, and instance IDs are rejected at `Start` if they do. (Francis itself only reserves `/`.) A hash of the three components would avoid constraining names at the cost of unreadable actor IDs in logs and traces; readability wins, since these IDs are what an operator greps for.

### 7.7 Alarms: exactly one per instance

The orchestrator keeps **one** alarm, named `deadline`, recomputed on every turn to the earliest of:

- the instance timeout from `WithTimeout`,
- the current step's timeout from `WithStepTimeout`,
- a `WaitForEvent` step's `WithEventTimeout`,
- the next watchdog tick, if `WithWatchdog` is set.

Alarms are named and replaceable, so recomputing is a single `SetAlarm`, and the alarm is deleted when the instance terminates. This is a deliberate improvement on `imageoptim`, which sets one `timeout` alarm and would need a second one per concept: one alarm per instance keeps the alarm table proportional to the number of *running instances*, not to the number of running steps.

When it fires, the orchestrator determines from the journal which deadline actually elapsed and applies it: fail the outstanding tasks of a timed-out step, fail the instance on an instance timeout, or simply re-run `advance` + `reconcile` for a watchdog tick.

`WithWatchdog(d)` is **off by default**. The dispatch path is already recoverable through job retries, and a repeating alarm per running instance is a real cost at scale (10,000 running instances on a one-minute watchdog is ~167 alarm executions per second). Enabling it is the right call for long-running workflows where a stall would otherwise go unnoticed until the instance timeout.

### 7.8 Failure of a task

A task has three ways to end, mirroring what `imageoptim`'s `thumbnailgen` does:

1. **Success** — it dispatches `done` with its output and halts.
2. **Retryable failure** — it returns the error, the job engine retries it with backoff up to `WithMaxAttempts`.
3. **Permanent failure** — it returns `actor.ErrJobPermanentFailure` (or exhausts its retries) and the job is dead-lettered. The task actor's `JobFailed` hook then dispatches `done` carrying the error, so the orchestrator learns immediately instead of waiting for a deadline.

The `JobFailed` hook is best-effort, so the `deadline` alarm remains the backstop, exactly as it is today in `imageoptim`.

A handler can also return `actor.ErrJobRejected` to decline a task on this host without counting an attempt, which re-routes it. This is the same escape hatch `taskpool` exposes through `WithAccept`, and it is how a host says "not me" for reasons a static capability cannot express.

## 8. Parallelism

### 8.1 Static parallel groups

`workflow.Parallel(name, steps...)` materializes one task per member. The group completes when every member has reported. Members are independent: they receive the same upstream outputs and cannot read each other's.

### 8.2 Dynamic fan-out

`workflow.ForEach(name, workflow.WithItemsFrom("plan"), ...)` materializes one task per element of the named step's output, which must decode to a JSON array. The size is decided at runtime, when the upstream step reports, and is then **journaled**: a retried turn re-reads the recorded items rather than re-deriving them.

Deriving the list is a normal step that runs on a task actor. That is a consequence of "no user code runs on the orchestrator": an expander callback invoked on the orchestrator's turn would be simpler to write but would put an arbitrary user function on the instance's control plane. The cost is one extra durable round-trip, and the benefit is that the expansion is itself retried, dead-lettered, traced, and recorded like any other step.

`WithMaxParallel(n)` bounds how many of a fan-out's tasks are in flight **per instance**: `reconcile` dispatches at most `n` at a time and releases the next as results arrive. It is orthogonal to the per-host bound in §8.4, which limits how much work a host accepts across all instances.

### 8.3 Failure policies

A group or fan-out chooses what a failing member means:

| Policy | Behavior |
|---|---|
| `workflow.FailFast` (default) | The first failure fails the step. Pending tasks of the group are cancelled with `CancelJob`; tasks already running are allowed to finish and their results are recorded (and compensated, if they succeeded). |
| `workflow.CollectFailures` | Every task runs to completion, then the step fails if any of them failed. Use when the tasks are independent and partial progress is worth having before unwinding. |
| `workflow.TolerateFailures` | Every task runs to completion and the step succeeds regardless. Failures are visible in the step's output, and it is the next step's business what to do about them. |

`TolerateFailures` is exactly `imageoptim`'s semantics: a thumbnail that cannot be encoded is recorded as failed in the manifest and does not fail the workflow.

Note that `CancelJob` removes a pending job but does not interrupt an occurrence that is already executing; a task that wants to stop early must observe its context.

### 8.4 Placement, capacity, and capabilities

Task actor types are registered with the same mechanics `taskpool` uses:

- `WithConcurrency(n)` puts every task type of the workflow into one **capacity group** with a strict, in-process per-host budget, and mirrors it as the cluster-wide `ConcurrencyLimit` placement hint so hosts are rarely handed more than they can run.
- `WithRequiredCapability(cap)` on a step routes its tasks to a per-capability actor type that only hosts advertising the capability register (`WithCapability(cap)` at the host's `New`). A step with no requirement runs anywhere.

This makes "the OCR step only runs on hosts with a GPU" a one-line property of the definition, and it means a workflow's throughput scales by adding hosts, with no change to the definition.

## 9. Compensation

### 9.1 Model

Compensation is a **stack**. Every task that completes successfully and whose step declares `WithCompensate` is pushed onto the journal's compensation stack in completion order. When the instance has to unwind, the stack is popped in reverse.

```text
forward:      charge-card ──► reserve-stock ──► ship[0] ship[1] ship[2] ──► ✗ confirm
                                                (parallel)

unwind:       refund-card ◄── release-stock ◄── cancel-courier ×3
                                                (parallel, in one frame)
```

Ordering rules:

- **Frames unwind in reverse order.** A step that ran after another is compensated before it, which is the invariant a saga depends on.
- **Within a frame, compensations run concurrently.** The tasks of a parallel group or fan-out had no order between them going forward, so imposing one on the way back would only make unwinding slower.
- **A frame is fully compensated before the next one starts.** This is what makes the reverse order meaningful, and it is why compensation is driven by the same `advance` + `reconcile` loop rather than by dispatching everything at once.

### 9.2 What triggers an unwind

- A step fails terminally, under a policy that makes that a step failure.
- `Service.Cancel` is called on a running instance.
- The instance timeout elapses.
- A `WaitForEvent` step's own timeout elapses without the event (unless the step declares a default).

In every case the journal records the **cause**, which is handed to each compensation as `Cause()`. A compensation frequently needs it: "release the stock because the payment failed" and "release the stock because the customer cancelled" may write different audit records.

### 9.3 Executing a compensation

A compensation is a durable job (`compensate`) to the **same task actor ID** the forward task used, carrying the same input and item plus the output that task produced. That output is usually what identifies the effect to undo — a charge ID, a reservation token, an object key — which is why `DecodeResult` exists.

Compensations get their own retry policy, `WithCompensateMaxAttempts`, defaulting higher than the forward policy: a failed rollback leaves the system inconsistent, so it is worth trying harder.

Compensations are **at-least-once**, like everything else, so `refundCard` must tolerate being called twice for the same charge. In practice this means keying the undo on the forward operation's identifier, which the handler already has.

### 9.4 The failing step itself

By default a step that **failed** is not compensated: the saga convention is that a step which did not complete did not take effect. That is a convention, not a guarantee — a step can fail after its side effect landed and before it reported.

`WithCompensateOnFailure()` opts a step into being compensated even when it failed, for handlers whose effect may be partial. Such a compensation must be written defensively: it may be undoing something that never happened.

### 9.5 When a compensation fails

`WithCompensationFailurePolicy` chooses:

| Policy | Behavior |
|---|---|
| `workflow.ContinueUnwinding` (default) | Record the failure and keep unwinding the remaining frames. The instance terminates as `failed` with `compensation: partial`. |
| `workflow.AbortUnwinding` | Stop at the failed frame. The instance terminates as `failed` with `compensation: failed`, and the journal names exactly which frames were not unwound. |

`ContinueUnwinding` is the default because stopping the unwind at the first problem usually leaves *more* state stranded than continuing does, and because the alternative is an instance that sits in `compensating` waiting for a human. Either way the outcome is explicit in the status, which is what an alert should be built on.

Neither policy silently succeeds: a workflow whose rollback did not complete is never reported as cleanly rolled back.

### 9.6 Status model

Rather than multiplying terminal statuses, the instance carries a status plus a compensation outcome:

```go
type Status string

const (
    StatusPending      Status = "pending"      // the start job is durable but has not run
    StatusRunning      Status = "running"
    StatusCompensating Status = "compensating"
    StatusCompleted    Status = "completed"    // terminal
    StatusFailed       Status = "failed"       // terminal
    StatusCancelled    Status = "cancelled"    // terminal
)

type CompensationOutcome string

const (
    CompensationNone      CompensationOutcome = "none"      // nothing needed unwinding
    CompensationCompleted CompensationOutcome = "completed" // every frame unwound
    CompensationPartial   CompensationOutcome = "partial"   // some frames failed, the rest were unwound
    CompensationFailed    CompensationOutcome = "failed"    // the unwind stopped early
)
```

Per-step status is `pending`, `running`, `completed`, `failed`, `skipped`, `compensating`, `compensated`, or `compensation-failed`.

```mermaid
stateDiagram-v2
    [*] --> pending: Start dispatches the start job
    pending --> running: the start job runs
    running --> completed: every step completed
    running --> compensating: a step failed, Cancel, or a deadline elapsed
    compensating --> failed: unwound after a failure
    compensating --> cancelled: unwound after a Cancel
    completed --> [*]: retention elapses
    failed --> [*]: retention elapses
    cancelled --> [*]: retention elapses
```

An instance with nothing on its compensation stack passes through `compensating` in a single turn, so the path is uniform whether or not anything has to be undone.

## 10. Journal

### 10.1 Shape

One state document per instance, as a single actor state value:

```go
type instanceState struct {
    Workflow    string          `json:"workflow"`
    Version     int             `json:"version"`
    Status      Status          `json:"status"`
    Compensation CompensationOutcome `json:"compensation,omitempty"`
    Input       json.RawMessage `json:"input,omitempty"`
    Cursor      string          `json:"cursor,omitempty"`   // name of the step being executed or unwound
    Steps       []stepRecord    `json:"steps"`              // in definition order, only steps that were reached
    Stack       []string        `json:"stack,omitempty"`    // compensation frames, oldest first
    Cause       string          `json:"cause,omitempty"`    // what triggered the unwind
    CreatedAt   time.Time       `json:"createdAt"`
    StartedAt   time.Time       `json:"startedAt"`
    CompletedAt time.Time       `json:"completedAt,omitzero"`
}

type stepRecord struct {
    Name        string       `json:"name"`
    Kind        Kind         `json:"kind"`
    Status      StepStatus   `json:"status"`
    Tasks       []taskRecord `json:"tasks"`
    Remaining   int          `json:"remaining"`   // tasks that have not reported, so completion is O(1)
    StartedAt   time.Time    `json:"startedAt"`
    CompletedAt time.Time    `json:"completedAt,omitzero"`
}

type taskRecord struct {
    Index       int             `json:"index"`
    Item        json.RawMessage `json:"item,omitempty"`   // fan-out item
    Output      json.RawMessage `json:"output,omitempty"`
    Error       string          `json:"error,omitempty"`
    Done        bool            `json:"done"`
    Compensated bool            `json:"compensated,omitempty"`
    CompletedAt time.Time       `json:"completedAt,omitzero"`
}
```

`Remaining` is carried explicitly rather than recomputed, which is what `imageoptim` does and what keeps the "have all tasks reported" check from scanning a large fan-out on every result.

### 10.2 Size

Francis state is read and written as one value per actor, so the journal has to stay small. Three bounds:

- `WithMaxOutputSize` (default 64 KiB) per task output, enforced on the task actor before it reports. Exceeding it fails the task permanently with a clear error rather than corrupting the instance.
- `WithMaxJournalSize` (default 1 MiB) on the encoded journal, checked before `SetState`. Exceeding it fails the instance, which is a much better outcome than an instance that can no longer persist and therefore can no longer progress.
- A documented guidance limit of a few hundred tasks per instance. Beyond that, the right shape is a parent workflow that starts child instances.

Every write of the journal rewrites the whole document. That is acceptable for the sizes above, and it is what buys the design its most important property: **a step transition is a single atomic state write**, so there is no partially-applied journal to reason about.

### 10.3 Retention

`WithRetention(d)` sets a TTL on the state of a terminated instance, exactly as `imageoptim` does with `CompletedWorkflowRetention`. Zero retains it indefinitely. `Service.Purge` deletes it early.

Once the state has expired, `GetStatus` reports "not found". This is a deliberate trade-off — the journal is an operational record, not an audit log. An application that needs a permanent record should write one from a terminal step, or from the `completed`/`failed` metric hooks.

## 11. Status, listing, and observability

### 11.1 Status

`GetStatus` is a `Peek`, so status reads run concurrently with each other and never queue behind another status read, only behind a write turn. The pattern `imageoptim` uses is generalized: read through the provider, and if the state does not exist yet, look for a live `start` job on the actor, which distinguishes "pending" from "no such instance".

```go
type InstanceStatus struct {
    InstanceID   string
    Workflow     string
    Version      int
    Status       Status
    Compensation CompensationOutcome
    CurrentStep  string
    Steps        []StepStatusView   // name, status, task counts, timings, error
    Cause        string
    CreatedAt, StartedAt, CompletedAt time.Time
}
```

### 11.2 Listing

`Service.List` is built on `Service.ListStates` over the orchestrator's actor type, which already returns actors with stored state, paginated by actor ID, without activating them. Because the default instance ID is a UUIDv7, the listing is in creation order. Filtering by status requires `IncludeData`, and is therefore a client-side filter over a page — good enough for an operator console, and explicitly not a query engine.

### 11.3 Metrics

Per workflow name: instances started, instances terminated by status, instance duration, step duration by step name and outcome, task attempts, tasks dead-lettered, compensations run and failed, and the current number of running instances.

### 11.4 Tracing

A workflow instance is a long-lived, multi-host activity, so it cannot be one span. The proposal is:

- one span per orchestrator turn, and one per task execution, tagged with instance ID, workflow, version, step, and index;
- the trace context of the `Start` call is recorded in the journal, and each task's span **links** to it, rather than being a child of a span that ended long ago.

Whether Francis already propagates trace context across the durable job boundary needs to be confirmed: `internal/tracing` and the peer/runtime clients propagate context across *transport* hops, but a job is persisted and executed later, so the context has to be carried in the job payload for this to work. If it is not carried today, this design needs it, and it is generally useful beyond workflows.

## 12. Definition versioning and rolling deployments

A definition lives in Go code on the hosts. A running instance's journal refers to steps by name, in the order the definition had when it started. A deployment that changes the definition while instances are running is therefore the hardest operational problem in this design, and the one Dapr's replay model handles worst.

The proposal:

1. **`WithVersion(n)` is recorded in the journal** when the instance starts.
2. **A host that does not have the instance's version declines to advance it.** The orchestrator's job handler returns `actor.ErrJobRejected`, which re-routes the occurrence to another host **without counting an attempt and without dead-lettering it**. During a rolling deployment, instances of the old version drain onto the hosts still running the old code, and new instances start on the new one. This falls straight out of a mechanism Francis already has.
3. **If no host claims a version**, the job keeps re-routing with backoff. The instance's `deadline` alarm eventually terminates it, and the status names the missing version. A `WithUnknownVersionPolicy(Park|Fail)` option lets an operator choose between failing such an instance and leaving it parked until the old code is redeployed.
4. **Compatible changes do not need a new version.** Adding a step after the cursor, or changing a handler's implementation, is safe. Renaming, reordering, or removing a step that instances may have reached is not, and should get a new version.

The engine can also compute a **fingerprint** of the step names, kinds, and order, and refuse to register two different definitions under the same name and version. That turns "someone edited the graph and forgot to bump the version" from a corrupted instance into a startup error.

## 13. Alternatives considered

**Code-as-workflow with replay (the Dapr/DTF model).** Rejected per §4: the expressiveness is real, but so are the determinism rules, the versioning trap, and the SDK surface required to hide the machinery. The stated goal here is the opposite — that nothing is hidden.

**Build the task executor on `taskpool`.** Attractive, since `taskpool` already gives strict per-host concurrency, capability queues, and re-routing. Rejected because a task pool is explicitly fire-and-forget and has no result path, and because a workflow needs the dead-letter hook to report a terminal failure back to its orchestrator. The workflow registers its own task types with the *same* mechanics (`CapacityGroup`, `CapacityGroupLimit`, per-capability types), which is the part worth reusing; the parts that differ are the parts that matter.

**Run steps on the orchestrator itself.** Much less code: no second actor type, no result jobs, no idempotency keys between the two. Rejected because a step would hold the instance's turn lock for its whole duration — blocking status reads, cancellations, and timeouts — and because it gives up parallelism entirely, which is one of the two features being asked for.

**One actor per step instead of one per task.** Would halve the number of activations for parallel steps. Rejected because a fan-out's tasks would then be serialized by that actor's turn lock, which defeats the purpose.

**An event-sourced journal (append-only records) instead of one document.** Better for large instances and gives a natural audit log. Rejected for v1 because Francis state is a single value per actor, so an append-only log would need side actors or a second storage concept, and because a single document makes a step transition one atomic write. Worth revisiting if the size bounds in §10.2 turn out to be too tight.

**Compensations as ordinary steps in a declared "on failure" branch.** More uniform, and it makes the rollback path visible in the graph. Rejected because the unwind set is determined at runtime by how far the instance got, so the branch would have to be conditional on each forward step's status — which is the compensation stack, written less directly.

## 14. Mapping `imageoptim` onto this design

The design is only worth building if it subsumes the case that motivated it. `imageoptim`'s workflow becomes:

```go
wf, err := workflow.New("thumbnails",
    workflow.WithTimeout(cfg.Actors.WorkflowTimeout),
    workflow.WithRetention(cfg.Actors.CompletedWorkflowRetention),
    workflow.WithConcurrency(cfg.Images.MaxConcurrentEncodes),
    workflow.WithSteps(
        // Normalizes the request and produces the list of thumbnails to generate
        workflow.Step("plan", workflow.WithRun(planThumbnails)),

        // One task per thumbnail, exactly as thumbnailgen does today
        // A thumbnail that cannot be encoded is recorded and does not fail the workflow
        workflow.ForEach("generate",
            workflow.WithItemsFrom("plan"),
            workflow.WithRun(generateThumbnail),
            workflow.WithFailurePolicy(workflow.TolerateFailures),
        ),

        // Writes manifest.json from the fan-out's outputs
        workflow.Step("manifest", workflow.WithRun(writeManifest)),
    ),
)

// The image ID is the instance ID, so a retried upload starts nothing new
id, err := svc.Start(ctx, req, workflow.WithInstanceID(imageID))
```

What the service keeps: the encoder, the object store, the request parsing, the metrics that are about images. What it deletes: `imageworkflow.go` and the orchestration half of `thumbnailgen.go` — the fan-out, the result accounting, the `Remaining` counter, the timeout alarm, the idempotency keys, the dead-letter hook, the `Peek` status endpoint's plumbing, and the "is this a duplicate delivery" checks in every handler. Roughly 400 lines of subtle, well-tested code becomes a declaration and three handler functions.

Two behaviors change, both for the better: the status endpoint reports per-step progress rather than a thumbnail count, and a failed workflow can be re-driven by starting a new instance with the same input rather than by re-uploading.

It also gains something it does not currently have: if thumbnails were written to a store where a partial result mattered, `WithCompensate(deleteThumbnail)` would be one line.

## 15. Phasing

**Phase 1 — the engine.** Orchestrator and task actors, journal, the `advance`/`reconcile` loop, sequential steps, static parallel groups, `deadline` alarm, `Start`/`GetStatus`/`Cancel`, retention, metrics. Enough to replace `imageoptim` except for the fan-out.

**Phase 2 — fan-out and compensation.** `ForEach` with `WithItemsFrom` and `WithMaxParallel`, the three failure policies, the compensation stack and its policies. This is the point at which `imageoptim` can be ported and the design validated against a real service.

**Phase 3 — waiting and routing.** `WaitForEvent` and `RaiseEvent`, per-step capabilities, `WithCondition` for skipping, `WithWatchdog`, `List`, `Purge`.

**Phase 4 — versioning and composition.** `WithVersion`, the `ErrJobRejected` drain, the definition fingerprint, and child workflows.

Documentation follows the existing built-in actor pages (`docs/content/builtin-actors/`), and the engine gets the same functional-test treatment as `taskpool` and `signal`, plus table-driven unit tests over `advance` since it is a pure function of a serialized journal.

## 16. Open questions

1. **Trace context across durable jobs.** Does a job carry the trace context of its dispatcher today? If not, this needs adding, and it affects more than workflows (§11.4).
2. **Fan-out over a field of the input.** Today the list must be a step's whole output, so fanning out over one field of the workflow input costs a `plan` step. A field selector would remove it, at the cost of introducing an expression of some kind. Is the round-trip worth avoiding?
3. **Child workflows.** A step that starts another workflow and waits for it. The mechanics are clear (the child dispatches `done` to its parent; compensating the step means cancelling the child instance), but it interacts with versioning and with the journal size bound. Phase 4 or later?
4. **Suspend and resume.** Dapr has it, and it is genuinely useful for operations ("stop making progress while we fix the downstream"). It is cheap to add — a status that makes `reconcile` a no-op — but it interacts with deadlines: does a suspended instance's timeout keep running?
5. **Conditions.** `WithCondition(fn)` would have to evaluate on the orchestrator, which violates "no user code on the orchestrator". The alternative is a step that returns a boolean and a `WithSkipIf("check", false)` on the following step — more durable round-trips, but consistent with the rest of the design. Which way?
6. **Retry policy granularity.** Step-level `WithMaxAttempts` maps onto the actor type's registration options, which are per *actor type*, not per step. Supporting genuinely per-step retry policies means either one task actor type per step (a lot of types) or implementing backoff in the engine on top of a single-attempt job. Which cost is right?
7. **`CancelJob` on an active occurrence.** It removes the job row, but does it interrupt an executing occurrence's context? The fail-fast policy's behavior depends on the answer.
8. **Should the orchestrator use `LockModeShared`?** `signal` does, to keep parked waiters from blocking the completion that releases them. The orchestrator's turns are short and it has no parked callers, so exclusive turns look right — but a very wide fan-out reporting simultaneously would serialize on it.
