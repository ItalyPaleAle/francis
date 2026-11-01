# Workflow engine design notes

These notes explain how the workflow engine in `builtin/workflow` works inside, for people changing it. The user-facing documentation, which explains what the engine does rather than how, lives in [`docs/content/workflows`](../../../docs/content/workflows/_index.md).

The notes describe the code as of IR format version 2 (`definitionIRVersion` in `ir.go`). When the code and these notes disagree, the code wins, and the notes need fixing.

## The three ideas everything else follows from

1. **The orchestration boundary.** Each workflow instance has one orchestrator actor that owns its journal, and that actor never runs user code. It reads and writes its own state, arms and drops its own alarm, and dispatches durable jobs. Every `WithRun` and `WithCompensate` handler runs on a separate worker actor. Because the orchestrator never executes user code, there is no replay and no determinism requirement on handlers: they may call the clock, do I/O, and use randomness, and they only need to be idempotent.
2. **The journal is the only source of truth, and every change to it is a turn.** A turn reads the journal, folds exactly one event into it, advances it as far as the definition allows, writes it back in a single state write, and then dispatches whatever the journal says should be running. Folding and advancing are pure functions of the journal and the definition, so any turn can be retried, and dispatching is idempotent, so a turn that failed halfway heals on the next one.
3. **The definition is compiled into a language-neutral IR, and its hash pins what a version means.** The Go declarations are lowered into an IR that contains only data, with the handlers kept aside as host-local bindings. The SHA-256 of the IR's canonical encoding is the definition's fingerprint, and a cluster-wide registry records the first fingerprint seen for each version, so two hosts can never run different graphs under the same version number.

## Reading order

| Document | What it covers |
| --- | --- |
| [01. Architecture](01-architecture.md) | The actors a workflow registers, how they talk to each other, and the life of one instance end to end |
| [02. Definitions and the IR](02-definitions-and-ir.md) | How `New` turns the Go declarations into the IR, the canonical encoding and fingerprint, validation, and the tools that print the IR |
| [03. The journal](03-journal.md) | The orchestrator's durable state, its records, and the state machines of instances, steps, and tasks |
| [04. Turns](04-turns.md) | The four phases every turn runs, the ordering invariants they keep, and what happens when a turn fails halfway |
| [05. Dispatch and workers](05-dispatch-and-workers.md) | Task payloads, worker execution, retries, idempotency keys, dead letters, and child instances |
| [06. Step kinds](06-step-kinds.md) | How each node kind opens, runs, settles, and produces output, plus conditions and data flow |
| [07. Compensation](07-compensation.md) | How an unwind opens, how the compensation stack is built and unwound, late successes, and unwinding children |
| [08. Versions and the registry](08-versions-and-registry.md) | Fingerprint registration, generations, the fences that keep a host from running a graph it does not have, and rolling deployments |
| [09. Time and suspension](09-time-and-suspension.md) | The single deadline alarm, instance and event timeouts, and suspend and resume |
| [10. History and retention](10-history-and-retention.md) | The event history, purge and retention, listing, and metrics |

## Glossary

| Term | Meaning |
| --- | --- |
| Definition | A validated workflow graph plus its settings, built by `New`; the `definition` type holds the IR, the indexes, the fingerprint, and the host-local bindings |
| Declaration | One node of the Go DSL (`stepDecl`) before lowering, built by `Step`, `Parallel`, `ForEach`, `Child`, `WaitForEvent`, or `Loop` |
| IR | The language-neutral form of a definition (`definitionIR` and `stepDef`), holding only data |
| Binding | The Go values linked to an IR step on this host (`stepBinding`): its `RunFunc`, its `CompensateFunc`, and the `*Workflow` a child step starts |
| Canonical encoding | The positional MessagePack encoding of the IR returned by `canonicalBytes` and `Workflow.CanonicalIR` |
| Fingerprint | The hex SHA-256 of the canonical encoding |
| Registry | The cluster-wide singleton actor per workflow that records the fingerprint of each version |
| Generation | A number the registry assigns each time it grants an identity to a version, so a start authorized before an operator reset cannot run after it |
| Instance | One run of a definition, identified by its instance ID, which is also the orchestrator's actor ID |
| Orchestrator | The workflow actor of one instance, which owns the journal (`orchestrator` in `orchestrator.go`) |
| Journal | The orchestrator's durable state (`instanceState`), rewritten whole on every write |
| Placeholder | The state stored together with a start job before the journal exists, so the pending instance can be listed |
| Turn | One run of the orchestrator's write path, folding one event into the journal |
| Event | The input of a turn, built from a delivered job or the deadline alarm; not to be confused with a history event |
| History event | One entry of the instance's event history, a timeline stored beside the journal for observability |
| Step | A top-level node of the definition, with one record in the journal |
| Task | One unit of work of a step: one worker actor and its jobs, or one child instance |
| Attempt | One execution of a task, numbered from 1 and recorded in the journal before its job is dispatched |
| Worker | The actor that runs one forward attempt; an undo worker runs one compensation attempt |
| Report | The job a worker or a child sends back to the orchestrator with a task's outcome |
| Frame | A settled step with something to undo, pushed on the compensation stack |
| Unwind | The compensation phase, during which frames are popped and compensated in reverse order |
| Dispatch marker | `DispatchedAttempt`, which records that the job of an attempt was accepted so later turns do not dispatch it again |

## Source map

| File | Responsibility |
| --- | --- |
| `workflow.go` | `New`, defaults, graph indexing and validation, the actor registrations, actor type and ID naming |
| `step.go` | The DSL: node kinds, policies, `StepSpec`, every `StepOption`, and option applicability rules |
| `options.go` | Workflow-wide options and their defaults |
| `ir.go` | The IR types, lowering from declarations, canonical encoding, and the fingerprint |
| `ir_decode.go` | Decoding the canonical encoding back into the IR |
| `ir_render.go` | `CanonicalIR`, `RenderIR`, and the Markdown and Mermaid renderers |
| `journal.go` | The journal types: instance, step, task, compensation, suspension, and parent records |
| `orchestrator.go` | The orchestrator actor: job and alarm entry points, the turn, persistence, reconcile, and deadlines |
| `advance.go` | The pure half of a turn: `apply`, which folds events, and `advance`, which moves the journal to a fixed point |
| `dispatch.go` | Building task payloads and choosing which upstream outputs a task may read |
| `worker.go` | The worker actor, the `Task` and `Compensation` interfaces handlers receive |
| `payloads.go` | Job methods and every job payload, plus how each kind of step exposes its output |
| `instance.go` | Purge, status reads, reporting to a parent, and job cleanup |
| `registry.go` | The definition registry actor and the authorization and confirmation calls |
| `service.go` | `WorkflowService`: start, status, events, control, list, purge, and registry operations |
| `history.go` | Deriving history events from journal diffs |
| `management.go` | Decoders and constants the management API and dashboard use |
| `metrics.go` | OpenTelemetry instruments |
| `describe_unit.go` | `(*instanceState).Describe`, a readable dump of a journal for debugging tests (built with the `unit` tag) |
| `../../cmd/workflow-ir` | The command-line tool that prints a canonical IR as Markdown or Mermaid |
