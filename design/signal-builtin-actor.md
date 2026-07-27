# Design: the `signal` built-in actor

Status: implemented
Scope: one new framework primitive (`RegisterActorOptions.LockMode`) plus one new built-in actor (`builtin/signal`)

## 1. Summary

A **signal** is a single-shot, cluster-wide broadcast point addressed by an ID. Any number of callers block waiting for it; one caller completes it, optionally with a payload; every waiter is released at once with that payload; and every caller that arrives *after* the completion gets the payload immediately without blocking.

It is implemented as a built-in actor, so it inherits placement, routing, durable state, activation, deactivation, tracing, and both topologies unchanged. The one thing it does not inherit is the exclusive turn lock, because a call that spends its whole life blocked cannot hold a lock that the call releasing it needs to acquire.

The completion payload is persisted with a **retention TTL configured when the signal is created**, which is the window during which late callers are guaranteed an immediate, correct answer.

## 2. Goals

- Many concurrent waiters per signal, on any host in the cluster
- Waiters may disconnect and reconnect freely; reconnecting is always correct and requires no server-side per-caller bookkeeping
- Completion returns to its caller immediately, and is durable before it returns
- Callers arriving after completion return immediately, for as long as the configured retention window
- No change to turn-based semantics for any existing actor type

## 3. Non-goals

- Repeatable or re-armable signals — a signal fires once; a re-armable event stream is a different primitive with different semantics (cursors, replay, ordering)
- Multi-payload / streaming broadcast
- Delivery guarantees to callers that are not currently waiting — a signal is level-triggered state, not a message queue. Callers that want durable at-least-once delivery should use jobs
- Ordering across different signals

## 4. Why the turn lock is the only obstacle

Recorded here because it drives every decision below.

Under today's model the natural encoding is "wait is a `Peek`, complete is an `Invoke`". That deadlocks:

- `TurnBasedLocker.Lock` only grants when there are no active holders (`internal/locker/turn-based.go:58`), so `complete` queues behind every parked waiter, and every parked waiter is waiting for `complete` to run
- Once a writer is queued, `RLock` also stops granting (`internal/locker/turn-based.go:82`, deliberately, to prevent writer starvation), so every waiter arriving after the first `complete` attempt blocks too. A single `complete` wedges the actor permanently
- Independently, a `Peek` cannot persist anything (`actor/service.go:41`), so a late caller after a completion could never be answered from durable state

Everything else about actors fits this use case well. Placement gives exactly one active instance per signal ID cluster-wide, which is what makes an in-process waiter registry correct. Durable state gives the late-caller answer. Idle deactivation gives the memory reclaim. So the change is to make the lock optional, not to leave the actor abstraction.

## 5. Core change: shared-lock actor types

### 5.1 The idea

Add a lock mode as a property of the **actor type**, not of the individual call. An actor type registered in shared mode has *every* invocation acquire the shared (read) lock instead of the exclusive one, and no invocation ever acquires the exclusive lock. Such an actor synchronizes itself internally.

This is deliberately not a new "unlocked" path. Reusing `RLock` for every call means:

- Invocations never block each other, which is the property we need
- `TryLock` still reports the actor busy while any invocation is in flight (`internal/locker/turn-based.go:161`), so idle deactivation stays correct with **zero changes** to `HandleIdleActor`
- `Halt` / `StopAndWait` drain semantics work unchanged
- Nothing can ever queue on the writer side, so the writer-blocks-readers rule in `RLock` never fires

### 5.2 API

In `internal/actorcore/register-actor.go`:

```go
// LockMode selects how the framework serializes invocations of an actor type
type LockMode uint8

const (
	// LockModeExclusive is the default turn-based model: one invocation at a time, with Peek calls sharing the read side
	LockModeExclusive LockMode = 0
	// LockModeShared runs every invocation of the type concurrently, under the shared lock, and never takes the exclusive lock
	// An actor type registered this way is responsible for its own synchronization, including protecting its own durable state writes
	// It is reserved for built-in actors, since the public RegisterActor exposes no option to select it
	LockModeShared LockMode = 1
)
```

added to `RegisterActorOptions` as a `LockMode LockMode` field.

Gating comes for free: `host/local/register-actor.go` and `host/remote/register-actor.go` build `RegisterActorOptions` only from the exported `With*` helpers, so simply not exporting a `WithLockMode` helper means application actors cannot select shared mode. Built-ins pass an `actorcore.RegisterActorOptions` struct directly through `BuiltInActorRegistration.RegisterOptions` and can.

### 5.3 Changes to `ActiveActor`

`NewActiveActor` takes the mode and stores it, plumbed from `createActorFn`. `ActiveActor.LockMode()` exposes it to the manager.

The per-type mode lives in a new `Manager.actorTypeLockMode` map rather than in `ActorsConfig`: that struct is `components.ActorHostType`, the placement store's view of an actor type, and the lock mode is purely host-local.

No changes to `Lock`, `RLock`, `TryLock`, `Unlock`, `RUnlock`, or `Halt`.

### 5.4 Changes to `lockAndInvokeActor`

`internal/actorcore/manager.go:265` currently selects the acquire/release pair from the `readOnly` flag. It gains one branch:

```go
// A shared-mode actor synchronizes itself, so every invocation takes the shared lock and none ever takes the exclusive one
// This is what lets a call that blocks (waiting on a signal) coexist with the call that releases it
if act.LockMode() == LockModeShared {
	acquire = act.RLock
	release = act.RUnlock
}
```

applied *after* the `readOnly` branch, so it wins for both Invoke and Peek.

### 5.5 Dispatch and the read-only context

A shared-mode invocation must **not** be stamped with `types.WithReadOnly`, because the actor legitimately mutates its own durable state. The `readOnly` flag on the wire continues to mean only "dispatch to `ActorPeek` instead of `ActorInvoke`", which it already does at both dispatch sites (`internal/actorcore/messaging.go:156` and `:388`).

Simplest coherent rule: **a shared-mode actor type rejects `Peek` and `PeekStream`.** All calls arrive as `Invoke`, dispatch to `ActorInvoke`, and are never marked read-only. Implemented by returning `ErrActorMethodUnsupported` when `readOnly` is set on a shared-mode type, which the caller already surfaces as a clean protocol error. A shared-mode actor simply does not implement `ActorPeek`, so the existing type assertion would produce the same error anyway; making it explicit gives a better message.

### 5.6 No protocol change

Because the mode is a property of the type and is resolved on the owning host, `protocol.InvokeActorRequest` is untouched. Callers do not know or care. Placement, transports, the stale-placement retry, request-ID coalescing, remote topology, and cluster admin all work as-is.

### 5.7 Halting: the one behavior that needs fixing

`lockAndInvokeActor` cancels an in-flight invocation only after `ShutdownGracePeriod` elapses following a halt (`internal/actorcore/manager.go:298`). Meanwhile `HaltActiveActor(act, true)` calls `locker.StopAndWait()`, which blocks until all readers release. With parked waiters that means **every graceful host shutdown stalls for the full grace period**.

The grace period exists to let a turn finish real work. A parked waiter has no work to finish; it should leave as soon as it learns the actor is going away. So the framework should let a handler observe halting directly:

```go
// actor/context.go

// HaltingFromContext returns a channel that is closed when the actor running the current invocation begins halting.
// A long-running handler should select on it and return promptly, so a graceful shutdown does not have to wait out the shutdown grace period.
// It returns nil for contexts that carry no halt signal, and a nil channel blocks forever, so a plain select on it is always safe.
func HaltingFromContext(ctx context.Context) <-chan struct{}
```

`lockAndInvokeActor` already holds `haltCh`; stamping it into `execCtx` is a two-line change. The signal actor's `wait` selects on it and returns `actor.ErrActorHalted`, which the caller's invocation layer already classifies as retryable (`internal/actorcore/messaging.go:193`), producing a re-resolve to the actor's new placement. This is useful well beyond signals — any long-running handler benefits.

Without this, shutdown correctness is preserved but every drain costs `ShutdownGracePeriod`.

## 6. The `signal` built-in

### 6.1 Public API

Mirrors `builtin/ratelimit` and `builtin/taskpool`.

```go
package signal

// New builds a signal built-in actor identified by name
// Register the returned value on a host with the host's RegisterBuiltInActor method, then obtain a SignalService with Service
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*Signal, error)

// Service binds the signal set to an actor.Service
func (s *Signal) Service(svc *actor.Service) *SignalService

// Wait blocks until the signal completes and returns its payload, or nil when the signal carried none
// It returns immediately when the signal has already completed and is still within its retention window
// It reconnects transparently across host failures and placement changes, and returns only when the signal completes or ctx is done
func (s *SignalService) Wait(ctx context.Context, signalID string) (actor.Envelope, error)

// Complete completes the signal, releasing every waiter with the given payload, and returns as soon as the completion is durable
// Pass nil when the signal carries no payload
// It returns ErrAlreadyCompleted when the signal has already completed, which is safe to ignore: the first completion stands
func (s *SignalService) Complete(ctx context.Context, signalID string, data any) error

// Check reports whether the signal has already completed, without blocking
func (s *SignalService) Check(ctx context.Context, signalID string) (data actor.Envelope, completed bool, err error)
```

### 6.2 Options

```go
// WithRetention sets how long a completed signal's payload is kept, which is the window during which a late Wait returns immediately
// It defaults to 24 hours
// A negative value keeps completions forever, which is appropriate when a caller may arrive arbitrarily late and the signal set is bounded
func WithRetention(d time.Duration) Option

// WithIdleTimeout overrides how long a signal's actor stays in memory after its last call before being deactivated
// It defaults to 5 minutes
// Deactivating a completed signal is free: a later Wait re-activates it and answers from durable state
func WithIdleTimeout(d time.Duration) Option

// WithMaxPayloadSize caps the size of a completion payload, in bytes, since the payload is stored in the actor's state row and returned to every waiter
// It defaults to 64 KiB
func WithMaxPayloadSize(n int) Option
```

Retention follows the existing `IdleTimeout` convention in `RegisterActorOptions.Validate` exactly: zero means "use the default", negative means "no expiration". `WithRetention(-1)` maps to `SetStateOpts.TTL == 0`, which the providers already treat as no expiry.

### 6.3 Registration

One actor type per named signal set, one actor **instance** per signal ID:

```
francis.builtin.signal.<name>     actor type
<signalID>                        actor ID
```

Not a singleton. `Signal.RegisterOptions()` returns `LockMode: LockModeShared`, the configured `IdleTimeout`, and no capacity group.

Register the same signal set (same name and options) on every host that should be able to wait on or complete its signals, as with the other built-ins.

### 6.4 Methods

Internal, reachable only through `SignalService` since built-in types are reserved from public clients:

| Method | Semantics |
|---|---|
| `wait` | Blocks until completion; returns the payload |
| `complete` | Persists then broadcasts; returns immediately |
| `check` | Non-blocking snapshot |

### 6.5 Durable state

```go
// signalState is the actor's durable record, written exactly once when the signal completes
// A pending signal has no state row at all, which is what distinguishes it from a completed one
type signalState struct {
	// Completed is always true when the record exists, and distinguishes a real record from the zero value GetState returns on a miss
	Completed bool `msgpack:"completed"`
	// Data is the caller's payload, already MessagePack-encoded, kept opaque end to end
	Data []byte `msgpack:"data,omitempty"`
	// CompletedAt is when the completion was persisted, for diagnostics
	CompletedAt time.Time `msgpack:"completedAt"`
}
```

The `Completed` field is load-bearing: `client.GetState` swallows `ErrStateNotFound` and returns the zero value with a nil error (`actor/client.go:152`), so there is no other way to tell a miss from a hit.

### 6.6 In-memory instance

```go
type signalActor struct {
	client    actor.Client[signalState]
	retention time.Duration

	mu        sync.Mutex
	loaded    bool          // whether durable state has been consulted for this activation
	completed bool
	data      []byte
	done      chan struct{} // closed on completion, the broadcast mechanism
}
```

`done` is created by the factory and closed exactly once. Closing a channel is the whole fan-out: every parked waiter is released simultaneously with no per-waiter bookkeeping, which is why waiter churn costs nothing.

### 6.7 `wait`

1. Take `mu`. If not `loaded`, read durable state through the client and set `loaded`, `completed`, `data`. Release `mu`, capturing `completed`, `data`, and `done`
2. If `completed`, return the payload immediately
3. Otherwise `select` on `done`, `ctx.Done()`, and `actor.HaltingFromContext(ctx)`
   - `done` closed: re-read `data` under `mu` and return it
   - `ctx` done: return `ctx.Err()`; the waiter simply disappears, nothing to clean up
   - halting: return `actor.ErrActorHalted`, which the caller's layer treats as retryable and re-resolves

The state read happens **once per activation**, not once per waiter. Because placement guarantees a single active instance cluster-wide, that instance is authoritative for the rest of its life: any completion must pass through it. So a signal with 10,000 waiters performs exactly one database read. (The client's own cache would collapse the reads anyway, including the miss, but the explicit `loaded` flag makes the invariant visible rather than incidental.)

Note the deliberate absence of a waiter registry, a waiter count, or any subscriber identity. There is nothing to leak and nothing to clean up on disconnect.

### 6.8 `complete`

1. Reject a payload over `MaxPayloadSize`
2. Take `mu`. Load durable state if not `loaded`. If already `completed`, release and return `ErrAlreadyCompleted`
3. Still holding `mu`, `client.SetState(ctx, signalState{...}, &actor.SetStateOpts{TTL: retention})`. On error, release and return it — nothing observable has changed
4. Set `completed`, `data`; `close(done)`; release `mu`
5. Return

The ordering matters: **durable before observable.** A crash between 3 and 4 loses only the in-memory broadcast; waiters see their connection break, re-resolve, and the fresh activation reads the persisted completion. A crash before 3 means the completion never happened, and `Complete` never returned success to its caller.

Holding `mu` across the state write serializes concurrent `complete` calls, giving first-write-wins within an activation. It blocks other `complete` calls only — `wait` touches `mu` briefly and never during I/O.

Cross-activation double-completion (an outgoing host that has not yet noticed it lost placement) is not fully preventable: the state API has no compare-and-swap. This is the framework's existing placement-handoff caveat and is acceptable here, since two racing completions of the same signal is a caller-level error and the payloads would normally be identical. Worth revisiting if a conditional-write primitive is ever added to `SetStateOpts`.

## 7. Client-side fan-in

This is the difference between working at 100 waiters and working at 100,000, and belongs in v1.

Every parked `Wait` from a host to the signal's owning host holds one QUIC stream and one in-flight slot on that host's peer session. `MaxInFlightRequests` defaults to 100 per session (`internal/peer/server.go:185`), and the QUIC server admits only that plus 8 streams (`internal/peer/server.go:93`). Without aggregation, the 101st concurrent waiter routed over one session is rejected as overloaded.

So `SignalService` keeps a host-local registry keyed by signal ID. The first local `Wait` for a signal opens the upstream invocation; every subsequent local `Wait` for the same signal attaches to that one call and is released from the same result. **Cross-host streams become O(hosts), not O(waiters)**, and a host with 50,000 local waiters holds exactly one upstream call.

Two requirements that a naive `singleflight` gets wrong:

- The shared upstream call must run under its **own context**, not any one caller's. Cancelling one waiter must not cancel the others. Reference-count the attached waiters and cancel the upstream context only when the last one leaves
- The shared call must **retry across failures** rather than propagating them. On a retryable error — host loss, halt, placement change, transport failure — it re-resolves and re-invokes with backoff, and only individual waiters' own contexts end their waits. This is what makes "clients may disconnect and reconnect at any time" true of the framework's own hops, not just the user's

The registry is also the natural place to fan a completion out to local waiters, which is a channel close, same as inside the actor.

A completed result is *not* cached locally. Retention lives in one place — the durable state — and a local cache would add a second, inconsistent expiry.

## 8. Retention semantics

Retention is the promise: **within the window, a late `Wait` returns immediately and correctly.**

After the window, the state row is gone and the actor cannot distinguish "never fired" from "fired and expired" — there is nothing left to distinguish it with. A `Wait` arriving after expiry therefore blocks as though the signal had never fired.

This is inherent to the model, not an implementation gap, and it is the primitive's sharpest edge. It must be stated plainly in the docs: **set retention longer than the longest plausible lateness of any caller.** For a signal whose waiters may arrive arbitrarily late, use `WithRetention(-1)` and accept unbounded growth of the state table for that signal set, or bound it by garbage-collecting signal IDs out of band with `Service.ListStates`.

Callers that need to distinguish "still pending" from "gave up" should bound their own `Wait` with a context deadline; `Check` gives the non-blocking snapshot for callers that want to decide for themselves.

## 9. Failure modes

| Event | Behavior |
|---|---|
| Waiter's context cancelled | `Wait` returns `ctx.Err()`; refcount drops; upstream call ends when the last local waiter leaves |
| Owning host crashes | Streams break, waiters re-resolve, the new activation reads durable state. A completion persisted before the crash is delivered; one that never persisted never happened |
| Owning host drains | Actor halts; `wait` observes halting via `HaltingFromContext` and returns `ErrActorHalted`; caller re-resolves to the new placement |
| Actor idles out with waiters parked | Cannot happen: `TryLock` reports busy while any invocation holds the shared lock, and `HandleIdleActor` re-enqueues (`internal/actorcore/manager.go:452`) |
| Actor idles out after completion | Normal deactivation; a later `Wait` re-activates and answers from state |
| `Complete` retried after a lost response | Request-ID coalescing collapses an in-flight retry (`internal/actorcore/manager.go:81`); a later retry gets `ErrAlreadyCompleted`, which is safe to ignore |
| Two callers complete concurrently | First wins; second gets `ErrAlreadyCompleted`; the payload delivered to waiters is the first one |
| Retention expires, then a `Wait` arrives | Blocks as though never fired — see §8 |

## 10. Capacity and observability

Hosts serving many signal waiters should raise `WithMaxInFlightRequests`. With fan-in, the requirement is roughly "one slot per distinct signal being waited on from this host", plus normal traffic — a much smaller number than the waiter count, but still worth sizing.

Tracing: `wait` produces an `actor.execute` span lasting the entire wait, which will look alarming in a trace view. The `actor.lock` span will be near zero, since the shared lock is never contended in shared mode. The `wait` span should be annotated so it reads as a parked call rather than slow work.

Worth adding: a gauge of locally attached waiters per signal, and a counter of upstream re-connections, since a high reconnection rate is the signal that placement is thrashing.

## 11. Alternatives rejected

**Peek for waiters, Invoke for completion.** Deadlocks, as shown in §4. Not repairable without changing the lock topology, which is what this design does instead.

**A separate non-actor primitive with its own placement.** Re-implements placement resolution with caching and invalidation, the stale-placement retry, peer routing, the runtime path for the remote topology, a second ID namespace in the placement store, durable storage with TTL, in-memory lifecycle and GC, capacity accounting, tracing, protocol messages on two paths, and cluster-admin visibility — to avoid one mutex. It also costs users a second addressing model to learn, forever. The actor stack minus the turn lock *is* the primitive we want.

**Durable callback subscriptions instead of parked calls.** Waiters register an actor ref plus a method, and completion dispatches jobs. Strictly better for in-cluster actor subscribers — at-least-once, survives restarts, no parked requests, no stream pressure, all on machinery Francis already has. It cannot serve an external client holding an open long-poll, which is the stated use case. Worth adding later as a complementary `Notify` mode on the same signal; the durable state record is already the right foundation for it.

## 12. Implementation plan

All five steps are implemented. Two things changed from the plan while building it, both recorded in the sections above:

- **Already-completed travels as a result, not an error.** The actor returns `completeResult{AlreadyCompleted: true}` and the service translates it into `ErrAlreadyCompleted`. An error returned from the actor is flattened into an opaque protocol error when it crosses a peer boundary, so `errors.Is(err, ErrAlreadyCompleted)` would have worked on the owning host and failed everywhere else.
- **The payload cap is enforced in the service**, before the invocation is sent, so an oversized payload fails fast and never reaches a host. The service is the only caller, since built-in types are reserved from public clients.

1. **`LockMode` in the actor core.** `RegisterActorOptions.LockMode`, `ActiveActor` plumbing, the `lockAndInvokeActor` branch, `Peek` rejection for shared types, and the same branch in `LockAndStream`. No public API surface
2. **`HaltingFromContext`.** Stamp `haltCh` into the invocation context; document it on the long-running-handler path
3. **`builtin/signal`.** The actor, options, and `SignalService` without fan-in — correct but bounded by `MaxInFlightRequests`
4. **Fan-in registry** in `SignalService`, with refcounted shared upstream calls and reconnect-with-backoff
5. **Docs.** A section in `docs/content/docs/builtin-actors.md`, with retention's expiry edge stated prominently, plus an example under `examples/`

Steps 1 and 2 are independently useful and independently reviewable. Step 4 changes no semantics, only scale, so it can land separately if step 3 needs to ship early.

## 13. Test plan

- **Locker.** A shared-mode actor admits N concurrent invocations; one invocation that blocks does not delay another; `TryLock` reports busy throughout; the exclusive lock is never taken
- **Deadlock regression.** The exact scenario from §4: N waiters parked, then `complete` — must return promptly. This is the test that justifies the whole design and should be named so
- **Fan-out.** N waiters across multiple hosts all released with the identical payload from one `complete`
- **Late waiter.** `Wait` after completion returns immediately, both on the live activation and after forced deactivation (proving the durable path)
- **Retention.** Completion readable within the window; after expiry a `Wait` blocks (asserting the documented behavior, so a future change to it is deliberate)
- **Idempotency.** Concurrent completions — one wins, the other gets `ErrAlreadyCompleted`, waiters all see the winner's payload
- **Host failure.** Kill the owning host with waiters parked; waiters re-resolve and are released by a later completion on the new host
- **Drain.** Halt a host with waiters parked; shutdown completes without waiting out `ShutdownGracePeriod`
- **Fan-in.** M local waiters produce exactly one upstream invocation; cancelling M-1 of them does not disturb the last; cancelling all M ends the upstream call
- **Payload cap.** An oversized payload is rejected at `Complete` and never persisted

The deadlock regression test earns its name: pointing it at a turn-based actor type instead of a shared-lock one hangs until the test's timeout, and it passes in well under a second against the shared-lock type.

## 14. Open questions

- Should `Wait` expose an option to fail fast instead of blocking when the signal is pending, or is `Check` sufficient? (Leaning: `Check` is sufficient)
- Should a signal set support explicit deletion of a completed signal's record, for callers that want to reclaim before retention expires? (Leaning: yes, a `Forget` method, cheap to add)
- Should shared mode ever be exposed to application actors? (Leaning: not until there is a second built-in using it, and the self-synchronization contract has been documented against real usage)
