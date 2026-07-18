# Francis API reference

Deep reference for the `actor` package public API. Read this when you need exact signatures, option tables, or error semantics beyond the SKILL.md cheat-sheet.

## Table of contents

- [Actor interfaces](#actor-interfaces)
- [The Envelope](#the-envelope)
- [Client vs Service](#client-vs-service)
- [State](#state)
- [Alarms](#alarms)
- [Jobs](#jobs)
- [Invoking actors](#invoking-actors)
- [Streaming invocations](#streaming-invocations)
- [Halting](#halting)
- [Registration options](#registration-options)
- [Sentinel errors](#sentinel-errors)

## Actor interfaces

All optional; an actor implements only what it needs. The factory type is:

```go
type Factory func(actorID string, service *Service) Actor   // Actor = any
```

```go
type ActorInvoke interface {
	Invoke(ctx context.Context, method string, data Envelope) (any, error)
}
type ActorPeek interface {
	Peek(ctx context.Context, method string, data Envelope) (any, error)
}
type ActorAlarm interface {
	Alarm(ctx context.Context, name string, data Envelope) error
}
type ActorJob interface {
	Job(ctx context.Context, method string, data Envelope) error
}
type ActorJobFailed interface {
	JobFailed(ctx context.Context, jobID string, method string, data Envelope, jobErr error) error
}
type ActorBootstrapper interface {
	Bootstrap(ctx context.Context, data Envelope) error
}
type ActorDeactivate interface {
	Deactivate(ctx context.Context) error
}
```

Also (streaming variants, see below): `ActorStream` / `ActorPeekStream`.

`const SingletonActorID = "singleton"` — the well-known ID for singleton actors.

## The Envelope

```go
type Envelope interface {
	Decode(into any) error
}
```

The `data` argument to `Invoke`/`Peek`/`Alarm`/`Job`/`Bootstrap` is an `Envelope` carrying the JSON payload. It may be `nil` when the caller sent no payload — always nil-check before `Decode`:

```go
var req MyReq
if data != nil {
	err := data.Decode(&req)
	if err != nil {
		return nil, err
	}
}
```

A method's `return` value (`any`) is JSON-serialized and delivered to the caller as *their* `Envelope`. Return `nil` for no response.

## Client vs Service

- **`actor.Client[T]`** is pre-bound to one actor `(type, id)` and typed to the state `T`. Create it in the factory with `actor.NewActorClient[T](actorType, actorID, service)` and store it on the struct. Use it for the actor's own state/alarms/jobs. It caches state in memory for the activation. **Do not** share a client across goroutines or reuse it across activations.
- **`actor.Service`** takes `(actorType, actorID)` explicitly on every call. Get it from `host.Service()` (for outside-actor code like HTTP handlers) — it's also passed into every factory. Use it to reach a *different* actor, or from outside any actor.

Client interface:

```go
type Client[T any] interface {
	SetState(ctx context.Context, state T, opts *SetStateOpts) error
	GetState(ctx context.Context) (state T, err error)
	DeleteState(ctx context.Context) error
	SetAlarm(ctx context.Context, alarmName string, properties AlarmProperties) error
	DeleteAlarm(ctx context.Context, alarmName string) error
	Invoke(ctx context.Context, actorType, actorID, method string, data any, opts ...InvokeOption) (Envelope, error)
	Peek(ctx context.Context, actorType, actorID, method string, data any, opts ...InvokeOption) (Envelope, error)
	Dispatch(ctx context.Context, method string, input any, opts ...JobOption) (jobID string, err error)
	GetJob(ctx context.Context, jobID string) (JobInfo, error)
	ListJobs(ctx context.Context) ([]JobInfo, error)
	CancelJob(ctx context.Context, jobID string) error
	RetryJob(ctx context.Context, jobID string) (newJobID string, err error)
	Halt()
}
```

The `Service` mirrors these with explicit `(actorType, actorID)` args, plus `InvokeStream`, `PeekStream`, `HaltAll`, `Halt`, `HaltDeferred`. From within a `Peek`/`PeekStream` turn, the mutating client calls (`SetState`, `DeleteState`, `SetAlarm`, `DeleteAlarm`, `Dispatch`) return `ErrReadOnly`; `GetState` is always allowed.

## State

State is a JSON-serializable Go value keyed by `(type, id)`, independent of activation. Exported fields only. Keep it small; store large blobs externally and keep a reference.

```go
// Client (typed): zero value on first read, never ErrStateNotFound
state, err := c.client.GetState(ctx)
err = c.client.SetState(ctx, state, nil)
err = c.client.DeleteState(ctx)

// With a TTL so state auto-expires
err = c.client.SetState(ctx, state, &actor.SetStateOpts{TTL: 24 * time.Hour})

// Service (untyped): decodes into dest; returns ErrStateNotFound when absent
var s cartState
err = service.GetState(ctx, "cart", "user-42", &s)
err = service.SetState(ctx, "cart", "user-42", s, nil)
err = service.DeleteState(ctx, "cart", "user-42")
```

```go
type SetStateOpts struct {
	TTL time.Duration   // zero/nil = never expires
}
```

Difference: the typed client smooths "no state" into the zero value; the service form returns `ErrStateNotFound`, so use the service when you must distinguish "no state" from "zero state". Because of single-activation + turn-based writes, you don't need optimistic concurrency or locks for an actor's own state.

## Alarms

Durable scheduled callbacks delivered to `Alarm(ctx, name, data)`. Setting an alarm with an existing name replaces it. At-least-once delivery, so handlers must be idempotent. A failing `Alarm` is retried per the actor type's `MaxAttempts`/`InitialRetryDelay`.

```go
type AlarmProperties struct {
	DueTime  time.Time  // absolute first-fire time
	Interval string     // optional repeat: Go duration ("60s"), ISO-8601, or ms; empty = fire once
	TTL      time.Time  // optional: stop repeating after this time
	Data     any        // optional payload delivered to Alarm as the data envelope
}
```

```go
err := c.client.SetAlarm(ctx, "timeout", actor.AlarmProperties{
	DueTime: time.Now().Add(30 * time.Second),
	Data:    map[string]any{"reason": "checkout-timeout"},
})
err = c.client.DeleteAlarm(ctx, "timeout")   // ErrAlarmNotFound if absent
// From outside: service.SetAlarm(ctx, type, id, name, props) / service.DeleteAlarm(...)
```

## Jobs

Durable, fire-and-forget tasks dispatched to an actor `(type, id)`, delivered to `Job(ctx, method, data)`. Each dispatch is distinct and runs; on permanent failure the job is dead-lettered rather than lost.

### Jobs vs alarms

| | Alarms | Jobs |
|---|---|---|
| Delivered to | `Alarm(name, data)` | `Job(method, data)` |
| Keyed by | name (set replaces on collision) | server-issued JobID (each dispatch distinct) |
| Dispatching N | N alarms with same name coalesce to one | N dispatches run N times |
| On permanent failure | alarm is deleted | job is dead-lettered |

Use an **alarm** for a self-scheduled reminder/timer the actor owns (one per name). Use a **job** to dispatch background work, especially when each dispatch must run and failures must be recorded.

### Dispatching

```go
jobID, err := c.client.Dispatch(ctx, "send-email", payload)                  // to self, ASAP
jobID, err := service.Dispatch(ctx, "worker", "worker-7", "send-email", pl)  // to any actor
```

Job options (`actor.JobOption`):

| Option | Description |
|---|---|
| `WithJobDueTime(t)` | First run at an absolute time (default: ASAP). |
| `WithJobDelay(d)` | First run after a delay. Mutually exclusive with `WithJobDueTime`. |
| `WithJobInterval(iso8601)` | Repeat on an ISO-8601 / Go-duration interval. Mutually exclusive with cron. |
| `WithJobCron(expr)` | Repeat on a standard cron expression. Mutually exclusive with interval. |
| `WithJobTTL(t)` | Stop a repeating job after this time. |
| `WithIdempotencyKey(key)` | Dedup re-dispatch within the same actor (first wins). |

A server-issued `jobID` is always unique. An idempotency key is caller-supplied and scoped to the actor: two dispatches with the same key produce a single job, so retrying the dispatch itself doesn't duplicate work.

### Handling & failure

```go
func (w *Worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	// Returning an error retries per MaxAttempts/InitialRetryDelay, then dead-letters
	// Returning actor.ErrJobPermanentFailure skips retries and dead-letters immediately
	return nil
}

// Optional best-effort reaction after dead-lettering (dead-letter record is the source of truth)
func (w *Worker) JobFailed(ctx context.Context, jobID, method string, data actor.Envelope, jobErr error) error {
	return nil
}
```

For a *repeating* job, one terminally-failing occurrence is dead-lettered while later occurrences still run.

### Managing jobs

Available on both client (self-bound) and service (any actor):

```go
info, err := service.GetJob(ctx, jobID)              // spans live + dead-lettered; ErrJobNotFound if absent
jobs, err := service.ListJobs(ctx, "worker", "w-7")
err = service.CancelJob(ctx, "worker", "w-7", jobID) // cancel a live (pending/active) job
newID, err := service.RetryJob(ctx, jobID)           // re-dispatch a dead-lettered job, remove dead record
```

```go
type JobInfo struct {
	JobID, ActorType, ActorID, Method string
	Status    JobStatus  // JobStatusPending | JobStatusActive | JobStatusDeadLettered
	DueTime   time.Time
	Interval  string
	Cron      string
	Attempts  int        // populated once dead-lettered
	LastError string     // populated once dead-lettered
	CreatedAt time.Time
}
```

### Delivery semantics (alarms & jobs)

Durable (survive restarts), leased execution (never run by two hosts at once), activation-on-trigger, turn-based serialization (share the actor's single turn lock with `Invoke`), at-least-once with retries. **Design handlers to be idempotent.**

## Invoking actors

```go
resp, err := service.Invoke(ctx, "cart", "user-42", "addItem", data)  // data: any JSON value or nil
var out MyResp
if resp != nil {
	err = resp.Decode(&out)
}
```

`InvokeOption`:
- `actor.WithInvokeActiveOnly()` — invoke **only** if already active; otherwise returns `ErrActorNotActive` (does not activate).

Call `service.Peek(...)` (same args/options) to run the actor's `Peek` under the shared read lock; fails if the actor doesn't implement `ActorPeek`. You can invoke from any host — Francis routes to the owner.

## Streaming invocations

For large request/response bodies, stream bytes instead of buffering JSON.

Caller side — `service.InvokeStream` returns the response content type plus a reader you must close:

```go
respContentType, resp, err := service.InvokeStream(
	ctx, "report", "2026-q1", "render", "application/json", requestBody,
)
if err != nil {
	return err
}
defer resp.Close()
// read from resp...
```

Actor side — implement `ActorStream`; you read `body` and write the response to `w`, holding the turn lock for the whole call:

```go
type ActorStream interface {
	InvokeStream(ctx context.Context, method string, reqContentType string, body io.Reader, w StreamResponseWriter) error
}
// w.SetContentType(ct) before the first w.Write; the first Write flushes response metadata
```

The read-only analogue is `service.PeekStream` (caller) + `ActorPeekStream` (actor), which holds the shared read lock for the whole call so concurrent `PeekStream`s overlap. Host enforces a max request body size (`WithMaxRequestBodySize`).

## Halting

Deactivate immediately instead of waiting for the idle timeout. State survives; the next call reactivates.

- Inside an actor: `c.client.Halt()` — deferred, runs after the current invocation returns (avoids self-deadlock).
- Outside: `service.Halt(type, id)` (this host only; `ErrActorNotHosted` if not active here), `service.HaltAll()`, `service.HaltDeferred(type, id)` (non-blocking).

## Registration options

`RegisterActor(actorType, factory, opts...)`, `RegisterSingletonActor(...)`, and `RegisterBuiltInActor(b)` — all before `host.Run`. Options are functional (`local.With...` / `remote.With...`):

| Option | Default | Description |
|---|---|---|
| `WithIdleTimeout(d)` | `5m` | Idle time before deactivation. Negative disables the idle timeout. |
| `WithDeactivationTimeout(d)` | `5s` | Max time allowed for `Deactivate` to run. |
| `WithConcurrencyLimit(n)` | `0` (unlimited) | Max actors of this type active on one host. |
| `WithMaxAttempts(n)` | `3` | Max attempts when invoking or running an alarm/job. |
| `WithInitialRetryDelay(d)` | `2s` | Initial retry delay (with backoff). |
| `WithBootstrapData(any)` | — | Singletons only: payload delivered to `Bootstrap` as its `data` envelope. |

## Sentinel errors

Match with `errors.Is` (package `actor`):

| Error | Meaning |
|---|---|
| `ErrStateNotFound` | No state exists (service-level `GetState`/`DeleteState`). |
| `ErrAlarmNotFound` | Named alarm doesn't exist. |
| `ErrActorNotActive` | `WithInvokeActiveOnly()` used and the actor isn't active. |
| `ErrActorNotHosted` | `Halt` targeted an actor not active on the current host. |
| `ErrActorHalted` | Actor is halted where it was active; retry after a delay. |
| `ErrActorTypeUnsupported` | No host in the cluster serves this actor type. |
| `ErrActorTypeReserved` | Tried to target a `francis.builtin.*` type directly through a public client/service. |
| `ErrMethodReserved` | Tried to invoke a reserved framework method (e.g. bootstrap). |
| `ErrNoHost` | No host is available to place the actor. |
| `ErrJobNotFound` | Job not found. |
| `ErrJobPermanentFailure` | Return from `Job` to skip retries and dead-letter immediately. |
| `ErrReadOnly` | A mutating client call was made from within `Peek`/`PeekStream`. |
| `ErrServiceNotInitialized` | Service wasn't created via a host (`NewService`). |
