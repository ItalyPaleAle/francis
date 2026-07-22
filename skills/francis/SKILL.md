---
name: francis
description: How to build apps with the Francis distributed-actor framework (Go). Covers virtual actors, durable state, alarms, jobs, cron jobs, rate limiters, singleton actors, and host/topology setup. Use this whenever the user is writing or modifying Francis actors, wiring up a host, adding durable state/alarms/jobs, using the built-in cron or rate-limiter actors, choosing between local and remote topologies, or asks how to do any of these — even if they don't name "Francis" but are clearly working in this repo (imports of github.com/italypaleale/francis, actor.Client, RegisterActor, etc.).
---

# Francis actor framework

Francis is a Go framework for **distributed actors** (a.k.a. virtual actors / durable objects). An actor is a Go struct addressed by `(type, id)`; Francis activates it on demand, routes calls to the single activation of it that exists cluster-wide, persists its state, and schedules its alarms/jobs. The only hard dependency is a relational database (SQLite or PostgreSQL).

Use this skill to write actors and wire up a host correctly. Prefer these patterns over inventing your own — the framework has strong opinions (turn-based concurrency, durable state, at-least-once delivery) that the patterns below respect.

## Mental model — internalize these first

- **Virtual & single-activation**: you never `new` an actor. You invoke `("cart", "user-42", ...)` and Francis activates it somewhere if needed. At any instant a given `(type, id)` is active on **at most one host**.
- **Turn-based concurrency**: an actor handles **one invocation at a time**. You never need locks for the actor's own fields or state. `Peek` is the only exception (see patterns).
- **Durable state, ephemeral activation**: the struct is recreated on every activation. Anything that must survive goes in the actor's persisted **state**; struct fields are per-activation scratch space.
- **Location transparency**: call from any host; Francis forwards to the owner over mTLS. Callers never know where an actor lives.
- **Deactivation can be abrupt**: after an idle timeout / halt / shutdown the actor deactivates (`Deactivate` runs if implemented). On a crash it may vanish **without** `Deactivate`. Never rely on `Deactivate` to persist critical data — persist eagerly with `SetState`.

## The three things every actor needs

```go
// 1. A JSON-serializable state struct (exported fields only)
type cartState struct {
	Items []string
}

// 2. The actor struct, holding a typed client bound to this actor
type Cart struct {
	client actor.Client[cartState]
	log    *slog.Logger
}

// 3. A factory Francis calls to activate the actor
func NewCart(actorID string, service *actor.Service) actor.Actor {
	return &Cart{
		client: actor.NewActorClient[cartState]("cart", actorID, service),
	}
}
```

The actor then implements one or more optional interfaces (`Invoke`, `Peek`, `Alarm`, `Job`, `Bootstrap`, `Deactivate`, `JobFailed`). Implement only what you need.

## Minimal working example (local topology, SQLite)

Actor:

```go
func (c *Cart) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	// Load state: the first time, GetState returns the zero value (not an error)
	state, err := c.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	switch method {
	case "addItem":
		// Decode the request payload only if one was sent
		var req struct{ Item string }
		if data != nil {
			err = data.Decode(&req)
			if err != nil {
				return nil, err
			}
		}
		state.Items = append(state.Items, req.Item)
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}

	// State is durable only after SetState returns
	// Mutating the struct alone persists nothing
	err = c.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, err
	}
	return len(state.Items), nil
}
```

Host + registration + invocation:

```go
h, err := local.NewHost(
	local.WithAddress("127.0.0.1:7571"),
	local.WithSQLiteProvider(local.SQLiteProviderOptions{ConnectionString: "data.db"}),
	// The runtime PSK derives the cluster CA used for host-to-host mTLS; use a strong secret in prod
	local.WithRuntimePSKs([]byte("change-me-please")),
)
// Register every actor type BEFORE Run
err = h.RegisterActor("cart", NewCart, local.WithIdleTimeout(10*time.Minute))

service := h.Service()
// Invoke from anywhere (e.g. an HTTP handler). data is any JSON-serializable value or nil
resp, err := service.Invoke(ctx, "cart", "user-42", "addItem", map[string]any{"Item": "book"})
var count int
if resp != nil {
	err = resp.Decode(&count)
}

// Run blocks until ctx is cancelled and the host drains
err = h.Run(ctx)
```

Key rules: register **before** `Run`; the factory's `actorType` string, the `RegisterActor` string, and the invoke string must all match; `data` in/out is JSON, so use exported fields.

## API cheat-sheet

**Interfaces an actor may implement** (all optional, package `actor`):

| Interface | Method | Purpose |
|---|---|---|
| `ActorInvoke` | `Invoke(ctx, method, data) (any, error)` | Handle a call (exclusive turn) |
| `ActorPeek` | `Peek(ctx, method, data) (any, error)` | Read-only call, runs concurrently with other Peeks |
| `ActorAlarm` | `Alarm(ctx, name, data) error` | Handle a fired alarm |
| `ActorJob` | `Job(ctx, method, data) error` | Handle a dispatched background job |
| `ActorJobFailed` | `JobFailed(ctx, jobID, method, data, jobErr) error` | React after a job is dead-lettered (best-effort) |
| `ActorBootstrapper` | `Bootstrap(ctx, data) error` | One-time startup step (singletons) — must be idempotent |
| `ActorDeactivate` | `Deactivate(ctx) error` | Cleanup before deactivation (keep it fast; not guaranteed on crash) |

**`actor.Client[T]`** (bound to the current actor; get via `NewActorClient[T]`): `GetState`, `SetState`, `DeleteState`, `SetAlarm`, `DeleteAlarm`, `Dispatch` (jobs), `GetJob`/`ListJobs`/`CancelJob`/`RetryJob`, `Invoke`/`Peek` (call other actors), `Halt()`.

**`actor.Service`** (from `host.Service()`, also passed to factories): the same operations but taking `(actorType, actorID)` explicitly — use it from outside actors (HTTP handlers) or to touch a *different* actor. `Invoke`, `Peek`, `InvokeStream`, `SetState`/`GetState`/`DeleteState`, `SetAlarm`/`DeleteAlarm`, `Dispatch`, job management, `Halt`/`HaltAll`/`HaltDeferred`.

`data.Decode(&dest)` reads the request/alarm/job payload (may be `nil`). A method's `return` value is JSON-serialized back to the caller; return `nil` for no response.

For full signatures, options tables, streaming, and the sentinel errors table, read `references/api-reference.md`.

## Common patterns

### 1. Stateful entity (the default)
One actor per entity (user, cart, device, game session). `Invoke` loads state, mutates, `SetState`s. See the minimal example above. The client caches state in memory for the activation, so repeated `GetState` calls in one turn don't re-hit the DB. Give ephemeral entities a **state TTL** so they self-expire: `c.client.SetState(ctx, state, &actor.SetStateOpts{TTL: 24*time.Hour})`.

### 2. Read-heavy actor → add `Peek`
`Invoke` is an exclusive (write) lock; `Peek` is a shared (read) lock. Many `Peek`s run at once, but never overlap an `Invoke`. Implement `Peek` for hot reads, and call `service.Peek(...)` instead of `service.Invoke(...)`.

```go
func (c *Cart) Peek(ctx context.Context, method string, data actor.Envelope) (any, error) {
	state, err := c.client.GetState(ctx)   // GetState is allowed in Peek
	if err != nil {
		return nil, err
	}
	return len(state.Items), nil
}
```

**Rule:** `Peek` must not mutate anything. State-mutating client calls (`SetState`, `DeleteState`, `SetAlarm`, `DeleteAlarm`, `Dispatch`) return `actor.ErrReadOnly` from within `Peek`. The framework can't stop you mutating your own struct fields, but doing so is a data race across concurrent Peeks — treat the actor as read-only in your own code too.

### 3. Self-scheduled reminder / timeout → **alarm**
An alarm is a durable callback the actor schedules for itself. Setting an alarm with an existing name **replaces** it, so it's perfect for "reset the deadline" logic. Handlers must be **idempotent** (at-least-once delivery).

```go
// In Invoke: remind the user in 24h; re-adding with the same name resets the timer
err := c.client.SetAlarm(ctx, "abandon-reminder", actor.AlarmProperties{
	DueTime: time.Now().Add(24 * time.Hour),
})

// Repeating: fire now, then every 24h, stop after 30 days
err := c.client.SetAlarm(ctx, "daily-rollup", actor.AlarmProperties{
	DueTime:  time.Now(),
	Interval: "24h",                               // Go duration, ISO-8601, or ms
	TTL:      time.Now().Add(30 * 24 * time.Hour), // optional stop time
})

func (c *Cart) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	switch name {
	case "abandon-reminder":
		// ... notify; keep idempotent ...
	}
	return nil
}
```
Cancel with `c.client.DeleteAlarm(ctx, "abandon-reminder")` (returns `actor.ErrAlarmNotFound` if absent).

### 4. Background work dispatched to an actor → **job**
Use a job (not an alarm) when you need each dispatch to run and failures to be recorded rather than dropped. N dispatches run N times (alarms with the same name coalesce to one). On permanent failure a job is **dead-lettered**, not lost.

```go
// Dispatch to self (from inside) — returns a unique jobID
jobID, err := c.client.Dispatch(ctx, "send-email", map[string]any{"to": addr})

// Dispatch to any actor (from outside)
jobID, err := service.Dispatch(ctx, "worker", "worker-7", "send-email", payload)

// Scheduling & dedup options
_, err = c.client.Dispatch(ctx, "reconcile", payload, actor.WithJobDelay(5*time.Minute))
_, err = c.client.Dispatch(ctx, "daily-report", nil, actor.WithJobCron("0 9 * * 1-5"))
_, err = c.client.Dispatch(ctx, "charge", payload, actor.WithIdempotencyKey("order-42")) // first wins

func (w *Worker) Job(ctx context.Context, method string, data actor.Envelope) error {
	// Return an error to retry (per MaxAttempts); return actor.ErrJobPermanentFailure to dead-letter now
	return nil
}
```
Choosing: **alarm** = self-owned reminder/timer, one per name. **job** = dispatched background work, each runs, failures recorded. See `references/api-reference.md` for the full jobs vs alarms table and dead-letter/replay APIs.

### 5. Recurring work that runs once cluster-wide → **cron job** (built-in actor)
Don't hand-roll cluster-wide scheduling. Use the built-in cron job: it's a cluster-wide singleton backed by one durable repeating job, so exactly one host runs each occurrence.

```go
import "github.com/italypaleale/francis/builtin/cronjob"

job, err := cronjob.New("nightly-cleanup",
	cronjob.WithCron("0 2 * * *"),        // or WithInterval(d) / WithPeriod("PT5M")
	cronjob.WithJob(func(ctx context.Context) error { /* runs once cluster-wide */ return nil }),
)
err = h.RegisterBuiltInActor(job)          // before Run; register the SAME job on every host
// On demand: job.Service(h.Service()).Trigger(ctx)
```

### 6. Throttle by key → **rate limiter** (built-in actor)
Token-bucket limiter, one independent bucket per free-form key (IP, user, route…). `Allow` is non-blocking.

```go
import "github.com/italypaleale/francis/builtin/ratelimit"

limiter, err := ratelimit.New("api", ratelimit.WithRate(100), ratelimit.WithBurst(20))
err = h.RegisterBuiltInActor(limiter)
// In a handler:
rl := limiter.Service(h.Service())
allowed, retryAfter, err := rl.Allow(ctx, clientIP)   // set Retry-After from retryAfter on a 429
```

See `references/builtin-and-singletons.md` for all cron/rate-limiter options.

### 7. Exactly one instance cluster-wide → **singleton actor**
For cluster-wide coordination or a single shared unit of state. Reached at the fixed ID `actor.SingletonActorID`.

```go
err := h.RegisterSingletonActor("scheduler", NewScheduler)
// Bootstrap runs once at startup on the owning host; every host triggers it, so it MUST be idempotent
func (s *scheduler) Bootstrap(ctx context.Context, data actor.Envelope) error { /* reconcile, don't double-create */ return nil }
// Invoke it from anywhere:
_, err = service.Invoke(ctx, "scheduler", actor.SingletonActorID, "someMethod", nil)
```

### 8. Actor-to-actor calls
The factory hands you `*actor.Service`, so an actor can call others: `service.Invoke(ctx, "other", id, method, data)` (or `client.Invoke(...)`). **Avoid re-entrancy** — A→B→A in the same call chain deadlocks, because each actor serves one turn at a time. Never invoke/peek your own `(type, id)` from within its own turn.

### 9. Halting
Deactivate immediately instead of waiting for the idle timeout. From inside: `c.client.Halt()` (deferred — runs after the current invocation returns, avoiding self-deadlock). From outside: `service.Halt(type, id)`, `service.HaltAll()`, `service.HaltDeferred(type, id)`. A halted actor just hibernates; state stays and the next call reactivates it.

## Non-negotiable rules (get these wrong and it breaks subtly)

- Register all actor types **before** `host.Run`.
- State must be JSON-serializable with **exported fields**; keep it small (store big blobs elsewhere, keep a reference).
- **Alarm and job handlers must be idempotent** — delivery is at-least-once with retries.
- **`Bootstrap` must be idempotent** — every host triggers it at startup.
- Don't rely on `Deactivate` for critical persistence; it may be skipped on a crash.
- No re-entrancy; don't call your own actor from your own turn.
- In `Peek`, treat everything as read-only; mutating client calls return `ErrReadOnly`.
- Multi-node clusters need a shared DB (PostgreSQL) — a SQLite file is effectively single-node.

## Choosing a topology

Actor code is **identical** across topologies; only host construction changes.
- **Local** (`host/local`): DB embedded in each app; hosts coordinate peer-to-peer; no separate control plane. Best for single-node, dev, and small clusters (≤4).
- **Remote** (`host/remote`): a standalone `runtime` owns the DB and coordination; workers are stateless. Best for larger/auto-scaling clusters.

Full setup (both topologies, providers, PSKs, security, remote worker) is in `references/hosts-and-topologies.md`.

## Reference files (read as needed)

- `references/api-reference.md` — full interface/method signatures, registration & invoke & job & alarm option tables, state TTL, jobs-vs-alarms, dead-letter/replay, streaming, and the sentinel error table.
- `references/hosts-and-topologies.md` — local vs remote host construction, provider options (SQLite/PostgreSQL/in-memory), PSKs and mTLS, running the standalone runtime.
- `references/builtin-and-singletons.md` — cron job & rate limiter options and behavior, singleton actors and the `Bootstrap` lifecycle.

## Where things live in this repo

The framework source: `actor/` (public API — `client.go`, `service.go`, `types.go`, `actor.go`, `jobs.go`), `host/local/` & `host/remote/` (hosts), `builtin/cronjob/` & `builtin/ratelimit/`. Runnable samples: `examples/worker` (local, all interfaces), `examples/remote-worker` (remote), `examples/cronjob`, `examples/ratelimit`. Docs: `docs/content/docs/`.
