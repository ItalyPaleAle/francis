# Built-in actors & singletons

Built-in actors are framework-managed actors registered under a reserved `francis.builtin.*` type. Register them with `host.RegisterBuiltInActor(...)` **before** `Run` (works on local and remote hosts). Clients **cannot** target them directly — you drive them through the small typed service each returns (`x.Service(host.Service())`).

Register the **same** built-in actor (same name and options) on **every** host that should serve it. A given key/schedule is placed on a single host at a time, so behavior stays consistent cluster-wide.

## Cron job

Runs a function on a schedule, once cluster-wide per occurrence (a cluster-wide singleton backed by one durable repeating job; each occurrence is leased so only one host runs it).

```go
import "github.com/italypaleale/francis/builtin/cronjob"

job, err := cronjob.New("nightly-cleanup",
	cronjob.WithCron("0 2 * * *"),
	cronjob.WithJob(func(ctx context.Context) error {
		// runs once across the cluster, every night at 2am
		return nil
	}),
)
if err != nil {
	return err
}
err = host.RegisterBuiltInActor(job)   // before host.Run
```

### Options

`cronjob.New(name, opts...)` — `name` builds the reserved type and must not contain `/`.

| Option | Description |
|---|---|
| `WithJob(fn)` | Function to run each occurrence. **Required.** |
| `WithInterval(d)` | Repeat every `time.Duration` `d`. |
| `WithPeriod(iso8601)` | Repeat on an ISO-8601 duration string, e.g. `"PT5M"`, `"P1D"`. |
| `WithCron(expr)` | Repeat on a standard cron expression, e.g. `"0 9 * * 1-5"`. |
| `WithImmediate()` | Also run once right away, but only the first time it is registered. |

Exactly one of `WithInterval` / `WithPeriod` / `WithCron` is required, plus `WithJob`. Without `WithImmediate`, the first run happens after one interval (or at the next cron tick).

### How it works

At startup each host bootstraps the cron scheduler (the cluster-wide singleton). If the schedule is already registered, bootstrapping is a no-op — so it's safe for every host to register the same job, and it survives restarts. Otherwise it dispatches the repeating job that drives the schedule. Concurrent registrations from multiple hosts collapse to a single recurring job.

### On-demand & teardown

```go
svc := job.Service(host.Service())
err := svc.Trigger(ctx)     // run once now, regardless of schedule; returns promptly (runs on the runner)
err = svc.Unregister(ctx)   // cancel the recurring job and clear state; a later startup re-registers cleanly
```

Multiple triggers that pile up while a run is pending collapse into a single run.

## Rate limiter

Token-bucket throttle, one independent bucket per free-form key (IP, user ID, route, API token…). Per-key limiter state lives only in the activated actor's memory. `Allow` is non-blocking.

```go
import "github.com/italypaleale/francis/builtin/ratelimit"

limiter, err := ratelimit.New("api",
	ratelimit.WithRate(100),          // 100 calls per period (per key)
	ratelimit.WithPer(time.Minute),   // window; default 1s
	ratelimit.WithBurst(20),          // bucket capacity; default 1 (strict)
)
if err != nil {
	return err
}
err = host.RegisterBuiltInActor(limiter)   // before host.Run
```

### Options

`ratelimit.New(name, opts...)` — `name` builds the reserved type and must not contain `/`.

| Option | Description |
|---|---|
| `WithRate(n)` | Calls admitted per period. **Required, > 0.** |
| `WithPer(d)` | Window the rate applies over. Default 1s (so `WithRate(100)` alone = 100/s). |
| `WithBurst(n)` | Bucket capacity: how many calls may be admitted instantly before throttling. Default 1 (strict). Raise to tolerate short bursts. |
| `WithIdleTimeout(d)` | How long a key's in-memory limiter is kept after its last call. Default = 2× the period, min 1 minute. Lower it to reclaim memory when limiting many keys. |

### Using it

```go
rl := limiter.Service(host.Service())

allowed, retryAfter, err := rl.Allow(ctx, clientIP)
if err != nil {
	// key was invalid or the invocation failed (e.g. ctx cancelled) — never signals throttling
	return err
}
if !allowed {
	// retryAfter = how long until this key admits another call
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}
// ... handle the request ...
```

`Allow` never blocks; `retryAfter` is zero when `allowed` is true. The `error` is non-nil only for an invalid key or a failed invocation (including context cancellation) — not for throttling.

## Singleton actors

Exactly one instance cluster-wide, with a one-time setup step at startup. Use for cluster-wide coordination or a single shared unit of persistent state. Reached at the fixed ID `actor.SingletonActorID`.

Register with `RegisterSingletonActor` (before `Run`), which reuses the same options as `RegisterActor` plus `WithBootstrapData`:

```go
type scheduler struct {
	client actor.Client[schedulerState]
}

func NewScheduler(actorID string, svc *actor.Service) actor.Actor {
	return &scheduler{client: actor.NewActorClient[schedulerState]("scheduler", actorID, svc)}
}

err := host.RegisterSingletonActor("scheduler", NewScheduler,
	local.WithIdleTimeout(10*time.Minute),
	local.WithBootstrapData(map[string]string{"env": "prod"}),  // optional; arrives in Bootstrap's data
)
```

### The `Bootstrap` lifecycle

```go
func (s *scheduler) Bootstrap(ctx context.Context, data actor.Envelope) error {
	// Runs at host startup on the single owning host, serialized by the singleton's turn lock
	// Every host triggers it, so it MUST be idempotent: reconcile, don't double-create durable work
	if data != nil {
		var cfg schedulerConfig
		err := data.Decode(&cfg)
		if err != nil {
			return err
		}
		// use cfg...
	}
	// e.g. register a durable recurring job only if one is not already set up
	return nil
}
```

- The call is a normal invocation routed through placement; only the owning host runs it, serialized by the instance's turn lock.
- `Bootstrap` runs at **host startup**, not on every activation. After idle timeout the singleton deactivates and reactivates on the next invocation without re-running `Bootstrap`.
- An actor registered as a singleton that doesn't implement `ActorBootstrapper` simply has no startup step.
- Clients cannot invoke `francis.builtin.bootstrap` or other reserved `francis.builtin.*` methods directly.

### Invoking

```go
_, err := host.Service().Invoke(ctx, "scheduler", actor.SingletonActorID, "someMethod", nil)
```

All callers across the cluster reach the same instance without knowing which host owns it.
