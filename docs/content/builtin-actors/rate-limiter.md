---
title: "Rate limiter"
weight: 20
description: "Throttle calls per key, consistently across the cluster"
---

A rate limiter actor throttles calls **per key**, a free-form string you choose (e.g. an IP address, user ID, route, API token, etc). Each key is limited independently, and its limiter state lives only in the activated actor's memory, for optimal performance.

It follows the token-bucket model, and `Allow` is a non-blocking check: it reports whether the call is admitted right now and, when it is not, how long the caller should wait before retrying.  
The returned wait can be used as a  `Retry-After` header on a `429 Too Many Requests` response.

## Registering

Build a rate limiter with `ratelimit.New` and pass it to the host:

```go
import "github.com/italypaleale/francis/builtin/ratelimit"

limiter, err := ratelimit.New("api",
	ratelimit.WithRate(100), // 100 calls per second, per key
)
if err != nil {
	return err
}

host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Register the built-in actor before calling host.Run
err = host.RegisterBuiltInActor(limiter)
```

As with any built-in actor, register the same rate limiter (same name and options) on every host that should serve it. A given key is always placed on a single host at a time, so its limiter is consistent cluster-wide.

## Options

`ratelimit.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithRate(n)` | Number of calls admitted per period. **Required**, must be greater than zero. |
| `WithPer(d)` | The window the rate applies over. Defaults to one second, so `WithRate(100)` alone is 100/s; combine with `WithPer(time.Minute)` for a per-minute rate. |
| `WithBurst(n)` | The token bucket's capacity: how many calls may be admitted instantly before throttling kicks in, refilling at the configured rate. Defaults to **1** (strict), so calls are admitted one at a time - raise it to tolerate short bursts above the steady rate. |
| `WithIdleTimeout(d)` | How long a key's in-memory limiter is kept after its last call before the actor is deactivated. Defaults to double the period (the `WithPer` window), with a minimum of one minute. Lower it to reclaim memory faster when limiting many distinct keys. |

## Throttling by key

The `Allow` operation is bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
rl := limiter.Service(host.Service())

// Non-blocking: reports whether this key may proceed under the configured rate
allowed, retryAfter, err := rl.Allow(ctx, clientIP)
if err != nil {
	// The key was invalid or the invocation failed (e.g. ctx was cancelled)
	return err
}
if !allowed {
	// Throttled: retryAfter is how long until the key admits another call
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}
// ... handle the request ...
```

`Allow` never blocks. When `allowed` is `false`, `retryAfter` tells the caller how long to wait before the key admits another call (it is zero when `allowed` is `true`).  
The returned `error` is non-nil only when the key is invalid or the underlying actor invocation fails, including context cancellation - it never signals throttling.
