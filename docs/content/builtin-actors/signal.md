---
title: "Signal"
weight: 40
description: "Broadcast a one-shot notification to many waiting callers"
---

A signal actor broadcasts a **one-shot notification** to any number of waiting callers, addressed by a **signal ID**: a free-form string you choose (e.g. a deployment ID, a job ID, a request ID).

Callers block in `Wait` until the signal fires. One caller fires it with `Complete`, optionally attaching a payload, then every waiter is released at that moment with that payload. A caller that arrives *after* the completion does not block: it is answered from the durable completion record.

Because of that, a client that disconnects and calls `Wait` again is answered correctly whether the signal fired while it was away or has yet to fire.

## Registering

Build a signal set with `signal.New` and pass it to the host:

```go
import (
	"github.com/italypaleale/francis/builtin/signal"
)

deploys, err := signal.New("deploys",
	signal.WithRetention(24*time.Hour),
)
if err != nil {
	return err
}

host, err := local.NewHost(/* ... options ... */)
if err != nil {
	return err
}

// Register the built-in actor before calling host.Run
err = host.RegisterBuiltInActor(deploys)
```

As with any built-in actor, register the same signal set (same name and options) on every host that should be able to wait on or complete its signals.

## Options

`signal.New(name, opts...)` takes a unique `name` (used to build the reserved actor type, and must not contain `/`) and these options:

| Option | Description |
|--------|-------------|
| `WithRetention(d)` | How long a completed signal's payload is kept, which is the window during which a late `Wait` returns immediately. Defaults to **24 hours**. A negative value keeps completions forever. See [Retention](#retention) below. |
| `WithIdleTimeout(d)` | How long a signal's actor is kept in memory after its last call before it is deactivated. Defaults to 5 minutes. A signal with callers parked on it is never deactivated, however long they wait. |
| `WithMaxPayloadSize(n)` | Caps the completion payload, in bytes, after encoding. Defaults to **64 KiB**. The payload is stored in the signal's state and returned to every waiter. |

## Waiting and completing

The operations are bound to an `actor.Service` via `Service(...)`, which you obtain from a host with `host.Service()`:

```go
sig := deploys.Service(host.Service())

// Blocks until the deployment finishes, however long that takes
env, err := sig.Wait(ctx, deploymentID)
if err != nil {
	// The signal never fired before ctx was done
	return err
}

var result DeployResult
if env != nil {
	err = env.Decode(&result)
	if err != nil {
		return err
	}
}
```

Firing the signal returns as soon as the completion is durable, and releases every waiter across the cluster:

```go
err := sig.Complete(ctx, deploymentID, DeployResult{Version: "v2"})
if errors.Is(err, signal.ErrAlreadyCompleted) {
	// Somebody else got there first, and their payload is the one waiters received
	err = nil
}
```

Pass `nil` as the payload for a signal that only needs to say "it happened". In this case, waiters receive a `nil` envelope.

`Check` is the non-blocking version of `Wait`, for callers that want to decide for themselves whether to wait:

```go
env, completed, err := sig.Check(ctx, deploymentID)
```

Bound the wait with a context deadline when a caller should give up after a while. By itself, `Wait` never times out.

## Retention

Retention is the promise that within the window, a `Wait` arriving after the completion returns immediately and correctly.

Once the window passes, the completion record is gone, and the signal becomes indistinguishable from one that never fired: a `Wait` arriving after that blocks, and `Check` reports it as not completed.

Set retention longer than the longest lateness you expect from any caller. If callers may arrive arbitrarily late, use `WithRetention(-1)` to keep completions forever, and garbage-collect the signal IDs you no longer need yourself, out of band.
