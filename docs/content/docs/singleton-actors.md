---
title: "Singleton actors"
weight: 28
---

Singleton actors give you exactly one instance of an actor cluster-wide, with a one-time setup step that runs when hosts become ready. Use them when you need shared cluster-wide coordination, such as owning a single, cluster-wide unit of persistent state.

A singleton actor is always reached at the well-known ID `actor.SingletonActorID`, so callers from every host target the same instance.

## Registering

Register a singleton with `RegisterSingletonActor` instead of `RegisterActor`, before the host starts. The host exposes this method on both the local and remote host types, and it can be called multiple times for several singletons.

```go
import (
    "context"

    "github.com/italypaleale/francis/actor"
    "github.com/italypaleale/francis/host/local"
)

type scheduler struct {
    client actor.Client[struct{}]
}

func NewScheduler(actorID string, svc *actor.Service) actor.Actor {
    return &scheduler{
        client: actor.NewActorClient[struct{}]("scheduler", actorID, svc),
    }
}

func (s *scheduler) Bootstrap(ctx context.Context, data actor.Envelope) error {
    // Runs once per startup cycle on the single owning host, serialized by the singleton's turn lock
    // Must be idempotent: every host triggers it, so reconcile rather than double-create durable work
    // When BootstrapData was set at registration, it arrives here as an Envelope: call `data.Decode(&dest)`
    return nil
}

host, err := local.NewHost(/* ... options ... */)
if err != nil {
    return err
}

err = host.RegisterSingletonActor("scheduler", NewScheduler, local.RegisterSingletonActorOptions{})
```

`RegisterSingletonActorOptions` embeds the regular `RegisterActorOptions` and adds an optional bootstrap payload:

| Field | Type | Description |
|-------|------|-------------|
| `IdleTimeout` | `time.Duration` | From `RegisterActorOptions`: how long the singleton can stay idle before deactivation. Defaults to `5m`. |
| `DeactivationTimeout` | `time.Duration` | From `RegisterActorOptions`. |
| `ConcurrencyLimit` | `int` | From `RegisterActorOptions`. |
| `MaxAttempts` | `int` | From `RegisterActorOptions`. |
| `InitialRetryDelay` | `time.Duration` | From `RegisterActorOptions`. |
| `BootstrapData` | `any` | Optional data delivered to `Bootstrap` as its `data Envelope` argument, like `Invoke`. Encoded by the host and decoded by the actor via `data.Decode(&dest)`. `nil` when not provided. |

```go
err = host.RegisterSingletonActor("scheduler", NewScheduler, local.RegisterSingletonActorOptions{
    RegisterActorOptions: local.RegisterActorOptions{
        IdleTimeout: 10 * time.Minute,
    },
    BootstrapData: map[string]string{"env": "prod"},
})
```

## The `ActorBootstrapper` interface

Implement `actor.ActorBootstrapper` on your actor to receive the startup call:

```go
// ActorBootstrapper is called once the host is ready, on the singleton instance
type ActorBootstrapper interface {
    Bootstrap(ctx context.Context, data actor.Envelope) error
}
```

Details:

- The host invokes `Bootstrap` on the `actor.SingletonActorID` instance after it becomes ready and can serve invocations.
- Under the hood, the call is a normal actor invocation and behaves in the same way: it is routed to the current owning host and serialized by the singleton instance's turn lock.
- Every host triggers bootstrapping at startup, so `Bootstrap` **must be idempotent**. If it registers durable work, check whether that work already exists first.
- An actor registered as a singleton that does not implement `ActorBootstrapper` simply has no startup step.
- The bootstrap payload arrives as `data actor.Envelope`, just like `Invoke` and `Alarm`. Call `data.Decode(&dest)` to read it. `data` is `nil` when no bootstrap data was supplied at registration.

```go
func (s *scheduler) Bootstrap(ctx context.Context, data actor.Envelope) error {
    if data != nil {
        var cfg schedulerConfig
        err := data.Decode(&cfg)
        if err != nil {
            return err
        }

        // use cfg...
    }

    // idempotent registration...
    return nil
}
```

## Invoking a singleton

A singleton is just an actor with a fixed well-known ID. Invoke it like any other actor, but target `actor.SingletonActorID`:

```go
_, err := host.Service().Invoke(ctx, "scheduler", actor.SingletonActorID, "someMethod", nil)
```

Because placement pins the type+ID pair to a single host at a time, all callers across the cluster end up at the same instance without needing to know which host owns it.

## Lifecycle notes

- The reserved `Bootstrap` lifecycle is framework-driven. Clients cannot invoke `francis.builtin.bootstrap` directly, and the public client and service reject reserved method names (`francis.builtin.*`).
- Like other actors, a singleton deactivates after its idle timeout and is reactivated on the next invocation. `Bootstrap` only runs at host startup (bootstrapped from each host that registered the singleton), not on every activation.
