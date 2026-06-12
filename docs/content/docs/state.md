---
title: "Actor state"
weight: 25
---

Every actor has its own **durable state**: a Go value that Francis serializes to JSON and stores in the database, keyed by the actor's type and ID. State is independent of whether the actor is currently active — it survives deactivation, process restarts, and an actor moving between hosts.

## Working with state through the client

The most convenient way to manage an actor's own state is the typed `actor.Client[T]`, where `T` is the type of your state. You create one in the factory and keep it on the actor struct:

```go
type cartState struct {
	Items []string
}

type Cart struct {
	client actor.Client[cartState]
}

func NewCart(actorID string, service *actor.Service) actor.Actor {
	return &Cart{
		client: actor.NewActorClient[cartState]("cart", actorID, service),
	}
}
```

### Reading state

```go
state, err := c.client.GetState(ctx)
```

`GetState` returns the actor's state as a typed `T`. If the actor has no stored state yet, it returns the **zero value** of `T` (not an error), so you can treat "first time" and "existing" uniformly.

The client caches the state in memory for the lifetime of the activation. Because an actor handles one invocation at a time, this cache is always consistent: repeated `GetState` calls within an activation don't re-read the database.

### Writing state

```go
state.Items = append(state.Items, "book")
err := c.client.SetState(ctx, state, nil)
```

`SetState` persists the value and updates the in-memory cache. State is only durable once `SetState` returns successfully — mutating the struct alone does not persist anything.

### Deleting state

```go
err := c.client.DeleteState(ctx)
```

`DeleteState` removes the stored state. After deletion, `GetState` again returns the zero value.

## State TTL

You can give state a **time-to-live** so it expires automatically. Pass `SetStateOpts` to `SetState`:

```go
err := c.client.SetState(ctx, state, &actor.SetStateOpts{
	TTL: 24 * time.Hour,
})
```

After the TTL elapses, the state is treated as absent (and is eventually purged by the provider's cleanup). This is useful for ephemeral actors whose state should not linger indefinitely. Pass `nil` (or a zero `TTL`) for state that never expires.

## Accessing state through the service

The typed client is built on top of `actor.Service`, which exposes the same operations for any actor:

```go
// dest is decoded from the stored JSON
var state cartState
err := service.GetState(ctx, "cart", "user-42", &state)

err = service.SetState(ctx, "cart", "user-42", state, nil)
err = service.DeleteState(ctx, "cart", "user-42")
```

At the service level, `GetState` and `DeleteState` return `actor.ErrStateNotFound` when no state exists; the typed client smooths this over by returning a zero value instead. Use the service form when you need to manage another actor's state, or when you want to distinguish "no state" from "zero state".

## How state is stored

State is serialized to JSON, so your state type must be JSON-serializable. A few practical notes:

- Use exported fields; unexported fields are not serialized.
- Keep state reasonably small — it's read and written as a single value per actor.
- The storage backend is your configured [provider](/docs/topologies): SQLite or PostgreSQL.

Because state is durable and single-activation guarantees one writer at a time, you generally don't need optimistic concurrency or your own locking for an actor's own state.
