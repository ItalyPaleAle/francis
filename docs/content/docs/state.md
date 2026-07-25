---
title: "Actor state"
weight: 25
---

Every actor has its own **durable state**: a Go value that Francis stores in the database (internally, serialized to JSON), keyed by the actor's type and ID. State is independent of whether the actor is currently active: it survives deactivation, process restarts, and an actor moving between hosts.

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

`GetState` returns the actor's state as a typed `T`. If the actor has no stored state yet, it returns the zero value of `T` (not an error), so you can treat "first time" and "existing" uniformly.

The client caches the state in memory for the lifetime of the activation. Because an actor handles one invocation at a time, this cache is always consistent: repeated `GetState` calls within an activation don't re-read the database.

### Writing state

```go
state.Items = append(state.Items, "book")
err := c.client.SetState(ctx, state, nil)
```

`SetState` persists the value and updates the in-memory cache. State is only durable once `SetState` returns successfully: mutating the struct alone does not persist anything.

### Deleting state

```go
err := c.client.DeleteState(ctx)
```

`DeleteState` removes the stored state. After deletion, `GetState` again returns the zero value.

## State TTL

You can give state a **time-to-live** (TTL) so it expires automatically. Pass `SetStateOpts` to `SetState`:

```go
err := c.client.SetState(ctx, state, &actor.SetStateOpts{
	TTL: 24 * time.Hour,
})
```

After the TTL elapses, the state is treated as absent and is eventually purged by the provider's cleanup. This is useful for ephemeral actors whose state should not linger indefinitely (for example: user sessions, shopping carts…). Pass `nil` (or a zero `TTL`) for state that never expires (state has no TTL by default).

## Listing the actors that have state

`ListStates` returns the actors of a given type that have state stored in the database. This is how you enumerate the actors your application knows about: this listing covers every actor that has persisted state, whether or not it is active right now. Listing actor states does not re-activate actors that are idle.

```go
list, err := c.client.ListStates(ctx, nil)
for _, s := range list.States {
	fmt.Println(s.ActorID)
}
```

Through the typed client, the listing is always scoped to the current actor's own type. It is a read, so it's also allowed inside a `Peek` invocation.

By default only the actor IDs are returned, which keeps the query cheap when you don't need the payloads. Set `IncludeData` to also get each actor's state, decoded into `T`:

```go
list, err := c.client.ListStates(ctx, &actor.ListStatesOpts{IncludeData: true})
for _, s := range list.States {
	fmt.Println(s.ActorID, len(s.Data.Items))
}
```

Actors whose stored state is empty come back with the zero value of `T`.

### Pagination

Results are ordered by actor ID and returned one page at a time. `Limit` sets the page size (capped by the provider, and defaulting to 100 when unset), `HasMore` reports whether more actors follow, and `After` resumes the listing from a given actor ID.

`AfterID` is a convenience method giving you the cursor for the next page. It returns an empty string once there are no more pages, so a full walk of the collection is a loop that ends when the cursor runs out:

```go
opts := actor.ListStatesOpts{Limit: 50}
for {
	list, err := c.client.ListStates(ctx, &opts)
	if err != nil {
		return err
	}

	for _, s := range list.States {
		fmt.Println(s.ActorID)
	}

	opts.After = list.AfterID()
	if opts.After == "" {
		break
	}
}
```

Because the cursor is an actor ID rather than an offset, actors added or removed while you are paging don't shift the pages you have not read yet.

State that has expired through its TTL is never listed, even before the provider's cleanup has removed it.

## Accessing state through the service

The typed client is built on top of `actor.Service`, which exposes the same operations for any actor:

```go
// dest is decoded from the stored JSON
var state cartState
err := service.GetState(ctx, "cart", "user-42", &state)

err = service.SetState(ctx, "cart", "user-42", state, nil)
err = service.DeleteState(ctx, "cart", "user-42")

list, err := service.ListStates(ctx, "cart", &actor.ListStatesOpts{IncludeData: true})
```

At the service level, `GetState` and `DeleteState` return `actor.ErrStateNotFound` when no state exists. The typed client smooths this over by returning a zero value instead. Use the service form when you need to manage another actor's state, or when you want to distinguish "no state" from "zero state".

`ListStates` takes the actor type to list, and takes the same options as the client's. Since the service isn't bound to a state type, each listed actor carries its state as an `actor.Envelope` you decode yourself (and which is `nil` when the data wasn't requested):

```go
for _, s := range list.States {
	var state cartState
	err = s.Data.Decode(&state)
}
```

## How state is stored

State is serialized to JSON, so your state type must be JSON-serializable. A few practical notes:

- Use exported fields - unexported fields are not serialized.
- Keep state reasonably small — it's read and written as a single value per actor. If you need to store large blobs (for example, full images), it's best to save them somewhere else (like an object storage service) and keep only a reference in the actor's state.
- The storage backend is your configured [provider](/docs/topologies) (SQLite or PostgreSQL).

Because state is durable and single-activation guarantees one writer at a time, you generally don't need optimistic concurrency or your own locking for an actor's own state.
