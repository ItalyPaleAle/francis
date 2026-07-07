---
title: "Writing actors"
weight: 24
---

This guide covers how to implement actors in Francis: the factory, the methods an actor can implement, how to invoke actors, and how to manage an actor's lifecycle.

## Anatomy of an actor

An actor is a Go struct plus a **factory** function. The factory is what you register with a host; Francis calls it to activate an actor on demand.

```go
func(actorID string, service *actor.Service) actor.Actor
```

`actor.Actor` is an alias for `any`, so your actor can be any type. What makes a struct an actor is the set of optional interfaces it implements.

```go
type Cart struct {
	client actor.Client[cartState]
	log    *slog.Logger
}

type cartState struct {
	Items []string
}

func NewCart(actorID string, service *actor.Service) actor.Actor {
	return &Cart{
		client: actor.NewActorClient[cartState]("cart", actorID, service),
	}
}
```

A fresh instance is created on every activation. Hold per-activation data on the struct (such as the typed [client](/docs/concepts#client)). Anything that must outlive the activation should be stored in the actor's persistent [state](/docs/state).

## Actor methods

An actor implements behavior by satisfying one or more of these interfaces. Implement only the ones you need.

### `Invoke`: handle method calls

Implement `actor.ActorInvoke` to handle invocations:

```go
func (c *Cart) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	// "method" is the method name chosen by the caller
	// "data" carries the request payload (may be nil)

	switch method {
	case "addItem":
		var req struct{ Item string }
		if data != nil {
			err := data.Decode(&req)
			if err != nil {
				return nil, err
			}
		}

		state, err := c.client.GetState(ctx)
		if err != nil {
			return nil, err
		}
		state.Items = append(state.Items, req.Item)

		err = c.client.SetState(ctx, state, nil)
		if err != nil {
			return nil, err
		}

		return len(state.Items), nil
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}
```

- `method` is an arbitrary string chosen by the caller - switch on it to dispatch.
- `data` is an `actor.Envelope`. Call `data.Decode(&dest)` to decode the request payload into a Go value. It may be `nil` when there is no payload.
- The return value (`any`) is serialized and returned to the caller, who decodes it from their own `Envelope`. Return `nil` for no response.
- Returning an error fails the invocation, which returns the error to the caller.

### `Peek`: handle read-only method calls

`Invoke` serializes every call to an actor: only one runs at a time. Implement `actor.ActorPeek` to add a read-only method that many callers can run **concurrently** with each other, while still being mutually exclusive with any in-flight `Invoke`. Use it for reads that don't need to wait behind other invocations:

```go
func (c *Cart) Peek(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case "itemCount":
		state, err := c.client.GetState(ctx)
		if err != nil {
			return nil, err
		}
		return len(state.Items), nil
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}
```

`Peek` has the same shape as `Invoke`, but they differ in how state is handled:

- The framework rejects mutating client calls. `SetState`, `DeleteState`, `SetAlarm`, `DeleteAlarm`, and `Dispatch` all return `actor.ErrReadOnly` when called from within `Peek`. `GetState` is always allowed.
- However, the framework cannot stop you from mutating your own in-memory fields. Since multiple `Peek` calls can run at the same time, mutating a field on the actor struct itself (not just its persisted state) from within `Peek` is a data race. **Treat the actor as read-only in your own code too**, not just through the client.

An actor can implement `Peek`, `Invoke`, both, or neither, as all interfaces are optional.

### `Alarm`: handle scheduled callbacks

Implement `actor.ActorAlarm` to receive [alarms](/docs/alarms):

```go
func (c *Cart) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	// "name" is the alarm name you chose when scheduling it
	// "data" carries the optional data attached to the alarm (may be nil)
	c.log.InfoContext(ctx, "alarm fired", "name", name)
	return nil
}
```

When an alarm fires, Francis activates the actor if needed and calls `Alarm`. See [Alarms](/docs/alarms) for scheduling.

### `Deactivate`: clean up before deactivation

Implement `actor.ActorDeactivate` to run logic right before the actor is deactivated (because it went idle, was halted, or the host is shutting down):

```go
func (c *Cart) Deactivate(ctx context.Context) error {
	c.log.InfoContext(ctx, "cart deactivating")
	return nil
}
```

Keep `Deactivate` quick: it runs within a deactivation timeout (5 seconds by default, configurable per actor type). Use it to flush in-memory work, not for long-running tasks.

**Important:** an actor could disappear at any point because its host (or the physical node it's on) crashes. In that case, the `Deactivate` method may not be invoked. Do not wait for the `Deactivate` to persist critical data.

## Concurrency model

An actor processes **one invocation at a time**. Calls to the same actor are serialized (_turn-based concurrency_), so you never need locks to protect the actor's own fields or its state from concurrent access.

`Peek` is an optional method that relaxes this for read-only calls: think of it as the equivalent of Go's `sync.RWMutex` around the actor's turn.  
`Invoke` takes the write lock (exclusive), and `Peek` takes the read lock (shared) - many `Peek` calls can run at once, but a `Peek` and an `Invoke` never overlap, and callers are served in FIFO order so a waiting `Invoke` is never starved by a steady stream of `Peek` calls.  

Different actors (different IDs, or different types) run concurrently across the cluster, so your app scales by having many actors rather than by making one actor handle parallel work.

The context passed to your methods is cancelled if the invocation times out or the host is shutting down.

## Registering an actor

Register each actor type with the host **before** calling `Run`:

```go
err := h.RegisterActor("cart", NewCart, local.RegisterActorOptions{
	IdleTimeout: 10 * time.Minute,
})
```

`RegisterActorOptions` controls activation and retry behavior:

| Option | Default | Description |
|--------|---------|-------------|
| `IdleTimeout` | `5m` | How long an actor can stay idle before it's deactivated. A negative value disables the idle timeout. |
| `DeactivationTimeout` | `5s` | Maximum time allowed for `Deactivate` to run. |
| `ConcurrencyLimit` | `0` (unlimited) | Maximum number of actors of this type active on a single host. |
| `MaxAttempts` | `3` | Maximum attempts when invoking the actor or running an alarm. |
| `InitialRetryDelay` | `2s` | Initial delay before retrying a failed invocation, with backoff. |

## Invoking actors

From outside an actor, e.g. an HTTP handler, use the `actor.Service` you got from `host.Service()`:

```go
resp, err := service.Invoke(ctx, "cart", "user-42", "addItem", map[string]any{"Item": "book"})
if err != nil {
	// handle error
}

var count int
if resp != nil {
	err = resp.Decode(&count)
}
```

- The `data` argument is any value that serializes to JSON and it becomes the `Envelope` your actor decodes.
- The response is an `actor.Envelope` (or `nil`), call `Decode` to read it.
- You can invoke from **any** host: Francis routes the call to whichever host owns the actor, activating it if needed.

### Invoking only if active

By default, invoking an actor activates it if it isn't already. To invoke **only** when the actor is already active, pass `actor.WithInvokeActiveOnly()`. If the actor isn't active, you get `actor.ErrActorNotActive`:

```go
resp, err := host.Invoke(ctx, "cart", "user-42", "itemCount", nil, actor.WithInvokeActiveOnly())
```

### Peeking actors

If your actor implements `actor.ActorPeek` (see [`Peek`](#peek-handle-read-only-method-calls) above), call `service.Peek` instead of `service.Invoke` to run it under the shared (read) lock instead of the exclusive (write) lock:

```go
resp, err := service.Peek(ctx, "cart", "user-42", "itemCount", nil)
```

`Peek` takes the same arguments and options as `Invoke`, including `actor.WithInvokeActiveOnly()`, and fails if the actor doesn't implement `ActorPeek`.

### Calling another actor from an actor

Because the factory hands you the `*actor.Service`, an actor can invoke other actors by calling `service.Invoke(...)` (or `service.Peek(...)` for a read-only call).

Avoid **re-entrancy** (actor A calling actor B, which calls back into A in the same call chain), since each actor handles one invocation at a time.

## Streaming invocations

For large request or response bodies, use `InvokeStream`, which streams bytes instead of buffering a JSON payload:

```go
respContentType, resp, err := service.InvokeStream(
	ctx, "report", "2026-q1", "render",
	"application/json", requestBody,
)
if err != nil {
	return err
}
defer resp.Close()
// read from resp...
```

The host enforces a maximum request body size (configurable with `WithMaxRequestBodySize`).

Implement `actor.ActorPeekStream` and call `service.PeekStream` for the read-only, streaming analogue of `InvokeStream`: the actor holds the shared (read) lock for the whole call, so multiple `PeekStream` calls can run concurrently with each other, on the same terms as `Peek`.

## Halting an actor

Halting deactivates an actor immediately rather than waiting for the idle timeout.

From inside an actor, call `Halt()` on its client to deactivate after the current invocation returns:

```go
func (c *Cart) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	if method == "checkout" {
		// ... finalize ...
		c.client.Halt() // deactivate once this invocation completes
		return nil, nil
	}
	// ...
}
```

> `client.Halt()` is deferred: it schedules the halt so it runs after the current invocation, avoiding a deadlock.

From outside, the service offers:

- `service.Halt(actorType, actorID)`: halt a specific actor active on this host
- `service.HaltAll()`: halt all actors active on this host
- `service.HaltDeferred(actorType, actorID)`: non-blocking variant of `Halt`

A halted actor is simply hybernated. Its state stays in the database, and the next invocation re-activates it.

## Common errors

Methods that invoke actors or manage state and alarms may return these sentinel errors (in package `actor`), which you can match with `errors.Is`:

| Error | Meaning |
|-------|---------|
| `ErrStateNotFound` | No state exists for the actor (returned by `GetState`/`DeleteState` at the service level). |
| `ErrAlarmNotFound` | The named alarm doesn't exist. |
| `ErrActorNotActive` | `WithInvokeActiveOnly()` was used and the actor isn't active. |
| `ErrActorNotHosted` | `Halt` targeted an actor that isn't active on the current host. |
| `ErrActorHalted` | The actor is halted on the host where it was active, retry after a delay. |
| `ErrActorTypeUnsupported` | No host in the cluster serves this actor type. |
| `ErrNoHost` | No host is currently available to place the actor. |
| `ErrReadOnly` | A `Client` method that mutates state, alarms, or jobs (`SetState`, `DeleteState`, `SetAlarm`, `DeleteAlarm`, `Dispatch`) was called from within `Peek`. |
