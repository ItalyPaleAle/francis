---
title: "The simple & low-maintenance Go distributed actor framework"
nav_title: "Introduction"
weight: 11
source_path: "README.md"
---

Francis is a framework and runtime for **Distributed Actors** (also known as **Durable Objects**) for Go apps.

![Francis logo](/docs/img/francis-logo.png)

With Francis, you can build **highly-available** apps that **scale horizontally** and/or use **microservices**. Unlike other actor frameworks, Francis is designed to be simpler to add to your solution and lower-maintenance: it only requires a relational database (PostgreSQL or SQLite) and can optionally run embedded in your apps too, without a separate control plane service.

**What you can use Francis for:**

- Build **stateful services** where each entity (a user, a device, a shopping cart, a game session) is an actor with its own durable state
- **Scale horizontally** across many hosts without sharding your data by hand: Francis places each actor on exactly one host and routes calls to it
- Run **background work on a schedule** with durable [alarms](/docs/alarms) that survive restarts
- Add resilience to **microservices** without standing up extra infrastructure beyond a database

```go
// An actor is just a Go struct that implements one or more methods
func (c *Counter) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	state, _ := c.client.GetState(ctx)
	state.Count++
	_ = c.client.SetState(ctx, state, nil)
	return state.Count, nil
}
```

Francis is [fully open source](https://github.com/ItalyPaleAle/francis) and released under a permissive MIT license.

> ## ⚠️🚧 Work in progress 🚧⚠️
>
> While currently functional, Francis is still under **heavy development**. APIs are not final and are expected to change until the first official release.

## How it works

1. You write an **actor** as a Go struct and register it with an actor **host** under a type name.
2. Your app invokes an actor by its **type** and **ID** (e.g. `cart` / `user-42`). Francis activates the actor on exactly one host in the cluster and routes the call to it.
3. The actor reads and writes its own **durable state**, stored in the database. State outlives the actor: when an actor is deactivated and later re-activated (possibly on another host), its state is still there.
4. Actors can schedule **alarms** to run work at a future time, optionally on a repeating interval. Alarms are durable and survive restarts.
5. After a configurable idle period, an actor is **deactivated** automatically to free resources. The next call re-activates it.

## Key features

- **Virtual actors** — actors are addressed by type and ID, activated on demand, and run one invocation at a time, so you never manage their lifecycle or worry about concurrent access to their state
- **Durable state** — each actor has its own state persisted in PostgreSQL or SQLite; it survives deactivation, restarts, and moving between hosts
- **Durable alarms** — schedule one-off or repeating work that survives process restarts
- **Two topologies, same code** — run everything embedded in your app (**local**) with no extra services, or point your workers at a standalone **runtime** (**remote**) when you want a dedicated control plane; your actor code is identical
- **Low-maintenance** — the only hard dependency is a relational database; no separate message broker, no external coordination service
- **Secure by default** — hosts authenticate each other with mTLS using certificates derived from a shared cluster key, with pluggable host bootstrap (pre-shared key or JWT)
- **Built for Go** — a small, idiomatic API built around the standard library, `context.Context`, and generics

## Where to go next

- New to actors? Start with [What is Francis](/docs/what-is-francis) and the [core concepts](/docs/concepts).
- Want to run something now? Follow the [Quickstart](/docs/quickstart).
- Ready to write code? See [Writing actors](/docs/writing-actors).
