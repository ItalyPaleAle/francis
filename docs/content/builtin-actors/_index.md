---
title: "Built-in actors"
nav_title: "Overview"
weight: 25
description: "Framework-managed actors you register on a host"
aliases:
  - /docs/builtin-actors/
---

A built-in actor is a framework-managed actor that a host registers under a reserved type, and bootstraps at startup when it is a cluster-wide singleton. Francis ships several, each solving a coordination problem that is awkward to build yourself but falls out naturally from placement and durable state.

You register one by calling `host.RegisterBuiltInActor(...)` before the host starts (available on both the local and remote hosts), similarly to how you register your own actors with `RegisterActor`. It can be called more than once to register several built-in actors.

Built-in actors are reserved: their type names carry a `francis.builtin.` prefix, and clients **cannot target them directly**. You drive one through the service it exposes, which you obtain by binding it to a host's `actor.Service`:

```go
// Every built-in follows the same shape: build it, register it, then bind it to the host's service
svc := builtInActor.Service(host.Service())
```

As a rule, register the same built-in (same name and options) on every host that should serve it.
