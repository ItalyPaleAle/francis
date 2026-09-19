---
title: "Deploying and versioning"
weight: 80
description: "Versions, rolling deployments, and draining old instances"
---

A workflow definition lives in Go code on your hosts, and a running instance refers to its steps by name. `WithVersion` is what makes it safe to deploy a changed definition while instances are still in flight.

## When to bump the version

Francis records a fingerprint of each version the first time it sees it, covering the graph and every setting that governs how a step runs. A second, different definition under the same number is refused.

**Bump `WithVersion` for any of these:**

- a step added, removed, renamed, or reordered
- a changed failure policy, timeout, attempt budget, or backoff
- a changed `WithInputFrom`, `WithItemsFrom`, `WithSkipIf`, `WithSkipOnFailure`, `WithOptional`, `WithMaxParallel`, `WithCompensateOnFailure`, or required capability
- adding or removing a step's compensation
- pointing a child step at a different definition or version
- a change to the definition's own `WithTimeout`, `WithRetention`, size caps, `WithMaxDepth`, `WithUnknownVersionPolicy`, or `WithCompensationFailurePolicy`

**Changing only a handler's body needs no bump**, since handler code is not part of the fingerprint. That has one consequence:

> Note: **handler changes take effect part-way through a running instance**  
> An instance that has not reached a step yet runs whatever code is deployed when it gets there, so two instances of the same version can behave differently. Put a materially changed handler behind a feature flag, or bump the version anyway so old instances drain on the old code.

## If you forget to bump

Nothing corrupts, and you find out within seconds.

A host whose definition disagrees with a running instance declines to touch it, and the work re-routes to hosts that match. `Start` returns `ErrDefinitionConflict`, the `francis.workflow.definition.conflicts` counter increments, and both fingerprints are logged at error level.

Whichever side deployed first owns the version number. **The fix is a version bump.**

```go
// What is registered, and whether this host disagrees with any of it
defs, err := svc.Definitions(ctx)
for _, d := range defs {
	// d.Version, d.Fingerprint, d.FirstSeenAt, d.Conflicts
}
```

`ForgetVersion` is the repair for a version that was registered wrongly:

```go
err = svc.ForgetVersion(ctx, 3)
```

It refuses a version that still has instances, with `ErrVersionInUse`. Forgetting the version this service itself serves replaces the recorded entry with this host's definition.

## Rolling deployments

The version is stamped on each instance when it starts, and **a host without that version declines to advance it**. Old instances drain onto the hosts still running the old code while new instances start on the new one. Declining does not spend an attempt and does not dead-letter.

The drain is **not fast**. Each re-route costs one to two alarm poll intervals. At a 30-second poll interval, a 20-step instance on a four-host cluster with one old host takes about an hour to drain.

**Keep the old-version hosts up until `List(Version: old)` comes back empty.**

```go
// Watch the drain
page, err := svc.List(ctx, &workflow.ListOptions{Version: 2, Limit: 1})
drained := len(page.Instances) == 0
```

## When no host has the version

If you retire the old hosts too early, the instances left behind cannot advance. `WithUnknownVersionPolicy` decides what happens:

| Policy | Behavior |
|--------|----------|
| `workflow.ParkUnknownVersion` *(default)* | Wait indefinitely for a host that can serve the version. |
| `workflow.FailUnknownVersion` | Once the instance timeout elapses, terminate it as `failed` with cause `unknown version`, **without compensation**. |

Francis uses the instance's own timeout and policy, not the ones on whatever host it lands on. Suspended instances stay suspended.

`Definitions` tells you which versions are registered, and `List(Version: v)` which instances are parked on one.

## A deployment checklist

- Changing only a handler? Deploy. Bump anyway if the new behavior is materially different.
- Changing the graph? Bump `WithVersion`, deploy, and watch `francis.workflow.definition.conflicts` stay at zero.
- Wait for `List(Version: old)` to fall empty before removing the old-version hosts.
- Registered a version by mistake? `ForgetVersion` once it has no instances left.
