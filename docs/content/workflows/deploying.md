---
title: "Deploying and versioning"
weight: 80
description: "Versions, rolling deployments and draining old instances"
---

A workflow definition lives in Go code on your hosts, and a running instance refers to its steps by name. `WithVersion` is what makes it safe to deploy a changed definition while instances are still in flight.

## When to bump the version

Francis records a fingerprint of each version the first time it sees it, covering the graph and every setting that governs how a step runs. A second, different definition under the same number is refused.

You must bump the version when any of these happen:

- A step added, removed, renamed, or reordered
- A changed failure policy, timeout, attempt budget, or backoff
- A changed `WithInputFrom`, `WithItemsFrom`, `WithSkipIf`, `WithSkipOnFailure`, `WithOptional`, `WithMaxParallel`, `WithCompensateOnFailure`, or required capability
- Adding or removing a step's compensation
- Pointing a child step at a different definition or version
- A change to the definition's own `WithTimeout`, `WithRetention`, size caps, `WithMaxDepth`, `WithUnknownVersionPolicy`, or `WithCompensationFailurePolicy`

**Changing only a handler's body needs no bump**, since handler code is not part of the fingerprint. That has one consequence:

## If you forget to bump

Francis refuses to start when a workflow is changed but the version has not been bumped, returning `ErrDefinitionConflict`. In this case, you must bump the version.

> Whichever side deployed first owns the version number.

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

The version is stamped on each instance when it starts, and a host without that version declines to advance it.  
Old instances drain onto the hosts still running the old code while new instances start on the new one.

You should keep the old-version hosts up until `List(Version: old)` comes back empty.

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
| `workflow.FailUnknownVersion` | Once the instance timeout elapses, terminate it as `failed` with cause `unknown version`, without running compensations. |

`Definitions` tells you which versions are registered, and `List(Version: v)` which instances are parked on one.

## Deployment checklist

- Changing only a handler? Deploy. Bump anyway if the new behavior is materially different.
- Changing the graph? Bump `WithVersion`, deploy, and watch `francis.workflow.definition.conflicts` stay at zero.
- Wait for `List(Version: old)` to fall empty before removing the old-version hosts.
- Registered a version by mistake? `ForgetVersion` once it has no instances left.
