---
title: "Deploying and versioning"
weight: 80
description: "Versions, rolling deployments, and draining old instances"
---

A workflow definition lives in Go code on your hosts, and an instance that is already running refers to its steps by name. Deploying a changed definition while instances are in flight is the operation to get right here, and `WithVersion` is what makes it safe.

## When to bump the version

Francis records each version's definition cluster-wide the first time it sees it, as a **fingerprint** over the graph and every setting that governs how a step runs. A second, different definition under the same number is refused.

**Bump `WithVersion` for any of these:**

- a step added, removed, renamed, or reordered
- a changed failure policy, timeout, attempt budget, or backoff
- a changed `WithInputFrom`, `WithItemsFrom`, `WithSkipIf`, `WithSkipOnFailure`, `WithOptional`, `WithMaxParallel`, `WithCompensateOnFailure`, or required capability
- adding or removing a step's compensation
- pointing a child step at a different definition or version
- a change to the definition's own `WithTimeout`, `WithRetention`, size caps, `WithMaxDepth`, `WithUnknownVersionPolicy`, or `WithCompensationFailurePolicy`

**Changing only a handler's body needs no bump.** Handler code is not part of the fingerprint, which has one consequence worth planning for:

> Note: **handler changes take effect part-way through a running instance**  
> An instance that has not reached a step yet runs whatever code is deployed when it gets there, so two instances of the same version can behave differently for the same step. Treat a materially changed handler like any other live code swap: put it behind a feature flag, or bump the version anyway so old instances drain on the old code.

## If you forget to bump

Nothing corrupts, and you find out within seconds.

A host whose definition disagrees with a running instance's recorded version declines to touch it, and the work re-routes to hosts that match. `Start` returns `ErrDefinitionConflict`. The `francis.workflow.definition.conflicts` counter increments with the workflow and version, and both fingerprints are logged at error level.

Whichever side deployed first owns the version number, so either the old hosts start conflicting or the new ones do and the deploy drains no work at all. Either way, **the fix is a version bump**.

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

It refuses a version that still has instances, with `ErrVersionInUse`, since forgetting one out from under a running instance would let a different graph claim its number. Forgetting the version this service itself serves replaces the recorded entry with this host's definition, so a corrected graph takes over atomically.

## Rolling deployments

The version is stamped on each instance when it starts, and **a host without that version declines to advance it**. Old instances drain onto the hosts still running the old code while new instances start on the new one, and no task is ever run by a handler set from a different version. Declining costs nothing: it does not spend an attempt and it does not dead-letter.

The drain is correct but **not fast**. Each re-route costs one to two alarm poll intervals. With `k` old-version hosts out of `N`, every step of an old instance expects roughly `N/k` re-routes before it lands somewhere that can serve it. At a 30-second poll interval, a 20-step instance on a four-host cluster with one old host takes on the order of an hour to drain.

So: **keep the old-version hosts up until `List(Version: old)` comes back empty, and expect to wait.**

```go
// Watch the drain
page, err := svc.List(ctx, &workflow.ListOptions{Version: 2, Limit: 1})
drained := len(page.Instances) == 0
```

## When no host has the version

If you retire the old hosts too early, the instances left behind cannot advance. `WithUnknownVersionPolicy` decides what happens to them:

| Policy | Behavior |
|--------|----------|
| `workflow.ParkUnknownVersion` *(default)* | Wait for a host that can serve the version. The instance sits there indefinitely and resumes if one appears. |
| `workflow.FailUnknownVersion` | Once the instance timeout has elapsed, terminate it as `failed` with cause `unknown version`, **without compensation**, since no host can run the compensations either. |

The instance's own timeout and policy are used, not the ones on whatever host it lands on, so retiring a version does not change how its stragglers behave. Suspended instances stay suspended either way.

`Definitions` tells you which versions are registered, and `List(Version: v)` which instances are parked on one.

## A deployment checklist

- Changing only a handler? Deploy. Nothing else to do, unless the new behavior is materially different, in which case bump anyway.
- Changing the graph? Bump `WithVersion`, deploy, and watch `francis.workflow.definition.conflicts` stay at zero.
- Wait for `List(Version: old)` to fall empty before removing the old-version hosts.
- Registered a version by mistake? `ForgetVersion` once it has no instances left.
