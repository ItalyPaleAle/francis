---
title: "Deploying and versioning"
weight: 80
description: "The registry, what needs a version bump, and rolling deployments"
---

A definition lives in Go code on your hosts. A running instance's journal refers to steps by name, in the order the definition had when it started. Deploying a changed definition while instances are running is therefore the hardest operational problem here, and three mechanisms address it.

## The registry

Each workflow has a cluster-wide singleton that maps every version of the definition to the **fingerprint** of its graph: a hash over everything the engine reads while running an instance. That is the ordered step names and kinds, the options that shape the graph — `WithInputFrom`, `WithItemsFrom`, `WithSkipOnFailure`, `WithSkipIf`, `WithOptional`, the failure policies, whether a step has a compensation, its required capability, and any child definition's name and version — and the settings that decide what a turn does with a result: the attempt budgets and backoffs, `WithStepTimeout`, `WithEventTimeout`, `WithMaxParallel`, `WithCompensateOnFailure`, and at the definition level `WithTimeout`, `WithRetention`, the size caps, `WithMaxDepth`, `WithUnknownVersionPolicy`, and `WithCompensationFailurePolicy`.

Two hosts that agree on the graph but not on those would apply different transitions to one journal, which is exactly what the registry exists to stop.

Handler **bodies** are not fingerprinted.

The registry answers one question: is this the graph recorded for this version? If the version is unknown, it records it and answers yes. If it is known with the same fingerprint, yes. Otherwise, **conflict**, with the recorded fingerprint and when it was first seen. It never overwrites, so the first deployment of a version defines it.

The check is made before a host's orchestrator, worker, or undo actor handles a job. A host asks again for each delivery so `ForgetVersion` takes effect across hosts that remain online; otherwise an old host could keep serving a fingerprint that the registry no longer authorizes. Known versions use concurrent read-only checks, while the first caller for an unknown version registers it in an exclusive turn. A lookup failure returns the job for retry rather than letting it run without a consistency decision.

## When a host conflicts

A host whose graph disagrees with the recorded one **declines every job of that version**, so the work re-routes to hosts whose code matches. It emits `francis.workflow.definition.conflicts` with the version and both fingerprints, and logs at error level. It does not stop the host: other workflows and other versions are unaffected.

The failure mode is loud by design. If someone changes the graph and forgets to bump the version, the first host to deploy registers the new fingerprint under the old number and every host still running the old code starts conflicting — or, if old hosts got there first, every new host conflicts and the deploy drains no work at all. Either way the metric fires within one turn, **nothing corrupts**, and the fix is a version bump.

```go
// What the registry holds, and whether this host disagrees with any of it
defs, err := svc.Definitions(ctx)
for _, d := range defs {
	// d.Version, d.Fingerprint, d.FirstSeenAt, d.Conflicts
}

// The reset for a version registered wrongly, once it has no instances left
err = svc.ForgetVersion(ctx, 3)
```

`ForgetVersion` refuses a version that still has instances with `ErrVersionInUse`, since forgetting one under a running instance would let a different graph claim its number.

Live hosts observe the reset on their next delivery. This lets a corrected definition claim the version without leaving an earlier host's approval cached.

## What needs a new version

**Any change to the graph, or to any setting a turn reads.** A step added, removed, renamed, reordered, or with a changed policy, timeout, attempt budget, or backoff changes the fingerprint, and so does a change to the definition's own timeout, retention, caps, or unknown-version and compensation-failure policies. The registry refuses any of them under the old number.

**A change to a handler's body alone does not.** That is the direct consequence of not replaying code — and its converse is worth understanding:

> An instance that has not reached a step yet runs whatever code is deployed for it. Two instances of the "same" version can observe different behavior for the same step, depending on when each reaches it relative to a deploy.

Treat a materially different handler the way you would any other live code swap: feature-flag it, or bump the version anyway so old instances drain on old code.

## Rolling deployments

1. **`WithVersion(n)` is stamped on the journal** when the instance starts.
2. **A host without the instance's version declines to advance it.** The `Workflow` actor returns a rejection, which halts the actor to clear its placement and re-routes the occurrence to another host — **without counting an attempt and without dead-lettering it**. Old instances drain onto the hosts still running the old code; new instances start on the new one. The same rule applies on the worker and undo types, so a task is never run by a handler set from another version.
3. **The drain is correct but not fast.** A re-route costs a jittered one to two alarm poll intervals. With `k` old-version hosts among `N`, each turn of an old instance expects about `N/k` re-routes before it lands; at a 30-second poll interval, a 20-turn instance on a four-host cluster with one old host takes on the order of an hour to drain.

So: **keep old-version hosts up until `List(Version: old)` is empty, and expect it to take a while.**

```go
// Watch the drain
page, err := svc.List(ctx, &workflow.ListOptions{Version: 2, Limit: 1})
drained := len(page.Instances) == 0
```

## When no host has the version

The deadline alarm is delivered to the instance's actor on whatever host holds it, so a version-mismatched host cannot simply decline it forever. When the alarm fires there, the handler works **from the journal alone** — the full step list is recorded at `Start` for exactly this reason — and applies `WithUnknownVersionPolicy`:

| Policy | Behavior |
|--------|----------|
| `workflow.ParkUnknownVersion` *(default)* | Re-arm the alarm and wait for a host that can serve the version. |
| `workflow.FailUnknownVersion` | Once the instance timeout has elapsed, terminate it as `failed` with cause `unknown version`, **without compensation** — since no host can run the compensations either. |

`Definitions` tells you which versions are registered, and `List(Version: v)` which instances are parked on one.

## A deployment checklist

- Changing only a handler? Deploy. Nothing else to do, unless the new behavior is materially different — then bump anyway.
- Changing the graph? Bump `WithVersion`, deploy, and watch `francis.workflow.definition.conflicts` stay at zero.
- Watch `List(Version: old)` fall to empty before removing the old-version hosts.
- A version you registered by mistake, with no instances left, clears with `ForgetVersion`.
