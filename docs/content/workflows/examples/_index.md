---
title: "Examples"
nav_title: "Overview"
weight: 110
description: "Three complete workflows, with every handler and what happens when they go wrong"
---

Three workflows that between them exercise every feature. Each one is complete: the definition, every handler, and a walk through the ways it goes wrong.

| Example | What it shows |
|---------|---------------|
| [Thumbnails and a manifest](/workflows/examples/thumbnails) | Fan-out with `TolerateFailures`, `WithSkipOnFailure`, `WithOptional`, and the three different costs a step's failure can have |
| [Checkout with compensations](/workflows/examples/checkout) | The compensation stack, idempotent handlers, per-step retry policies, and the partial-rollback outcome |
| [Tenant provisioning](/workflows/examples/tenant-onboarding) | Waiting on an event, conditional steps, a parallel group of child workflows, capabilities, and suspend |

They are written to be copied. The handlers assume an object store, a payments provider, and a cloud API that you will have your own versions of, but the shapes are the point.
