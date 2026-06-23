---
title: "Security model"
weight: 29
---

Francis secures all host-to-host and host-to-runtime communication with **mutual TLS (mTLS)**. Hosts authenticate each other using certificates derived from a shared **cluster key**, so only hosts that belong to your cluster can place actors, forward invocations, or talk to the runtime.

This page explains how trust is established and the knobs you control.

## The cluster CA and runtime PSKs

A Francis cluster has a **certificate authority (CA)** that all members trust. The CA is **derived** from one or more **runtime pre-shared keys (PSKs)** — there is no separate CA file to distribute.

- Every member configured with the same runtime PSK derives the same CA, and therefore trusts the same issuer.
- The first PSK is the **primary**, used to sign new certificates. Additional PSKs are trusted too, which enables **rolling key rotation**: add a new primary, let everyone trust both, then retire the old one.

In the **local** topology, you set the PSKs with `local.WithRuntimePSKs(...)`. Every local host self-issues its own workload certificate from the derived CA, so hosts sharing the PSKs authenticate each other with mTLS.

In the **remote** topology, the runtime holds the PSKs (`runtimePSKs` in its config) and acts as the issuer: it mints short-lived workload certificates for hosts that bootstrap successfully.

> Treat runtime PSKs as **highly sensitive cluster secrets**. Anyone holding a current PSK can mint a trusted certificate and join the cluster. Inject them from your secret store / environment rather than committing them.

## Host bootstrap (remote topology)

Before the runtime issues a worker a certificate, the worker must prove it's allowed to join. This is the **bootstrap** step, and it supports two methods:

- **Pre-shared key (PSK)** — the worker proves knowledge of a shared `hostPSK` via a channel-bound challenge-response.
- **JWT** — the worker presents a JWT that the runtime validates against a JWKS (issuer, audience, signing keys). This fits environments that already issue workload identities, like Kubernetes projected service-account tokens.

After a successful bootstrap, the runtime issues a **short-lived workload certificate**, and every subsequent connection — to the runtime and between peer hosts — uses mTLS with that certificate. Short lifetimes (default one hour) bound how long a compromised or revoked host stays trusted. See [Deploying the runtime](/docs/deploying-the-runtime#host-bootstrap) for configuration.

## Closing the first-connection trust gap

On a worker's **very first** connection to the runtime, it hasn't yet received a certificate and needs a way to verify it's talking to the real runtime. **Pin the cluster CA** to close this gap:

```sh
# Print the cluster CA derived from the runtime PSKs
runtime print-ca -config config.yaml
```

Pass the PEM to the worker with `remote.WithPinnedCA(caPEM)`. The worker then verifies the runtime from the first byte.

The alternative, `remote.WithUnsafeNoPinnedCA()`, trusts the runtime's certificate on first use. This is **unsafe**: a meddler-in-the-middle on that first connection could impersonate the runtime — and with JWT bootstrap, capture the bearer token. Use it only for local development. Exactly one of `WithPinnedCA` or `WithUnsafeNoPinnedCA` must be set.

## Key rotation

Because the CA is derived from the runtime PSKs, you rotate trust by rotating PSKs:

1. Add a new PSK as the **primary** while keeping the old one in the trusted list. Members trust certificates from both.
2. Roll out the new configuration to all runtimes and hosts.
3. Once everything has re-issued certificates under the new primary, remove the old PSK.

Workload certificates are short-lived, so they roll over quickly once the primary changes — no manual certificate distribution is needed.

## What's protected

- **Host-to-runtime** traffic (bootstrap, placement, state, alarms) is mTLS once the host has a certificate.
- **Host-to-host** peer invocations — when a call is forwarded to the host that owns an actor — are mTLS using the cluster CA.
- Hosts reject requests that don't present a valid certificate chain to the cluster CA, so outsiders can't place actors or read/write state.

## Operational guidance

- **Keep PSKs secret and injected at deploy time** — mount the runtime's config file from a secret manager or orchestrator secret, not source control.
- **Always pin the CA in production** (`WithPinnedCA`), especially with JWT bootstrap.
- **Prefer JWT bootstrap** where your platform already issues workload identities; it avoids distributing a long-lived shared host secret.
- **Bind to addresses deliberately** — the peer address you advertise (`WithAddress`) must be reachable by other hosts for invocation forwarding to work.
