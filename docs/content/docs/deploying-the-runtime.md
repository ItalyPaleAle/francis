---
title: "Deploying the runtime"
weight: 28
---

In the [remote topology](/docs/topologies#remote-topology), the **runtime** is a standalone control plane: it owns the data store and coordinates placement, state, and alarms for a fleet of stateless worker hosts. This page covers building, configuring, and running the runtime.

If you're using the [local topology](/docs/topologies#local-topology), you don't need the runtime — skip this page.

## Building the runtime

The runtime is the `cmd/runtime` binary in the Francis repository:

```sh
go build -o bin/runtime github.com/italypaleale/francis/cmd/runtime
```

## Configuration

The runtime is configured with a YAML file, passed via `-config`:

```sh
bin/runtime -config config.yaml
```

A minimal configuration:

```yaml
# Address and port the runtime's WebTransport server listens on
bind: "0.0.0.0:7400"

# The runtime PSKs derive the cluster CA
# Every runtime sharing these keys is the same certificate issuer — keep them secret
runtimePSKs:
  - "change-me-runtime-psk"

# How joining hosts authenticate themselves to the runtime
bootstrap:
  method: psk
  hostPSK: "change-me-host-bootstrap-psk"

# Where state and alarms are stored
provider:
  type: sqlite
  connectionString: "data.db"

log:
  level: info
```

### Configuration reference

| Key | Description |
|-----|-------------|
| `bind` | Address and port the runtime listens on. Default `:8443`. |
| `runtimeId` | Optional identifier for this runtime, used in its server certificate. |
| `runtimePSKs` | List of runtime pre-shared keys from which the cluster CA is derived. **Required.** |
| `bootstrap.method` | How hosts authenticate when joining: `psk` or `jwt`. **Required.** |
| `bootstrap.hostPSK` | The shared host bootstrap secret, for `method: psk`. |
| `bootstrap.jwt.issuer` / `audience` / `jwksURL` / `staticJWKS` | JWT validation settings, for `method: jwt`. |
| `provider.type` | Data store: `sqlite`, `postgres`, or `memory`. **Required.** |
| `provider.connectionString` | Connection string or file path for the provider. |
| `workloadCertTTL` | Lifetime of the workload certificates issued to hosts. Default `1h`. |
| `healthCheckDeadline` | Maximum interval between host health pings. Default `20s`. |
| `alarmsPollInterval` | How often the runtime polls for due alarms. Default `1.5s`. |
| `alarmsLeaseDuration` | How long an alarm lease is held while executing. Default `20s`. |
| `shutdownGracePeriod` | Grace period for a clean shutdown. Default `30s`. |
| `log.level` | `debug`, `info`, `warn`, or `error`. |

Durations accept Go duration strings (e.g. `"1h"`, `"1500ms"`).

## Host bootstrap

When a worker first connects, it must prove it's allowed to join. The runtime supports two bootstrap methods:

### Pre-shared key (PSK)

The simplest method: the worker proves knowledge of a shared secret via a channel-bound challenge-response.

```yaml
bootstrap:
  method: psk
  hostPSK: "change-me-host-bootstrap-psk"
```

The worker must be configured with the matching key using `remote.WithHostBootstrapPSK([]byte(...))`.

### JWT

The worker presents a JWT that the runtime validates against a JWKS. This suits environments that already issue identity tokens (for example, Kubernetes projected service-account tokens).

```yaml
bootstrap:
  method: jwt
  jwt:
    issuer: "https://issuer.example.com"
    audience: "francis-runtime"
    jwksURL: "https://issuer.example.com/.well-known/jwks.json"
```

The worker provides the token with `remote.WithHostBootstrapJWTFile("/path/to/token")` (re-read on each bootstrap, so rotated tokens are picked up) or `remote.WithHostBootstrapJWT(token)` for a static token.

After a successful bootstrap, the runtime issues the host a short-lived workload certificate, and all later connections — to the runtime and between peer hosts — use mTLS. See [Security](/docs/security) for the full model.

## Pinning the cluster CA

To close the trust gap on a host's very first connection, print the cluster CA and pin it on your workers:

```sh
bin/runtime print-ca -config config.yaml
```

Pass the PEM output to the worker via `remote.WithPinnedCA(caPEM)`. Pinning is strongly recommended — especially with JWT bootstrap, where a bearer token would otherwise be exposed to a meddler-in-the-middle on the first connection. Only use `remote.WithUnsafeNoPinnedCA()` for local testing.

## Connecting workers

A worker is an ordinary `host/remote` host. The key options are the runtime address(es), the bootstrap credential, and the pinned CA:

```go
h, err := remote.NewHost(
	remote.WithAddress("10.0.0.5:7571"),       // peer address other hosts reach this one at
	remote.WithRuntimeAddresses("10.0.0.1:7400", "10.0.0.2:7400"), // runtime replicas
	remote.WithHostBootstrapPSK([]byte(os.Getenv("FRANCIS_HOST_BOOTSTRAP_PSK"))),
	remote.WithPinnedCA(caPEM),
)
```

`WithRuntimeAddresses` accepts multiple runtime replicas; the host connects to one at a time and rolls over to another on failure.

## Running multiple runtime replicas

For availability, you can run multiple runtime replicas that share the same `runtimePSKs` (so they form one certificate issuer) and the same database. Workers list all of them in `WithRuntimeAddresses` and fail over automatically.

## Database

The runtime stores all state and alarms in its configured provider:

- **PostgreSQL** (`provider.type: postgres`) is recommended for production. Use a standard connection string, e.g. `postgres://user:pass@host:5432/dbname`.
- **SQLite** (`provider.type: sqlite`) works well when a single runtime owns the database. Do **not** place the SQLite file on a networked filesystem (NFS/SMB).
- **In-memory** (`provider.type: memory`) is non-durable and intended for testing only.
