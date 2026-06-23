---
title: "Deploying the runtime"
weight: 28
---

In the [remote topology](/docs/topologies#remote-topology), the **runtime** is a standalone control plane: it owns the data store and coordinates placement, state, and alarms for a fleet of stateless worker hosts. This page covers obtaining, configuring, and running the runtime.

If you're using the [local topology](/docs/topologies#local-topology), you don't need the runtime — skip this page.

## Getting the runtime

The recommended way to run the runtime is the published container image. Pre-compiled binaries are also available.

> **Use UDP for port forwarding**  
> The runtime's WebTransport server runs over HTTP/3 (QUIC), which is based on UDP. Whenever you publish the runtime's port (in `docker run`, Compose, a firewall rule, or a load balancer) make sure it's the UDP port, not TCP.

### Container image (Docker or Podman)

The runtime is published to the GitHub Container Registry as `ghcr.io/italypaleale/francis`. Images are multi-arch (`linux/amd64`, `linux/arm64`, and `linux/arm/v7`). The available tags are:

- A full version, e.g. `1.2.3`.
- Floating `1.2` and `1` tags that track the latest patch/minor.

Mount your configuration file into the container and pass it with `-config`. The example below also mounts a named volume for the SQLite data store and publishes the UDP port:

```sh
docker run \
  --name francis-runtime \
  -p 7400:7400/udp \
  -v "$(pwd)/config.yaml:/config.yaml:ro" \
  -v francis-data:/data \
  ghcr.io/italypaleale/francis:1 \
  -config /config.yaml
```

Or, with [Podman](https://podman.io/):

```sh
podman run \
  --name francis-runtime \
  -p 7400:7400/udp \
  -v "$(pwd)/config.yaml:/config.yaml:ro" \
  -v francis-data:/data \
  ghcr.io/italypaleale/francis:1 \
  -config /config.yaml
```

> The image is built on a distroless base and runs as a **non-root** user (UID 65532). When using SQLite, persist the SQLite store on a volume, then point `provider.connectionString` at the mounted volume (for example `/data/data.db`). The data directory must be writable by that user.

The image ships with a `HEALTHCHECK` that probes the locally-running runtime, so `docker ps` and orchestrators report container health automatically.

### Docker Compose

To run the runtime under Docker Compose, drop this into a `docker-compose.yaml` next to your `config.yaml`:

```yaml
services:
  runtime:
    image: ghcr.io/italypaleale/francis:1
    command: ["-config", "/config.yaml"]
    ports:
      - "7400:7400/udp"
    volumes:
      - ./config.yaml:/config.yaml:ro
      - francis-data:/data
    restart: unless-stopped

volumes:
  francis-data:
```

Then start it in the background:

```sh
docker compose up -d
```

### Pre-compiled binaries

Pre-compiled binaries are attached to every release on the [releases page](https://github.com/ItalyPaleAle/francis/releases). Builds are published for Linux (`amd64`, `arm64`, `armv7`), macOS (`arm64`), and FreeBSD (`amd64`, `arm64`).

Download the archive for your platform, extract it, and run the `francis` binary inside:

```sh
# Replace VERSION and the platform suffix to match the release you want
VERSION=1.2.3
curl -LO https://github.com/ItalyPaleAle/francis/releases/download/v${VERSION}/francis-${VERSION}-linux-amd64.tar.gz
tar -xzf francis-${VERSION}-linux-amd64.tar.gz

./francis-${VERSION}-linux-amd64/francis -config config.yaml
```

## Configuration

The runtime is configured with a YAML file, passed via `-config`:

```sh
francis -config config.yaml
```

A minimal configuration:

```yaml
# Address and port the runtime's WebTransport server listens on (using UDP)
bind: "0.0.0.0:7400"

# The runtime PSKs derive the cluster CA
# Every runtime sharing these keys is the same certificate issuer
# Keep them secret
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
| `alarmsPollInterval` | How often the runtime polls for due alarms. Default `1500ms`. |
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
francis print-ca -config config.yaml
```

When running from a container, invoke the same subcommand inside it — for example with the config mounted as above:

```sh
docker run --rm \
  -v "$(pwd)/config.yaml:/config.yaml:ro" \
  ghcr.io/italypaleale/francis:1 \
  print-ca -config /config.yaml
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
- **SQLite** (`provider.type: sqlite`) works well when a single runtime owns the database. Do **not** place the SQLite file on a networked filesystem like NFS/SMB.
- **In-memory** (`provider.type: memory`) is non-durable and intended for testing only.
