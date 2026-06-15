---
title: "Host options reference"
weight: 41
---

This page lists the options accepted by `local.NewHost` and `remote.NewHost`. Both follow the functional-options pattern: each option is a `With...` function you pass to `NewHost`.

## Local host options (`host/local`)

Used to construct an embedded host in the [local topology](/docs/topologies#local-topology).

### Networking

| Option | Description |
|--------|-------------|
| `WithAddress(addr string)` | Address the host is reachable at and advertises to peers, e.g. `"127.0.0.1:7571"`. |
| `WithBindAddress(addr string)` | Address to bind the peer server to. Defaults to the address part of `WithAddress`. |
| `WithBindPort(port int)` | Port to bind the peer server to. Defaults to the port part of `WithAddress`. |

### Security

| Option | Description |
|--------|-------------|
| `WithRuntimePSKs(psks ...[]byte)` | Runtime pre-shared keys from which the cluster CA is derived. The first is the primary used to sign this host's certificate; additional keys are trusted during a rolling rotation. See [Security](/docs/security). |

### Data store provider

Set exactly one provider:

| Option | Description |
|--------|-------------|
| `WithSQLiteProvider(opts SQLiteProviderOptions)` | Use an embedded SQLite store. |
| `WithPostgresProvider(opts PostgresProviderOptions)` | Use a PostgreSQL store (recommended for multi-node clusters). |
| `WithStandaloneMemoryProvider(opts)` | Use a non-durable in-memory store (testing). |
| `WithStandaloneSQLiteProvider(opts)` | Use SQLite via an existing `*sql.DB` you supply. |
| `WithStandalonePostgresProvider(opts)` | Use PostgreSQL via an existing `*pgxpool.Pool` you supply. |

### Behavior & limits

| Option | Description |
|--------|-------------|
| `WithLogger(logger *slog.Logger)` | Provide an `slog` logger. |
| `WithShutdownGracePeriod(d time.Duration)` | Grace period for a clean shutdown. |
| `WithProviderRequestTimeout(d time.Duration)` | Timeout for requests to the data store provider. |
| `WithHostHealthCheckDeadline(d time.Duration)` | Maximum interval between health pings from a host. |
| `WithAlarmsPollInterval(d time.Duration)` | How often to poll for due alarms. |
| `WithAlarmsLeaseDuration(d time.Duration)` | How long an alarm lease is held while executing. |
| `WithAlarmsFetchAheadInterval(d time.Duration)` | Look-ahead window for pre-fetching upcoming alarms. |
| `WithAlarmsFetchAheadBatchSize(n int)` | Batch size for pre-fetching alarms. |
| `WithMaxInFlightRequests(n int)` | Concurrent peer invocations processed per session before excess calls are rejected with a retryable overloaded error. |
| `WithMaxRequestBodySize(n int64)` | Maximum size, in bytes, of a streamed peer invocation request body. |

## Remote host options (`host/remote`)

Used to construct a stateless worker in the [remote topology](/docs/topologies#remote-topology).

### Networking

| Option | Description |
|--------|-------------|
| `WithAddress(addr string)` | Peer address this host advertises (to the runtime and to other hosts). |
| `WithBindAddress(addr string)` | Address to bind the peer server to. Defaults to the address part of `WithAddress`. |
| `WithBindPort(port int)` | Port to bind the peer server to. Defaults to the port part of `WithAddress`. |
| `WithRuntimeAddresses(addresses ...string)` | One or more runtime replica addresses. The host connects to one at a time and rolls over on failure. |

### Bootstrap & trust

| Option | Description |
|--------|-------------|
| `WithHostBootstrapPSK(psk []byte)` | Bootstrap to the runtime with a host pre-shared key. |
| `WithHostBootstrapJWTFile(path string)` | Bootstrap with a JWT read fresh from a file on each bootstrap (picks up rotated tokens). |
| `WithHostBootstrapJWT(token string)` | Bootstrap with a static JWT (mainly for tests). |
| `WithPinnedCA(caPEM ...[]byte)` | Pin one or more PEM-encoded cluster CA certificates trusted before the first connection. Recommended. |
| `WithUnsafeNoPinnedCA()` | Opt out of CA pinning, trusting the runtime on first connection. **Unsafe** — development only. |

Exactly one of `WithPinnedCA` or `WithUnsafeNoPinnedCA` must be set. Exactly one bootstrap method must be set.

### Behavior & limits

| Option | Description |
|--------|-------------|
| `WithLogger(logger *slog.Logger)` | Provide an `slog` logger. |
| `WithShutdownGracePeriod(d time.Duration)` | Grace period for a clean shutdown. |
| `WithRequestTimeout(d time.Duration)` | Timeout for individual requests sent to the runtime. |
| `WithMaxInFlightRequests(n int)` | Concurrent peer invocations processed per session before excess calls are rejected with a retryable overloaded error. |
| `WithMaxRequestBodySize(n int64)` | Maximum size, in bytes, of a streamed peer invocation request body. |

## Register-actor options

`RegisterActorOptions` is passed to `host.RegisterActor` and is the same type in both topologies:

| Field | Default | Description |
|-------|---------|-------------|
| `IdleTimeout` | `5m` | Idle time before an actor is deactivated. Negative disables it. |
| `DeactivationTimeout` | `5s` | Maximum time allowed for `Deactivate` to run. |
| `ConcurrencyLimit` | `0` (unlimited) | Maximum active actors of this type per host. |
| `MaxAttempts` | `3` | Maximum attempts when invoking the actor or running an alarm. |
| `InitialRetryDelay` | `2s` | Initial retry delay (with backoff) after a failed invocation. |

## Provider options

The SQLite and PostgreSQL provider options accept:

| Field | Applies to | Description |
|-------|------------|-------------|
| `ConnectionString` | SQLite, Postgres | Connection string or file path used to open a new connection. |
| `DB` | SQLite (`*sql.DB`), Postgres (`*pgxpool.Pool`) | Use an existing connection instead of opening one. |
| `Timeout` | SQLite, Postgres | Timeout for database queries. |
| `CleanupInterval` | SQLite, Postgres, memory | Interval for purging expired state and alarms. |

> When using SQLite, the database file must not be stored on a networked filesystem (NFS/SMB).
