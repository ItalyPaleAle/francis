# Hosts, topologies & providers

How to construct and run a Francis host. Actor code (factories, `Invoke`, `Alarm`, state, jobs) is **identical** across topologies — only host construction changes. Both host packages expose the same shape: `NewHost(...)`, `RegisterActor(...)`, `RegisterSingletonActor(...)`, `RegisterBuiltInActor(...)`, `Service()`, `Ready()`, `Run(ctx)`.

## Choosing a topology

| | Local (`host/local`) | Remote (`host/remote`) |
|---|---|---|
| Data store | Embedded in each host | Owned by a standalone runtime |
| Coordination | Peer-to-peer via shared store | The runtime |
| Extra process | None | `cmd/runtime` control plane (1+ replicas) |
| Best for | Single-node, dev, small clusters (≤4) | Larger / auto-scaling clusters (5+) |

## Local host

Everything embedded; hosts sharing a data store + runtime PSK form a cluster and forward invocations over mTLS.

```go
import "github.com/italypaleale/francis/host/local"

h, err := local.NewHost(
	local.WithAddress("127.0.0.1:7571"),   // peer address advertised to other hosts
	local.WithSQLiteProvider(local.SQLiteProviderOptions{ConnectionString: "data.db"}),
	local.WithRuntimePSKs([]byte("change-me-please")),
	local.WithLogger(logger),              // optional *slog.Logger
	local.WithShutdownGracePeriod(10 * time.Second),
)
if err != nil {
	return err
}

err = h.RegisterActor("cart", NewCart, local.WithIdleTimeout(10*time.Minute))
if err != nil {
	return err
}

svc := h.Service()
err = h.Run(ctx)   // blocks until ctx cancelled + drained
```

### Local providers

| Provider | Option | Connection | Notes |
|---|---|---|---|
| SQLite | `WithSQLiteProvider(local.SQLiteProviderOptions{ConnectionString: "data.db"})` | file path | Single-node & dev. Not on NFS/SMB. |
| PostgreSQL | `WithPostgresProvider(local.PostgresProviderOptions{...})` | `postgres://…` | Multi-node clusters sharing one DB. |
| In-memory | `WithStandaloneMemoryProvider(standalone.StandaloneMemoryOptions{})` | — | Non-durable; tests / ephemeral. Lost on restart. |

Standalone variants wrap a DB connection you already own (`*sql.DB`):

```go
// e.g. persist the cronjob/ratelimit examples across restarts with your own SQLite handle
local.WithStandaloneSQLiteProvider(local.StandaloneSQLiteProviderOptions{DB: db})
// also: WithStandalonePostgresProvider(...)
```

Import for the standalone memory options type: `github.com/italypaleale/francis/components/standalone`.

> Multi-node local clusters must share a store all nodes can reach — use **PostgreSQL**. A single embedded SQLite file is effectively single-node.

## Remote host (worker)

The worker embeds no data store; it connects to the standalone runtime over WebTransport. Actor registration and `Run` are the same as local.

```go
import "github.com/italypaleale/francis/host/remote"

h, err := remote.NewHost(
	remote.WithAddress("127.0.0.1:7571"),
	remote.WithRuntimeAddresses("127.0.0.1:7400"),
	remote.WithHostBootstrapPSK([]byte("host-bootstrap-psk")),  // must match runtime's bootstrap.hostPSK
	remote.WithPinnedCA(caPEM),                                 // pin the cluster CA (recommended)
	remote.WithLogger(logger),
	remote.WithShutdownGracePeriod(10 * time.Second),
)
```

- **Bootstrap flow:** the host proves the shared `HostBootstrapPSK` to the runtime, which issues it a workload certificate; all later connections (to the runtime and to peer hosts) use mTLS.
- **Trust:** prefer `WithPinnedCA(caPEM)` in production (get the CA via `runtime print-ca`). `remote.WithUnsafeNoPinnedCA()` trusts the runtime on first connection — dev/examples only.

## Security model (both topologies)

- **Runtime PSK** (local): a shared secret from which the cluster CA is derived. Every host sharing the key self-issues its workload cert and can mTLS-authenticate peers. Use a strong secret; rotate deliberately.
- **Host bootstrap PSK** (remote): what a worker proves to the runtime to join. Can be a pre-shared key or JWT depending on runtime config.
- Host-to-host peer invocations are always mTLS.

## Running the standalone runtime (remote topology)

The runtime is the `cmd/runtime` binary, configured by a YAML file. It owns the data store and coordinates placement/state/alarms. It infers the backend from `provider.connectionString`: `postgres://` (or `postgresql://`) → PostgreSQL, `memory` → in-memory, anything else → SQLite file/DSN. Because the runtime persists state, actor state survives both worker and runtime restarts. See `docs/content/docs/deploying-the-runtime.md` for the config schema and `runtime print-ca` for the pinned CA.

## Lifecycle helpers

- `h.Ready()` returns a `<-chan struct{}` closed when the host can serve invocations — useful to gate work in tests/demos before invoking.
- `h.Run(ctx)` blocks until `ctx` is cancelled, then drains within the shutdown grace period. Wire `ctx` to SIGINT/SIGTERM (the examples use `go-kit/signals.SignalContext`).

## Switching topologies

Swap `host/local` for `host/remote` (or back) and adjust construction options. Factories, `Invoke`, `Alarm`, state, and job/alarm code do not change.

## See also (runnable in this repo)

- `examples/worker` — local host, control HTTP server, actor implementing every interface.
- `examples/remote-worker` — remote worker + `control-server.go` + `supervisord.conf`.
- `examples/cronjob`, `examples/ratelimit` — built-in actors with standalone providers.
