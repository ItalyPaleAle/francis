# Integration / E2E test harness

This directory contains Francis's integration and end-to-end tests. The harness spins up real actor **hosts** in-process (by importing the `host/local` and `host/remote` packages directly) against any supported **provider**, on either runtime topology, then runs assertions through the live `actor.Service`.

## Runtime topologies

The harness can run every scenario against both runtimes:

- **local** — each host embeds its own provider (`host/local`). Hosts coordinate through a shared store, so multi-host requires a provider that coordinates across processes (SQLite or Postgres).
- **remote** — a standalone **runtime** (`runtime`, the `cmd/runtime` control plane) owns the provider and coordinates placement, state, and alarms. Stateless hosts (`host/remote`) connect to it over WebTransport. Because coordination lives in the runtime, any provider variant supports multiple hosts here.

## Running

```sh
# All scenarios (Postgres ones skip unless the env vars below are set):
make test-integration
# or
go test -tags integration -v -count=1 -timeout 15m ./tests/integration/...

# A single topology or scenario (names are TestIntegration/<scenario>/<kind>/<variant>):
go test -tags integration -v -run 'TestIntegration/crosshost/remote/' ./tests/integration/...
go test -tags integration -v -run 'TestIntegration/state/local/sqlite$' ./tests/integration/...
```

Postgres-backed scenarios require connection strings:

```sh
export TEST_POSTGRES_CONNSTRING="postgres://actors:actors@localhost:5432/actors"
export TEST_STANDALONE_POSTGRES_CONNSTRING="postgres://actors:actors@localhost:5432/actors"
```


## Writing a scenario

A scenario is a `framework.Case`.

- In `Setup` it builds a cluster for a chosen topology and provider, stashes it, and returns its processes.
- In `Run` it drives the running hosts and asserts.
- Register it from an `init()`.

```go
func init() { suite.Register(&myCase{}) }

type myCase struct {
    cluster *cluster.Cluster
}

func (c *myCase) Setup(t *testing.T) []framework.Option {
    // Use context from t.Context()
    c.cluster = cluster.New(t, cluster.Options{
        Kind:    cluster.Remote,
        Variant: provider.SQLite,
        Hosts:   2,
        Actors:  []frameworkhost.ActorReg{shared.CounterReg(time.Minute)},
    })
    return []framework.Option{framework.WithProcesses(c.cluster.Processes()...)}
}

func (c *myCase) Run(t *testing.T) {
    // assert against c.cluster.Service(0), c.cluster.Service(1), ...
    // Use context from t.Context()
}
```

To run the same scenario across topologies or providers, give the case `kind` and `variant` fields plus an explicit `Name()` (implementing `suite.Named`), and register one instance per combination — see `suites/state/state.go`.

## Injecting failures

`suites/faults` covers what happens when parts of the cluster break rather than shut down cleanly. Three knobs on `cluster.Options` make that possible, and they compose with everything else:

- **`RuntimeLinks` / `PeerLinks`** put a severable UDP relay (`framework/process/netfault`) in front of an endpoint. `RuntimeLinks` gives every host its own link to the runtime, so `c.RuntimeLink(t, i).Sever(t)` cuts host `i` off from the control plane while the runtime and the other hosts keep running. `PeerLinks` puts one in front of each host's peer server (the host binds one port and advertises the link's), so `c.PeerLink(t, i).Sever(t)` cuts host-to-host invocations to host `i` without touching its connection to the database or runtime. A severed link is a black hole — no resets, no refusals — which is what QUIC sees when a network is cut. `Restore(t)` puts it back.
- **`StallableProvider`** (SQLite only) hands every host its own database handle, so `c.StallProvider(t, i)` parks host `i`'s only connection and makes all of its provider calls block until `c.UnstallProvider(t, i)`. That is a busy database from one host's point of view, with every other host still reading and writing the same file. Combined with `Host(i).Stop(t)`, it is also how a scenario kills a host *silently*: the deregistration on the shutdown path never lands, so the cluster is left with a registration for a host that no longer exists.
- **`HostHealthCheckDeadline`, `AlarmsLeaseDuration`, `ProviderQueryTimeout`, `HostRequestTimeout`** shorten the timers that failure detection hangs off, so a scenario waits seconds rather than the production defaults. They are applied consistently to the provider, the hosts, and the runtime.

`Host(i).WaitExit(t, timeout)` completes the picture: it blocks until a host's `Run` returns *on its own*, which is how a scenario asserts that a host noticed its own failure and stopped instead of lingering.
