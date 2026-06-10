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

## Scenarios

Scenarios live under `suites/` and self-register via `init()`:

- **state** / **statecrud** — single-host state: a basic round-trip across every provider, plus full CRUD coverage (get/set/update/delete, missing-key and TTL handling, per-actor isolation).
- **crosshost** — two hosts sharing one backend, exercising cross-host placement and shared state.
- **invocation** — placement and concurrency: the per-host limit on active actors of a kind (`invocation-capacity`), turn-based serialization of calls to one actor (`invocation-turnbased`), per-actor (not global) locking so distinct actors run in parallel (`invocation-parallel`), and serialization plus state consistency for one actor called concurrently from multiple hosts (`invocation-crosshost`).
- **alarms** — the alarm lifecycle: one-shot and repeating alarms, a repeating alarm bounded by a TTL deadline, editing and deleting, transient and persistent execution failures with retry and removal, and fetching many alarms across batches.
- **alarminvoke** — the interaction between alarms and invocations: that an alarm and an invocation on one actor share its turn and never overlap, and that an actor can schedule an alarm on itself from inside an invocation.
- **lifecycle** — idle deactivation and the Deactivate hook, halting, and that state survives deactivation and reactivation.
- **errorprop** — that an error returned by an actor propagates back to the caller, including across a peer or runtime hop.
- **streaming** — streamed invocation (`InvokeStream`): small and large bodies, and a cross-host case, round-trip through the actor with the response content type preserved.
- **routing** — that invoking an unregistered actor type fails with `ErrNoHost`.
- **failover** — resilience: an actor is re-placed with its state intact when its host is stopped, and a repeating alarm migrates to a surviving host.
- **durability** — what survives a host restart on the local runtime: persistent providers keep an actor's state and alarms, while the in-memory provider does not.

Alarm scenarios set `cluster.Options.AlarmsPollInterval` to poll quickly instead of waiting on the multi-second component defaults; the cluster applies it to the local hosts or the remote runtime depending on the topology. Resilience scenarios use `Instance.Stop` to shut a host down mid-test and `Instance.Run`/`Instance.Rebind` to bring it back up.

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
