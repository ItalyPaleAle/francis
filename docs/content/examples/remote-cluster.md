---
title: "Running a remote cluster"
weight: 32
---

This example runs a cluster using the **remote** topology: a standalone **runtime** owns the data store and coordinates placement, state, and alarms, while two **stateless** workers connect to it and host the actors.

It mirrors the [`examples/remote-worker`](https://github.com/ItalyPaleAle/francis/tree/main/examples/remote-worker) example in the repository. The actor code is identical to the [local cluster](/examples/local-cluster) example — only the host setup differs.

## What you'll run

- One **runtime** (the `cmd/runtime` binary) that owns a SQLite data store.
- Two **workers** (`host/remote`) that connect to the runtime, each serving the actor type `myactor` and exposing an HTTP control server.

## Configuring the runtime

The runtime is configured with a YAML file:

```yaml
# config.yaml
bind: "127.0.0.1:7400"

# Runtime PSKs derive the cluster CA — keep them secret, inject from the environment in production
runtimePSKs:
  - "example-runtime-psk-change-me"

# How joining hosts authenticate
# This example uses a shared host PSK; the workers must present the same value
bootstrap:
  method: psk
  hostPSK: "example-host-bootstrap-psk-change-me"

# Use SQLite as database
provider:
  connectionString: "data.db"

log:
  level: debug
```

Build the runtime binary and print the cluster CA so the workers can pin it:

```sh
go build -o bin/runtime github.com/italypaleale/francis/cmd/runtime

# Print the CA so workers can verify the runtime on first connection
# Loads config.yaml in the current directory
bin/runtime print-ca
```

## Creating a remote worker

A remote worker is a `host/remote` host. Unlike a local host, it doesn't embed a data store — it connects to the runtime:

```go
import (
	"time"

	"github.com/italypaleale/francis/host/remote"
)

// Shared secret the host proves to the runtime when it first joins
// Must match the runtime's bootstrap.hostPSK
const hostBootstrapPSK = "example-host-bootstrap-psk-change-me"

func runWorker(ctx context.Context) error {
	h, err := remote.NewHost(
		remote.WithAddress(actorHostAddress),               // peer address for host-to-host invocations
		remote.WithRuntimeAddresses(runtimeAddress),        // one or more runtime replicas
		remote.WithHostBootstrapPSK([]byte(hostBootstrapPSK)),
		remote.WithPinnedCA(caPEM),                         // pin the CA from "runtime print-ca"
		remote.WithShutdownGracePeriod(10 * time.Second),
	)
	if err != nil {
		return err
	}

	err = h.RegisterActor("myactor", NewMyActor, remote.RegisterActorOptions{
		IdleTimeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}

	service := h.Service()
	_ = service

	return h.Run(ctx)
}
```

> The repository example uses `remote.WithUnsafeNoPinnedCA()` to trust the runtime on first connection for simplicity. In production, pin the CA with `remote.WithPinnedCA` instead — see [Security](/docs/security).

## Running everything

Start the runtime, then two workers:

```sh
# Terminal 1 — the runtime
go run github.com/italypaleale/francis/cmd/runtime

# Terminal 2 — worker 1
go run /path/to/remote-worker -worker-address 127.0.0.1:8081 -actor-host-address 127.0.0.1:7571 -runtime-address 127.0.0.1:7400

# Terminal 3 — worker 2
go run /path/to/remote-worker -worker-address 127.0.0.1:8082 -actor-host-address 127.0.0.1:7572 -runtime-address 127.0.0.1:7400
```

## Invoking actors

The control API is the same as the local example — invoke through either worker:

```sh
curl -X POST http://localhost:8081/invoke/myactor/id1/increment --data '{"In": 42}'
curl -X POST http://localhost:8082/invoke/myactor/id2/increment --data '{"In": 42}'
```

The runtime decides which worker hosts each actor, so calls for the same ID always land on the same activation regardless of which worker you call.

## State survives restarts

Because the runtime persists state to SQLite (`data.db`), actor state outlives both the workers and the runtime. Invoke an actor a few times to build up its counter, restart the runtime, then invoke again — the counter continues from where it was.

## Full source

See [`examples/remote-worker`](https://github.com/ItalyPaleAle/francis/tree/main/examples/remote-worker) in the repository for the complete, runnable code, including a `supervisord` configuration that launches the runtime and both workers together.
