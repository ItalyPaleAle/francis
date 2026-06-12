---
title: "Running a local cluster"
weight: 31
---

This example runs a two-node cluster using the embedded **local** topology. Each worker embeds its own actor host and SQLite data store, and the workers coordinate peer-to-peer — there is no separate control plane process.

It mirrors the [`examples/worker`](https://github.com/ItalyPaleAle/francis/tree/main/examples/worker) example in the repository.

## What you'll run

- Two worker processes, each an actor host with an embedded SQLite store, both serving the actor type `myactor`.
- A small HTTP control server on each worker for invoking actors and scheduling alarms.

Both workers share a runtime PSK, so they authenticate to each other with mTLS and form one cluster.

## Creating the host

The heart of a local worker is constructing a `host/local` host and registering the actor type:

```go
import (
	"time"

	"github.com/italypaleale/francis/host/local"
)

// The shared cluster key from which the CA is derived
// Every host that shares this key authenticates its peers with mTLS
const runtimePSK = "example-runtime-psk-change-me-please"

func runWorker(ctx context.Context) error {
	h, err := local.NewHost(
		local.WithAddress(actorHostAddress), // peer address advertised to other hosts
		local.WithSQLiteProvider(local.SQLiteProviderOptions{
			ConnectionString: "data.db",
		}),
		local.WithRuntimePSKs([]byte(runtimePSK)),
		local.WithShutdownGracePeriod(10 * time.Second),
	)
	if err != nil {
		return err
	}

	// Register actors before calling Run
	err = h.RegisterActor("myactor", NewMyActor, local.RegisterActorOptions{
		IdleTimeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}

	// h.Service() gives you the handle to invoke actors and manage state/alarms
	service := h.Service()

	// Run the host (and your control server) until the context is cancelled
	// ...
	return h.Run(ctx)
}
```

In a real app you typically run the host alongside your own server (an HTTP API, gRPC, a queue consumer, etc.). The repository example uses a small HTTP control server so you can invoke actors with `curl`.

## Running two workers

Each worker writes its SQLite database to `data.db` in its working directory, so give each one its own directory:

```sh
mkdir -p worker1 worker2

# Terminal 1
cd worker1 && go run /path/to/worker -worker-address 127.0.0.1:8081 -actor-host-address 127.0.0.1:7571

# Terminal 2
cd worker2 && go run /path/to/worker -worker-address 127.0.0.1:8082 -actor-host-address 127.0.0.1:7572
```

- `-actor-host-address` is the **peer** address a host advertises so other hosts can forward invocations to it.
- `-worker-address` is the worker's own HTTP control server.

## Invoking actors

You can invoke an actor through **either** worker; Francis routes the call to whichever host owns the actor, activating it on demand:

```sh
# Invoke four different actors of type "myactor" through worker 1
curl -X POST http://localhost:8081/invoke/myactor/id1/increment --data '{"In": 42}'
curl -X POST http://localhost:8081/invoke/myactor/id2/increment --data '{"In": 42}'

# The same actors are reachable through worker 2
curl -X POST http://localhost:8082/invoke/myactor/id1/increment --data '{"In": 42}'
```

Placement is single-activation: some actors end up hosted on worker 1 and others on worker 2, but every call for a given ID is routed to the one host that owns it — no matter which worker you call.

After the configured idle timeout (10s here), idle actors are deactivated automatically; the next call re-activates them on any host.

## Scheduling alarms

The control server also exposes alarm scheduling:

```sh
# Fire right away (dueTime = now), then repeat every 60s until 2028-10-08
curl -X POST http://localhost:8081/alarm/myactor/actor1/alarm1 \
  --data '{"dueTime":"'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'","interval":"60s","ttl":"2028-10-08T10:00:02Z","data":{"Hello":"World"}}'
```

When the alarm fires, Francis activates `myactor`/`actor1` (if needed) and calls its `Alarm` method.

## Full source

See [`examples/worker`](https://github.com/ItalyPaleAle/francis/tree/main/examples/worker) in the repository for the complete, runnable code, including the HTTP control server and a `supervisord` configuration that launches both workers together.
