---
title: "Quickstart"
weight: 23
---

This quickstart builds a tiny app with a single actor type — a counter — running on the embedded **local** topology with a SQLite data store. No extra services are required.

By the end you'll have an actor host that you can invoke over HTTP.

## Prerequisites

- Go 1.26 or newer
- No external services: this quickstart uses an embedded SQLite database

## 1. Create the project

```sh
mkdir francis-quickstart && cd francis-quickstart
go mod init example.com/francis-quickstart
go get github.com/italypaleale/francis
```

## 2. Write the actor

Create `counter.go`. The actor keeps a counter in its durable state and exposes an `increment` method.

```go
package main

import (
	"context"
	"fmt"

	"github.com/italypaleale/francis/actor"
)

// Counter is our actor
type Counter struct {
	client actor.Client[counterState]
}

// counterState is the actor's durable state
type counterState struct {
	Count int64
}

// NewCounter is the factory Francis calls to activate a Counter
func NewCounter(actorID string, service *actor.Service) actor.Actor {
	return &Counter{
		client: actor.NewActorClient[counterState]("counter", actorID, service),
	}
}

// Invoke handles method calls to the actor
func (c *Counter) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	// Load the current state (returns a zero value the first time)
	state, err := c.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	// Update the counter based on the method
	switch method {
	case "increment":
		state.Count++
	case "reset":
		state.Count = 0
	}

	// Persist the new state
	err = c.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to set state: %w", err)
	}

	// Return the current count
	return state.Count, nil
}
```

## 3. Start a host

Create `main.go`. It creates a local host with a SQLite store, registers the `counter` actor type, and exposes a small HTTP server to invoke actors.

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/italypaleale/francis/host/local"
)

func main() {
	// Create a local host with an embedded SQLite data store
	h, err := local.NewHost(
		local.WithAddress("127.0.0.1:7571"),
		local.WithSQLiteProvider(local.SQLiteProviderOptions{
			ConnectionString: "data.db",
		}),
		// The runtime PSK derives the cluster CA used for host-to-host mTLS
		local.WithRuntimePSKs([]byte("change-me-please")),
	)
	if err != nil {
		log.Fatalf("failed to create host: %v", err)
	}

	// Register the counter actor type
	err = h.RegisterActor("counter", NewCounter, local.RegisterActorOptions{})
	if err != nil {
		log.Fatalf("failed to register actor: %v", err)
	}

	service := h.Service()

	// Expose an HTTP endpoint to invoke actors: POST /invoke/{id}/{method}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /invoke/{id}/{method}", func(w http.ResponseWriter, r *http.Request) {
		resp, err := service.Invoke(r.Context(), "counter", r.PathValue("id"), r.PathValue("method"), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var count int64
		if resp != nil {
			_ = resp.Decode(&count)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"count": count})
	})

	go func() {
		log.Println("control server listening on 127.0.0.1:8080")
		_ = http.ListenAndServe("127.0.0.1:8080", mux)
	}()

	// Run the host until the process is stopped
	err = h.Run(context.Background())
	if err != nil {
		log.Fatalf("host stopped with error: %v", err)
	}
}
```

> The `runtime PSK` is a shared secret from which the cluster CA is derived. In the local topology, every host that shares this key can authenticate to its peers with mTLS. Use a strong, secret value in production.

## 4. Run it

```sh
go run .
```

In another terminal, invoke some actors:

```sh
# Increment two different counter actors
curl -X POST http://127.0.0.1:8080/invoke/alice/increment
curl -X POST http://127.0.0.1:8080/invoke/alice/increment
curl -X POST http://127.0.0.1:8080/invoke/bob/increment
```

You'll see each actor track its own count:

```json
{"count":1}
{"count":2}
{"count":1}
```

Because state is persisted to `data.db`, the counts survive a restart: stop the process, run it again, and increment `alice` once more — you'll continue from where you left off.

## What just happened

- Each unique actor ID (`alice`, `bob`) is a separate actor with its own durable state.
- The first call for an ID **activated** the actor (running the factory); subsequent calls reused the activation.
- `SetState` persisted the counter to SQLite, so it outlives the actor and the process.

## Next steps

- Add **alarms** to run scheduled work — see [Alarms](/docs/alarms).
- Learn the full actor API in [Writing actors](/docs/writing-actors).
- Scale to multiple hosts and choose a deployment shape in [Topologies](/docs/topologies).
- Run a real cluster with the standalone runtime in [Deploying the runtime](/docs/deploying-the-runtime).
