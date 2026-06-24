# Example actor worker (remote runtime)

This folder contains a sample app that uses the **remote** runtime topology. It is the
counterpart to [`../worker`](../worker), which uses the embedded **local** runtime.

The difference is where the data store and coordination live:

| | `../worker` (local) | this example (remote) |
|---|---|---|
| Data store | Embedded in each worker (SQLite) | Owned by a standalone runtime |
| Placement / state / alarms | Decided inside each host | Coordinated by the runtime |
| Worker process | Self-contained | Stateless; connects to the runtime |

The setup here is:

- A **runtime** — the standalone control plane. It owns the SQLite data store and
  coordinates placement, state, and alarms. This is the existing `cmd/runtime` binary,
  configured by [`config.yaml`](./config.yaml).
- Two **workers** — stateless actor hosts that connect to the runtime over WebTransport.
  Each one hosts the actor type "myactor" and exposes a control server for invoking actors.
  The worker code is identical to `../worker` except it uses `host/remote` (which connects
  to a runtime) instead of `host/local` (which embeds a data store).

Because this requires running multiple processes, the example uses
[supervisord](http://supervisord.org/) to launch them all together.

## Configuration

The runtime is configured by `config.yaml` (create it next to `supervisord.conf`):

```yaml
bind: "127.0.0.1:7400"

# The runtime PSKs derive the cluster CA: every runtime sharing these keys is the same issuer
# Keep them secret
runtimePSKs:
  - "example-runtime-psk-change-me"

# How joining hosts authenticate
# This example uses a shared host PSK, which must match the worker's hostBootstrapPSK in main.go
# (The other option is "jwt", validated against a JWKS)
bootstrap:
  method: psk
  hostPSK: "example-host-bootstrap-psk-change-me"

# Use a SQLite backend
provider:
  connectionString: "data.db"

log:
  level: debug
```

The runtime discovers `config.yaml` from its working directory, so the commands below run from this example's directory without any flag. (You can also point it at any path with the `FRANCIS_CONFIG` environment variable.)

Once a host bootstraps, the runtime issues it a short-lived workload certificate and all
later connections (to the runtime and between peer hosts) use mTLS. To close the
first-connection trust gap, print the CA and pin it on the workers:

```sh
# the worker can pass the output to remote.WithPinnedCA
bin/runtime print-ca
```

## How to run (with supervisord)

First build the two binaries:

```sh
go build -o bin/runtime ../../cmd/runtime
go build -o bin/worker .
```

Then start the cluster:

```sh
supervisord -c supervisord.conf
```

This starts the runtime (on `127.0.0.1:7400`) plus two workers, whose control servers
listen on ports 8081 and 8082. Logs from all three processes are streamed to the console.

You can manage the processes with `supervisorctl`, e.g.:

```sh
supervisorctl -c supervisord.conf status
supervisorctl -c supervisord.conf restart runtime
```

## How to run (manually, without supervisord)

Start the runtime in one terminal (it loads `config.yaml` from the current directory):

```sh
go run ../../cmd/runtime
```

Then start two workers in separate terminals:

```sh
# Terminal 2
go run . -worker-address 127.0.0.1:8081 -actor-host-address 127.0.0.1:7571 -runtime-address 127.0.0.1:7400

# Terminal 3
go run . -worker-address 127.0.0.1:8082 -actor-host-address 127.0.0.1:7572 -runtime-address 127.0.0.1:7400
```

## Invoking actors

The control API is identical to the local example. You can perform operations on the
actor by invoking either control server (port 8081 or 8082):

```sh
# Invoke 4 different actors of type "myactor"
curl http://localhost:8081/invoke/myactor/id1/increment -X POST --data '{"In": 42}'
curl http://localhost:8081/invoke/myactor/id2/increment -X POST --data '{"In": 42}'
curl http://localhost:8081/invoke/myactor/id3/increment -X POST --data '{"In": 42}'
curl http://localhost:8081/invoke/myactor/id4/increment -X POST --data '{"In": 42}'

# Invoking using the control server of the second worker
curl http://localhost:8082/invoke/myactor/id1/increment -X POST --data '{"In": 42}'
curl http://localhost:8082/invoke/myactor/id2/increment -X POST --data '{"In": 42}'
curl http://localhost:8082/invoke/myactor/id3/increment -X POST --data '{"In": 42}'
curl http://localhost:8082/invoke/myactor/id4/increment -X POST --data '{"In": 42}'
```

> Regardless of which control server you use, the runtime decides which worker hosts each
> actor, so some actors will be activated on the first worker and others on the second.

The actors have an idle timeout of 10s, so after 10s of inactivity they get deallocated
automatically. Invoking them again re-activates them on any worker.

You can also schedule alarms, which are executed at a future point in time:

```sh
# Schedules an alarm to be executed right away (due time is the current time) and every 60s, until 2028-10-08
curl -v -X POST http://localhost:8081/alarm/myactor/actor1/alarm1 --data '{"dueTime":"'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'","interval":"60s","ttl":"2028-10-08T10:00:02Z","data": {"Hello": "World"}}'
```

Because the runtime persists state to SQLite (`data.db`), actor state survives a runtime
restart — restart the runtime (`supervisorctl -c supervisord.conf restart runtime`) and
the counters from earlier invocations are still there.
