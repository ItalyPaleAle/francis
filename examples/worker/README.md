# Example actor worker (local runtime)

This folder contains a sample app that uses the **local** runtime topology. It is the
counterpart to [`../remote-worker`](../remote-worker), which uses the remote runtime.

The local topology embeds everything into a single process: each worker contains its own
actor host *and* its own data store (SQLite). Workers discover and invoke each other
peer-to-peer; there is no separate control plane process.

The sample includes:

- An actor host with an embedded SQLite data store
  - Registers the actor type "myactor"
- A control server that allows invoking actors and scheduling alarms

## How to run

Start two instances of the worker:

```sh
# Terminal 1
go run . -worker-address 127.0.0.1:8081 -actor-host-address 127.0.0.1:7571

# Terminal 2
go run . -worker-address 127.0.0.1:8082 -actor-host-address 127.0.0.1:7572
```

The control servers run on ports 8081 and 8082. You can perform operations on the actor
by invoking either endpoint (run these in a separate terminal):

```sh
# Invoke 4 different actors of type "myactor"
# This uses the control server of the first worker
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

> Regardless of which control server you use, some actors will be activated on the first
> host and others on the second — placement is decided by each host's embedded data store.

The actors have an idle timeout of 10s, so after 10s of inactivity they get deallocated
automatically. You will see it logged in the terminal. Invoking the actors again will
cause them to be re-activated on any host.

You can also schedule alarms, which are executed at a future point in time:

```sh
# Schedules an alarm to be executed right away (due time is the current time) and every 60s, until 2028-10-08
curl -v -X POST http://localhost:8081/alarm/myactor/actor1/alarm1 --data '{"dueTime":"'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'","interval":"60s","ttl":"2028-10-08T10:00:02Z","data": {"Hello": "World"}}'
```
