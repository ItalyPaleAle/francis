# Example actor worker

This folder contains a sample app that includes:

- An embedded actor host
  - Includes the actor type "myactor"
  - Uses SQLite as data store
- A control server that allows invoking actors

## How to run

Start two instances of the control app:

```sh
# Terminal 1
go run . -worker-address 127.0.0.1:8081 -actor-host-address 127.0.0.1:7571

# Terminal 2
go run . -worker-address 127.0.0.1:8082 -actor-host-address 127.0.0.1:7572
```

The control servers run on ports 8081 and 8082. You can perform operations on the actor by invoking either of the two endpoints. Examples include (run these on a different terminal window):

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

> Regardless of which control server you use, some actors will be activated on the first host, and others on the second.

The actors have an idle timeout of 10s, so after 10s of inactivity they get deallocated automatically. You will see it logged in the terminal. Invoking the actors again will cause them to be re-activated on any host.

You can also schedule alarms, which are executed at a future point in time:

```sh
# Schedules an alarm to be executed right away (due time is the current time) and every 60s, until 20281'-08
curl -v -X POST http://localhost:8081/alarm/myactor/actor1/alarm1 --data '{"dueTime":"'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'","interval":"60s","ttl":"2028-10-08T10:00:02Z","data": {"Hello": "World"}}'
```
