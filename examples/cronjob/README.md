# Cron job example

A minimal Francis app that registers a [built-in cron job actor](../../docs/content/docs/builtin-actors.md) which prints a line to the console every 10 seconds.

The cron job is a cluster-wide singleton: register it on every host and it still runs on only one node at a time. This example runs a single host backed by a standalone provider, so can't run with multiple replicas.

## Run

```sh
go run ./examples/cronjob
```

You should see a tick right away (thanks to `WithImmediate`) and then one every 10 seconds:

```
level=INFO msg="Cron job tick" at=2026-06-30T12:00:00Z
level=INFO msg="Cron job tick" at=2026-06-30T12:00:10Z
level=INFO msg="Cron job tick" at=2026-06-30T12:00:20Z
```

## How it works

`cronjob.New` builds the job, and `host.RegisterBuiltInActor` registers it on the host before it starts. Once the host is ready it bootstraps the actor, which sets up a single durable repeating job that drives the schedule.

See the [built-in actors docs](../../docs/content/docs/builtin-actors.md) for the schedule options (`WithInterval`, `WithPeriod`, `WithCron`, `WithImmediate`) and for triggering a run on demand with `Message`.
