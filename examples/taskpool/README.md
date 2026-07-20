# Example task pool (local runtime)

This folder contains a sample app that uses the built-in **task pool** actor to run a distributed pool of workers, processing a bounded number of long-running tasks per host and scaling out across the cluster.

The example models a video-conversion service: each submitted video is a task, each host converts at most `WithConcurrency` videos at a time, and more hosts mean more videos convert in parallel.

## What it shows

- A task pool named `video-convert` with a strict per-host limit of **2** concurrent tasks
- The host advertising a **`gpu`** capability, so it can run tasks that require a GPU as well as plain tasks
- Submitting plain tasks (any host can run them) and one GPU task (only hosts advertising `gpu` run it)
- A long-running handler that respects context cancellation, so a draining host stops promptly

## How to run

```sh
go run .
```

The app starts a single-host local cluster with an embedded SQLite store, submits a batch of tasks, and logs each conversion as it runs. You will see at most two conversions in flight at once. Press Ctrl+C to drain and exit.

To see work spread across the cluster, run more hosts that share the same database and runtime PSK: each host runs at most its own `WithConcurrency` tasks at once, so total parallelism grows with the number of hosts.

## Communicating results

A task pool is **fire-and-forget**: it runs your handler and records failures (retries, then dead-letters), but it does **not** capture or return a task's result. Communicating the outcome is your handler's responsibility. Depending on your app, you might:

- write a row to a database,
- call an external API or webhook,
- upload the output to object storage, or
- invoke another actor with the result.

In this example the handler just logs that it finished; a real app would do one of the above where the comment says so in `main.go`.

## Capabilities

A task can optionally require a capability (such as `gpu`). A host advertises the capabilities it has with `WithCapability`, and a task that requires one is only ever run on a host that advertises it. A task with no required capability runs on any host — so a plain, CPU-only host does not need to declare anything.

If you submit a task requiring a capability that no host advertises, it stays **pending** (visible via `GetTask`) until a host that has it joins the cluster, rather than failing.
