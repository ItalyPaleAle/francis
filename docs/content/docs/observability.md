---
title: "Observability"
weight: 30
---

Francis is instrumented with [OpenTelemetry](https://opentelemetry.io). A single distributed trace follows a request end-to-end as it crosses hosts and the runtime, so you can see exactly where time goes: the actor invocation, the cross-host or host-to-runtime call, the turn-based lock wait, the actor's own method, state operations, placement lookups, and alarms.

Instrumentation is built into the framework and is always present, but it does nothing until OpenTelemetry is configured. With no exporter configured, spans are created against a no-op tracer with negligible overhead, so there is nothing to turn off in development.

## What gets traced

A trace is assembled from spans created at every meaningful boundary. The main span names are:

| Span | Where it runs | What it covers |
|------|---------------|----------------|
| `actor.invoke` / `actor.invoke.stream` | Caller's host | The whole logical invocation, including placement resolution and the single stale-placement retry |
| `actor.lock` | Owning host | Waiting for the actor's turn (turn-based concurrency) |
| `actor.execute` | Owning host | The actor's own `Invoke`/`InvokeStream` method |
| `actor.activate` | Owning host | Cold-start activation of an actor instance |
| `actor.deactivate` | Owning host | Halting and deactivating an idle or shutting-down actor |
| `placement.lookup` | Host or runtime | Resolving an actor's placement through the provider |
| `state.get` / `state.set` / `state.delete` | Host | An actor state operation |
| `alarm.dispatch` / `alarm.execute` | Runtime / host | Dispatching a due alarm and running its handler |
| `rpc.peer.invoke` | Both hosts | A host-to-host actor invocation (client and server spans) |
| `rpc.runtime.*` / `rpc.host.*` / `runtime.*` | Host / runtime | Host-to-runtime and runtime-to-host requests |

Spans are tagged with attributes such as `francis.actor.type`, `francis.actor.id`, `francis.actor.method`, `francis.request.id`, `francis.host.id`, and `francis.peer.address`.

> Note: alarms start their own trace.  
> A fired alarm is not triggered by a caller request, so `alarm.dispatch` and `alarm.execute` begin a fresh trace rather than attaching to an unrelated one. The trace still spans the runtime and the owning host.

## Trace context propagation

Francis propagates the [W3C Trace Context](https://www.w3.org/TR/trace-context/) on every request it sends between hosts and the runtime, so a trace started on one host continues seamlessly on another. Propagation uses the globally configured OpenTelemetry propagator; it is a no-op when none is set, so nothing is written to the wire unless tracing is enabled.

## Standalone runtime

The standalone runtime binary exports traces, metrics, and logs through the standard [`OTEL_*` environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/). **Export is opt-in**: each signal stays off until you set its exporter.

```sh
# Send traces to an OTLP collector
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT="http://collector:4318"

# Optionally enable metrics and logs the same way
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
```

Common values:

| Variable | Purpose |
|----------|---------|
| `OTEL_TRACES_EXPORTER` | `otlp`, `console`, or `none` (default). Also `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector endpoint for the OTLP exporters. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` or `grpc`. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Extra headers, e.g. for authentication. |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | Head sampling, e.g. `parentbased_traceidratio` with arg `0.1`. |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes, e.g. `service.instance.id=...`. |

### Tuning trace volume

In production you usually don't want every internal span for every request, so control the volume with the sampler rather than turning instrumentation off:

```sh
# Keep ~10% of traces, respecting the decision made upstream
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.1
```

Because the decision is parent-based, a trace that was sampled on a calling host stays sampled as it flows into the runtime and on to a peer host.

### Logs

Regardless of OTLP export, the runtime always writes logs to standard output. The format is controlled by the [runtime configuration](/docs/deploying-the-runtime#configuration):

```yaml
log:
  level: info   # debug, info, warn, or error
  json: true    # structured JSON instead of text
```

When `OTEL_LOGS_EXPORTER` is set, log records are also exported via OpenTelemetry and carry the active trace and span IDs, so logs correlate with traces.

### Metrics

When `OTEL_METRICS_EXPORTER` is set, the runtime emits:

| Metric | Type | Description |
|--------|------|-------------|
| `francis.runtime.rpc.requests` | Counter | Host requests handled, tagged by message `kind` |
| `francis.runtime.rpc.duration` | Histogram (s) | Time spent handling each request |
| `francis.runtime.alarms.executed` | Counter | Alarms dispatched and completed |
| `francis.runtime.hosts.connected` | Up-down counter | Hosts currently connected |

## Embedded (local) topology

In the [local topology](/docs/topologies#local-topology) your application owns the process, so it also owns the OpenTelemetry setup. Francis uses the **global** tracer and propagator, so it automatically picks up whatever your app configures.

Configure the OpenTelemetry SDK as usual, then install a trace-context propagator so spans flow across hosts:

```go
import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ... build and set your TracerProvider (exporter, sampler, resource) ...
otel.SetTracerProvider(tp)

// Install the W3C trace context propagator so traces flow across hosts
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
))
```

If your app never configures OpenTelemetry, Francis's spans are no-ops and nothing is propagated.

> Note: don't forget the propagator  
> Setting only a `TracerProvider` records spans within a single host but does not carry the trace across hosts. Install a `TraceContext` propagator (as above) for end-to-end distributed traces.
