---
title: "Management API"
weight: 31
---

Francis can serve an optional **management REST API**. You can use it to look at the cluster (hosts, placements, live activations, actor state, jobs, alarms, and workflow instances) and to run administrative actions: deactivating an actor, draining a host, and cancelling, suspending, or resuming a workflow instance.

The API is **disabled by default** in both [topologies](/docs/topologies). Until you enable it, no TCP port is opened.

## Enabling the API

### Remote topology

In the remote topology, the [runtime](/docs/deploying-the-runtime) serves the API. Enable it in the runtime's configuration file:

```yaml
management:
  enabled: true
  # TCP address the API listens on, default "0.0.0.0:7401" (all interfaces)
  bind: "0.0.0.0:7401"
  # Tokens that can read everything but perform no actions
  readOnlyTokens:
    - "<a random string of at least 32 characters>"
  # Tokens that can also perform actions
  managementTokens:
    - "<another random string of at least 32 characters>"
  # Optional: serve HTTPS directly (set both or neither)
  tls:
    certFile: "/etc/francis/management.crt"
    keyFile: "/etc/francis/management.key"
```

Every replica serves the whole cluster. When a request concerns a host that's connected to a different replica, the replica forwards it to that one over the runtime's existing UDP port. This requires PostgreSQL (the only provider that supports multiple replicas), an `advertiseAddress` on each replica, and UDP connectivity between replicas: see [Running multiple runtime replicas](/docs/deploying-the-runtime#running-multiple-runtime-replicas).

With the [Helm chart](https://github.com/ItalyPaleAle/francis/tree/main/charts/francis), set `management.enabled` and the token lists in your values. The chart binds the API on all interfaces, adds a TCP container port and a separate `<release>-francis-management` Service, and can serve HTTPS from a `kubernetes.io/tls` Secret with `management.tls.existingSecret`. If you supply the configuration with `existingConfigSecret`, you must add the `management` block to that Secret yourself. The chart's README has the details.

### Local topology

In the local topology, any host can serve the API with the `local.WithManagementAPI` option:

```go
h, err := local.NewHost(
	local.WithManagementAPI(local.ManagementOptions{
		Bind:             "0.0.0.0:7401",
		ReadOnlyTokens:   []string{readOnlyToken},
		ManagementTokens: []string{managementToken},
		// Optional: serve HTTPS directly
		TLSConfig: tlsConfig,
	}),
	// ...
)
```

Without this option the host starts no listener. The options follow the same rules as the runtime's configuration, except that `Bind` defaults to `127.0.0.1:7401`, which only accepts connections from the same machine. Your application is responsible for sourcing the tokens.

You only need to enable the API on one host: that host can serve the whole cluster. It reads durable data from the provider and reaches the other hosts through their existing peer ports, which local hosts already need to be able to reach each other on. Every host answers these internal requests whether or not it enables its own listener. The management port is an additional TCP port, only on the hosts that enable it.

## Security

The API exposes actor state and workflow inputs and outputs, so treat access to it like access to the database.

- **Tokens.** Callers authenticate with a bearer token in the `Authorization` header. Tokens are never accepted in the query string. When the API is enabled, at least one token is required. Every token must be at least 32 characters long, and the same token can't appear twice across the two lists. Tokens are compared in constant time.
- **Rotation.** Each list accepts multiple tokens. To rotate one, add the new token, update your clients, then remove the old one.
- **Audit logs.** Actions, and reads of actor state and workflow input and output, are logged with the request ID, the target, and the last 5 characters of the token that was used. The full token is never logged, and neither are state or workflow payloads. Different tokens can end with the same 5 characters, so the suffix helps you tell tokens apart but isn't a unique key.
- **TLS.** TLS is optional. The runtime listens on all interfaces by default (`0.0.0.0:7401`), so the API is reachable from outside its container, while a local host listens on `127.0.0.1:7401` unless you set `Bind`. If the API listens on an address other machines can reach without TLS configured, the server logs a warning at startup: in that case, terminate TLS in a proxy in front of it, or configure a certificate.
- **Separate credentials.** Management tokens are independent of the runtime PSKs and the host bootstrap credentials. Traffic between runtime replicas, and between local hosts, is authenticated with their certificates and never with management tokens.

### Scopes

Every route requires a scope. Tokens in `readOnlyTokens` receive every scope except those ending in `:manage`, and tokens in `managementTokens` receive every scope.

| Scope | Grants |
| --- | --- |
| `cluster:read` | Cluster summary, runtimes, and hosts |
| `actors:read` | Live activations, placements, actor types, and the list of actors with stored state |
| `actors:state:read` | The durable state of an actor, including built-in actor types |
| `workflows:read` | Workflows, instance metadata, and instance event history |
| `workflows:data:read` | Workflow input and output in instance details |
| `jobs:read` | Jobs and alarms |
| `actors:manage` | Deactivating an actor |
| `hosts:manage` | Draining a host |
| `workflows:manage` | Cancelling, suspending, and resuming a workflow instance |

A request without a valid token gets `401`, and a token that lacks the route's scope gets `403`.

## Endpoints

Every route is under `/api/v1`, except `GET /healthz`. The OpenAPI description of the API is served at `/api/v1/openapi.yaml`. Both of those routes are public and need no token.

Path segments must be percent-encoded. This matters for workflow child instance IDs, which contain `|`, and for actor IDs, which may contain any character other than `/`.

### Cluster and hosts

| Route | Scope | Description |
| --- | --- | --- |
| `GET /api/v1/cluster/summary` | `cluster:read` | Counts of runtimes, hosts by state, placements, live activations, jobs, and workflow instances by status, plus the holder of the cluster's exclusive-access lease when one is held. Counts stop at 10,000 and are then flagged as `truncated`. |
| `GET /api/v1/runtimes` | `cluster:read` | Runtime replicas with a live membership, their advertised addresses, and last heartbeat. Remote topology only. |
| `GET /api/v1/hosts` | `cluster:read` | Paginated hosts, with their state (`connected`, `draining`, or, in the remote topology, `unreachable`), last health check, owner runtime, and actor counts. Filter with `state`. |
| `GET /api/v1/hosts/{hostId}` | `cluster:read` | One host's registration, actor types with their placement limits and usage, and the capacity, drain state, and workflow definitions the host reports. |
| `GET /api/v1/hosts/{hostId}/activations` | `actors:read` | Paginated actors that are active in memory on one host right now, read from the host itself. Filter with `type`. |
| `POST /api/v1/hosts/{hostId}/drain` | `hosts:manage` | Drain a host so it deactivates its actors and shuts down. See [Draining a host](#draining-a-host). |

Only hosts with a live registration are listed. A host whose health check is older than the deadline is removed by the provider, so there's no "expired" state.

### Actors

| Route | Scope | Description |
| --- | --- | --- |
| `GET /api/v1/activations` | `actors:read` | Paginated, cluster-wide list of active actors, collected from every reachable host. Filter with `host` and `type`. |
| `GET /api/v1/placements` | `actors:read` | Paginated placements recorded in the provider (actor type, ID, host, and idle timeout). Filter with `host` and `type`. |
| `GET /api/v1/actor-types` | `actors:read` | Registered actor types, the hosts serving each, placement usage and limits, job retention, and execution capacity groups. |
| `GET /api/v1/actor-states?type={type}` | `actors:read` | Paginated IDs of the actors of a type that have stored state, whether or not they're active. `type` is required. |
| `GET /api/v1/actor-states/{type}/{id}` | `actors:state:read` | An actor's stored state. See [Actor state](#actor-state). |
| `POST /api/v1/actors/{type}/{id}/deactivate` | `actors:manage` | Ask the host an actor is active on to deactivate it. See [Deactivating an actor](#deactivating-an-actor). |

### Jobs and alarms

| Route | Scope | Description |
| --- | --- | --- |
| `GET /api/v1/jobs` | `jobs:read` | Paginated [jobs](/docs/jobs): pending, active, and, while their actor type retains them, completed and dead-lettered. Filter with `type`, `id`, and `status` (`pending`, `active`, `completed`, or `dead`). |
| `GET /api/v1/jobs/{jobId}` | `jobs:read` | One job. Terminal jobs include their attempts, last error, and end time. |
| `GET /api/v1/alarms` | `jobs:read` | Paginated [alarms](/docs/alarms), excluding jobs, with due time, repeat interval, and lease state. Filter with `type` and `id`. |

### Workflows

| Route | Scope | Description |
| --- | --- | --- |
| `GET /api/v1/workflows` | `workflows:read` | Known workflows, their registered versions and definition fingerprints, the hosts serving them, and any definition conflicts. |
| `GET /api/v1/workflows/{name}/instances` | `workflows:read` | Paginated instances of a workflow, with status, version, parent, and creation time. Filter with `status`, `version`, `parent`, `createdFrom`, and `createdTo` (RFC 3339). |
| `GET /api/v1/workflows/{name}/instances/{instanceId}` | `workflows:read` | An instance's status, steps with their timings, child instances, compensation outcome, and dead-lettered jobs. With `workflows:data:read` it also includes the input and output. |
| `GET /api/v1/workflows/{name}/instances/{instanceId}/events` | `workflows:read` | Paginated event history of an instance, in order. |
| `POST /api/v1/workflows/{name}/instances/{instanceId}/cancel` | `workflows:manage` | Cancel an instance. A `reason` is required. |
| `POST /api/v1/workflows/{name}/instances/{instanceId}/suspend` | `workflows:manage` | Suspend an instance, with an optional `reason`. |
| `POST /api/v1/workflows/{name}/instances/{instanceId}/resume` | `workflows:manage` | Resume a suspended instance. Takes no reason. |

Instance details include `hasOutput`, which tells apart an instance with no output from one whose output is JSON `null`. Callers without `workflows:data:read` get `dataRedacted: true` instead of the input and output. Reading input and output is recorded in the audit log.

A pending instance (one that hasn't started yet) is reported as long as its start job is live. The instance list reads only labels, so a pending instance whose start job was dead-lettered can stay in the list as `pending` for a while after its detail route has started returning `404`.

Event history is recorded for every workflow by default. A workflow can opt out with the `workflow.WithoutEventHistory()` option, because the history adds a write per transition and wide fan-outs produce many events. For a workflow that opted out, the events route returns `404` with the code `eventHistoryDisabled`.

The API reads stored data only, so completed jobs and finished workflow instances are only visible while they're retained.

## Examples

List the hosts:

```sh
curl -H "Authorization: Bearer $FRANCIS_TOKEN" \
  http://127.0.0.1:7401/api/v1/hosts
```

Read an actor's state as JSON, then as the exact stored bytes:

```sh
curl -H "Authorization: Bearer $FRANCIS_TOKEN" \
  http://127.0.0.1:7401/api/v1/actor-states/counter/my-counter

curl -H "Authorization: Bearer $FRANCIS_TOKEN" \
  -H "Accept: application/msgpack" \
  -o state.msgpack \
  http://127.0.0.1:7401/api/v1/actor-states/counter/my-counter
```

List the failed instances of a workflow:

```sh
curl -H "Authorization: Bearer $FRANCIS_TOKEN" \
  "http://127.0.0.1:7401/api/v1/workflows/order/instances?status=failed&limit=50"
```

Cancel a workflow instance:

```sh
curl -X POST \
  -H "Authorization: Bearer $FRANCIS_MANAGEMENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Duplicate order"}' \
  http://127.0.0.1:7401/api/v1/workflows/order/instances/order-1234/cancel
```

Drain a host, allowing up to two minutes for its actors to deactivate:

```sh
curl -X POST \
  -H "Authorization: Bearer $FRANCIS_MANAGEMENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"timeout": "2m", "reason": "Node maintenance"}' \
  http://127.0.0.1:7401/api/v1/hosts/<host-id>/drain
```

## Actor state

Actor state is stored as MessagePack. `GET /api/v1/actor-states/{type}/{id}` reads it from the provider without activating the actor, and returns the last committed state: work in progress inside a running actor call isn't visible. A missing or expired state returns `404`.

By default the state is converted to JSON and returned in the `state` field, together with `size` (the number of stored bytes) and a `lossy` flag. A Go struct encoded with `msgpack` tags appears with its MessagePack field names as keys. Values that have no JSON equivalent are rendered with this convention:

| MessagePack value | JSON rendering |
| --- | --- |
| nil, boolean, string, array, map with string keys | As-is |
| Integer within ±2^53, finite float | Number |
| Timestamp extension | RFC 3339 string with nanoseconds |
| Integer outside ±2^53 | Decimal string |
| NaN or infinity | The string `"NaN"`, `"Infinity"`, or `"-Infinity"` |
| Binary, or a string that isn't valid UTF-8 | `{"$binary": "<base64>"}` |
| Other extension types | `{"$ext": <type>, "data": "<base64>"}` |
| Map with non-string keys | Keys rendered as their JSON text, for example `"1"` or `"true"` |

`lossy` is `true` when any value used one of the renderings other than the first two rows, which means the JSON can't be converted back to the same bytes. To get the exact stored bytes, send `Accept: application/msgpack`. Quality values count: the bytes are returned only when MessagePack is listed with a quality above zero and at least as high as JSON's, so `*/*` alone, or `application/msgpack;q=0`, returns JSON.

If two keys of a map would become the same JSON name, such as `1` and `"1"` or the same key stored twice, the state can't be rendered as JSON, because JSON parsers disagree on which value they keep. The request then fails with `422 stateNotDecodable`, and the state can still be read with `Accept: application/msgpack`.

Built-in actor types (such as workflow orchestrators and workers) store their internals in the same way, and `actors:state:read` can read them. That's useful for debugging, but their format is internal and can change between releases. Use the workflow endpoints to inspect workflows.

Reads of actor state are recorded in the audit log.

## Actions

Actions are synchronous. Each request returns once its target has accepted it, or fails with the reason. Nothing is stored about an action apart from the audit log, so there's no operation ID to poll: anything that continues after the response, such as a host shutting down or a workflow compensating, shows up on the host or the workflow instance.

Repeating an action is safe. Deactivating an actor that isn't active, or draining a host that's already draining, succeeds without doing anything. A request that failed because its target was unreachable can simply be sent again.

While a [`clusteradmin`](https://pkg.go.dev/github.com/italypaleale/francis/clusteradmin) exclusive-access lease is held on the cluster, for example during a restore, every action returns `409` with the code `exclusiveLeaseHeld` and the lease's owner and expiry. Drains and workflow controls check the lease again in the same write that carries them out, so a lease taken while one of them runs still stops it. A deactivation that's already under way still finishes, which does no harm since the lease shuts down every host anyway.

Action request bodies are JSON objects. An empty body is the same as `{}`, and unknown fields are rejected. A `reason` can be at most 1024 bytes, and is recorded in the audit log.

### Deactivating an actor

`POST /api/v1/actors/{type}/{id}/deactivate` asks the host the actor is active on to deactivate it, and returns once the host has done so. The actor's durable state is kept, and it's activated again by the next call. The response has `notActive: true` when the actor wasn't active.

Deactivating isn't pausing: a pending alarm or job for the actor can activate it again straight away.

### Draining a host

`POST /api/v1/hosts/{hostId}/drain` marks the host as draining, so no new actors are placed on it, then asks it to finish its work, deactivate its actors, and unregister. After that the host's `Run` method returns an error that matches `host.ErrAdministrativeDrain`, so your application can tell a requested drain apart and exit. A drained host can't be run again, and an external supervisor (such as Kubernetes) is responsible for starting a replacement process, which registers as a new host.

The request body accepts:

- `timeout`: a Go duration string, up to `5m`, bounding how long the host has to deactivate its actors gracefully before the remaining ones are halted. When omitted, the host waits for every actor to deactivate, with no time limit. The host keeps sending health checks until its actors have halted, so its registration doesn't expire while they finish.
- `force`: drain the host even if it's the last live host serving one or more actor types. Without it, the request fails with `409` and the code `lastServer`, listing the affected types in `details.actorTypes`. The check is atomic with other drain requests, so concurrent drains of the last hosts serving a type can't both succeed without `force`.
- `reason`: an optional reason, logged by the host and in the audit log.

The request returns `202` once the host has accepted the drain, without waiting for it to shut down. The host is listed as `draining` until it unregisters.

There's no way to undo a drain once the host has accepted it. If the host reconnects before it receives the request, including to a different runtime replica, the request is sent again to its new connection. Only if the host keeps reconnecting does the request fail with the code `hostReattached`: send it again. A host that reconnects on its own after an unrelated disconnection, without a drain request, is back in service.

The host is marked as draining before it's asked to drain, so a request that fails part of the way could leave a host that never accepted the drain out of placement. When the request fails, the API asks the host whether it's draining:

- If the host is draining, it accepted the drain and only its acknowledgement was lost, so the request succeeds.
- If the host isn't draining, its draining mark is removed, so it goes back into service, and the request fails with `details.hostDraining: false`.
- If the host can't be asked, it may have accepted the drain, so its mark stays, and the request fails with `details.hostDraining: true`. Send the drain again once the host is reachable. The mark also clears when the host registers again, which in the remote topology includes reconnecting to a runtime.

### Cancelling, suspending, and resuming a workflow instance

These requests send the instance the same durable control job that `WorkflowService.Cancel`, `Suspend`, and `Resume` send, and return `202` once the job is stored. The instance applies it on its next turn, and the result shows up in its status and event history.

- `coalesced: true` means a job of the same kind was already pending, so this request's reason was dropped.
- `noEffect: true` (with status `200`) means the instance is already finished, or is compensating and the request is a cancel, so no job was sent.

An instance that's waiting for a host serving its exact workflow version and definition applies the job only once such a host is available.

## Pagination

List routes accept `limit` (default 100, maximum 1000) and `cursor`. Responses have an `items` array and, when there are more results, a `nextCursor`: pass it as `cursor` to get the next page. Cursors are opaque.

Some filters are applied to each page after it's read, so a page can have fewer items than `limit` while more pages follow. Keep going until `nextCursor` is absent.

## Partial results

Routes that collect data from hosts (`/activations`, `/actor-types`, `/workflows`, and the activation counts in `/cluster/summary`) query every reachable host with a bounded number of concurrent requests. When some hosts can't be queried, the response has `partial: true` and an `errors` list with the host ID, code, and message of each failure. `/activations` returns `503` with the code `noHostsReachable` when none of the hosts could be queried.

Each host is read independently, and the data can change while the request runs, so results are operational evidence, not a consistent snapshot of the cluster: an actor that moves between hosts during a request can show up twice or not at all, and the same is true across pages. Responses include the time they were observed (`observedAt`, or `startedAt` and `finishedAt`). For the freshest view of a single host, use `/hosts/{hostId}/activations`.

## Errors

Errors return a JSON body:

```json
{
  "code": "lastServer",
  "message": "the host is the last live server of one or more actor types; set force to drain it anyway",
  "requestId": "0192f0c4-6f5e-7b7e-9d1a-3c2b1a0f9e8d",
  "details": {
    "actorTypes": ["counter"]
  }
}
```

- `code` is a stable, machine-readable error code, such as `badRequest`, `unauthorized`, `forbidden`, `notFound`, `notApplicable` (a route that doesn't exist in this topology, like `/runtimes` in the local topology), `exclusiveLeaseHeld`, `lastServer`, `hostUnavailable`, `hostReattached`, `noHostsReachable`, `eventHistoryDisabled`, `payloadTooLarge`, `stateNotDecodable`, `timeout`, or `internal`.
- `requestId` is also returned in the `X-Request-Id` header of every response, and matches the audit log.
- `retryable` is present, and `true`, when sending the same request again may succeed.
- `details` carries extra information for some errors.
