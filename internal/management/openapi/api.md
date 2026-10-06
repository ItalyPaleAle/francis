The management API exposes read-only views of a Francis cluster (hosts, runtimes, placements, in-memory activations, actor state, jobs, alarms, and workflows), plus a small set of audited actions (drain a host, deactivate an actor, cancel/suspend/resume a workflow instance).

## Authentication and scopes

Every route under `/api/v1` requires an `Authorization: Bearer <token>` header.
`GET /api/v1/openapi.yaml` and `GET /healthz` (served at the root, outside `/api/v1`) are public and don't require a token.

Each operation declares the scope it requires in its description and in the `x-required-scope` extension.
The server is configured with two token lists:

- **Read-only tokens** receive every scope except those ending in `:manage`.
- **Management tokens** receive every scope.

| Scope | Grants |
| --- | --- |
| `cluster:read` | Cluster summary, runtimes, hosts |
| `actors:read` | Activations, placements, actor types, actor-state listings |
| `actors:state:read` | Reading the stored state of an actor (audited); a workflow's actor types also need `workflows:data:read` |
| `workflows:read` | Workflows, instances, event history |
| `workflows:data:read` | Including workflow instance input and output in instance details, and reading the stored state of a workflow's actor types (audited) |
| `jobs:read` | Jobs and alarms |
| `actors:manage` | Deactivating an actor |
| `hosts:manage` | Draining a host |
| `workflows:manage` | Cancelling, suspending, and resuming workflow instances |

A missing or unknown token yields `401` with a `WWW-Authenticate: Bearer realm="francis-management"` header; a valid token lacking the route's scope yields `403`.
`GET /api/v1/token` accepts any valid token and returns the scopes it grants, so a client can find out whether it holds a read-only token before offering an action.

Browser pages on other origins can call the API only when the server's configuration lists their origin; the server answers their CORS preflight requests, and refuses those from other origins with `403 forbidden`.

## Pagination

Paginated endpoints accept `limit` (1 to 1000, default 100) and an opaque `cursor`, and return `{ "items": [...], "nextCursor": "..." }`.
`nextCursor` is omitted on the last page.
Pass it back unchanged as `cursor` to fetch the next page; its contents are not part of the contract.
A malformed cursor or an out-of-range limit yields `400 badRequest`.
A page may contain fewer items than `limit` even when more pages follow (for example when a filter is applied after reading a page), so always rely on `nextCursor` to detect the end.

## Fan-out endpoints and partial results

Some endpoints query every host's in-memory snapshot (`/activations`, `/actor-types`, `/workflows`, and the activation count of `/cluster/summary`).
Hosts are queried concurrently with a per-host timeout, and a host that cannot be queried does not fail the request: the response sets `partial: true` and lists the host in `errors` (see `HostError`).
Each host is sampled independently, so these responses are operational evidence rather than a transactionally consistent view of the cluster: an actor that moved during collection may appear twice or not at all, and actors may move between pages.
Use `GET /hosts/{hostId}/activations` for the freshest view of one host.

## Errors

Every error response has a JSON body described by the `Error` schema.
Every response carries an `X-Request-Id` header, which is also echoed as `requestId` in error bodies and recorded in audit logs.
Errors with `retryable: true` may succeed if the request is repeated later.

Error `code` values:

| Code | Status | Meaning |
| --- | --- | --- |
| `badRequest` | 400 | Invalid path segment, query parameter, cursor, or request body |
| `unauthorized` | 401 | Missing or unknown bearer token |
| `forbidden` | 403 | The token does not grant the route's scope |
| `notFound` | 404 | The resource does not exist, or no such route |
| `notApplicable` | 404 | The resource does not exist in this topology (for example runtimes in the local topology) |
| `eventHistoryDisabled` | 404 | The workflow opted out of event history |
| `methodNotAllowed` | 405 | The path exists but does not serve the method; the `Allow` header lists the methods it serves |
| `lastServer` | 409 | Draining the host would leave one or more actor types without a live server; `details.actorTypes` lists them |
| `exclusiveLeaseHeld` | 409 | An exclusive-access lease is held on the cluster (for example during a restore), so drains and workflow controls are refused; `GET /api/v1/cluster/summary` reports who holds it; retryable |
| `hostReattached` | 409 | The host kept reconnecting, so the request could not be delivered to its current session; send it again; retryable |
| `payloadTooLarge` | 413 | The request body is larger than 64 KiB |
| `internal` | 500 | Unexpected server error |
| `hostUnavailable` | 503 | The host, or the runtime owning its session, could not be reached or was too busy; retryable |
| `noHostsReachable` | 503 | None of the queried hosts could be reached; `details.errors` lists them; retryable |
| `timeout` | 504 | The request timed out; retryable |
