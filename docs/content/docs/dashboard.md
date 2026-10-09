---
title: "Dashboard"
weight: 31
---

Francis has a web dashboard for the [management API](/docs/management-api), built into the runtime binary and container image. You can use it in two ways:

- [Next to the management API](#next-to-the-management-api), served by a runtime on the same listener as the API.
- [Standalone](#standalone), served by the `francis dashboard` command, connecting to the endpoints you add.

## Next to the management API

When the runtime serves the [management API](/docs/management-api#enabling-the-api), it also serves the dashboard on the same listener, at the root path. Open `http://<runtime address>:7401/` in a browser (or `https://<runtime address>:7401/` if you configured TLS).

With the Helm chart, the management Service serves it. For example:

```sh
kubectl port-forward -n francis svc/<release>-francis-management 7401:7401
```

Then open `http://localhost:7401/`.

Hosts in the local (embedded) topology serve only the API: use a [standalone dashboard](#standalone) to browse them.

## Standalone

The runtime binary can also serve the dashboard on its own, without a configuration file and without connecting to any cluster:

```sh
francis dashboard
```

It listens on `127.0.0.1:7402` by default. Change it with the `-bind` and `-port` flags, for example `francis dashboard -bind 0.0.0.0 -port 8080`. The command serves plain HTTP and prints a warning when it's reachable from other machines: in that case, put it behind a TLS-terminating proxy.

In the standalone dashboard, add the endpoints to connect to, with the address and port of a runtime's or a local host's management API. The browser keeps the list in its local storage, and the dashboard asks for a token for each endpoint. Use **Switch** in the sidebar to move between endpoints.

### Allowing the dashboard's origin

The dashboard calls each endpoint from the browser across origins, so every endpoint must allow the dashboard's origin, such as `http://localhost:7402`.

In the runtime's configuration:

```yaml
management:
  allowedOrigins:
    - "http://localhost:7402"
```

With the Helm chart, set `management.allowedOrigins` the same way. In the local topology, set `AllowedOrigins` in `local.ManagementOptions`:

```go
local.WithManagementAPI(local.ManagementOptions{
	// ...
	AllowedOrigins: []string{"http://localhost:7402"},
})
```

Optionally use the wildcard `"*"` to allow any origin, although this is not recommended in production.

When an endpoint doesn't allow the origin, signing in fails with a message that names the origin to add.
