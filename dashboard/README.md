# Management dashboard

The web UI for the [management API](../docs/content/docs/management-api.md), served by the runtime at `/` on the management listener. Its user documentation is in [docs/content/docs/dashboard.md](../docs/content/docs/dashboard.md).

It's a Svelte 5 SPA built with Vite. The runtime binary embeds the output of `pnpm run build` from `dist/`.

## Development

Start the demo cluster from the root of the repository, then run the dev server, which proxies `/api` to it:

```sh
# In one terminal, from the root of the repository
make dashboard-demo

# In another, from dashboard/
pnpm install
pnpm run dev
```

Open `http://localhost:3000` and sign in with one of the tokens the demo prints: `demo-management-token-0123456789abcdefgh` or `demo-readonly-token-0123456789abcdefghij` to see the read-only experience.

To proxy to another runtime, set `FRANCIS_MANAGEMENT_URL`, for example `FRANCIS_MANAGEMENT_URL=https://francis.example.com:7401 pnpm run dev`.

### Demo cluster

`make dashboard-demo` runs `go run -tags dashboard ./cmd/runtime demo`: a runtime with the in-memory provider and three hosts, all in one process, with the management API on `127.0.0.1:7401`. The `demo` subcommand only exists in builds made with the `demo` build tag, and its code is in `internal/demo`. It's a real cluster, so the dashboard sees the same responses a production cluster returns, and its actions work: drained hosts are replaced, and workflow controls change the instances.

Pass flags with `DEMO_FLAGS`:

- `-bind`: the address of the management API, `127.0.0.1:7401` by default
- `-lease`: hold the cluster's exclusive-access lease, as a restore would: it evicts the hosts, which keep failing to register again, and the API refuses drains and workflow controls
- `-many-jobs`: dispatch more than 10,000 pending jobs, so the cluster summary shows capped counts
- `-verbose`: enable verbose logging for the runtime and the hosts

The demo allows `http://localhost:3000` and `http://localhost:7402` as origins, so it also works with `pnpm run dev:standalone` and `francis dashboard`. When the dashboard is built (`make dashboard`), the demo also serves it at `http://127.0.0.1:7401/`.

To work on the standalone mode, where the dashboard asks for the endpoints to connect to, run `pnpm run dev:standalone` instead, and add `http://localhost:3000` to the runtime's `management.allowedOrigins`.

The runtime serves the same build in both modes: at `/` on the management listener, and on its own with `francis dashboard`.

## Checks and builds

From the root of the repository:

```sh
# Formatting, lint rules, types, and unit tests
make dashboard-check

# Build into dist/, then build the runtime to embed it
make dashboard
go build ./cmd/runtime
```

## Playwright tests

The tests in `e2e/` drive the dashboard in a browser, against a real cluster that the test run starts and stops:

- A runtime with the management API, serving the embedded dashboard on `127.0.0.1:17401`.
- The fixture host in `e2e/fixture`, which seeds actors, alarms, jobs, and workflow instances in fixed states, and serves a control API on `127.0.0.1:17499` that tests use to create the actors and instances they change.
- `francis dashboard`, serving the standalone dashboard on `127.0.0.1:17402`.

```sh
# Once, to download the browsers
pnpm run e2e:install

# Build the dashboard, the runtime, and the fixture host, then run the tests
pnpm run e2e
```

They run on Chromium by default. Set `E2E_BROWSERS` to `all`, or to a comma-separated list of `chromium`, `firefox`, and `webkit`, to run on other engines. The cluster's logs are in `.bin/e2e/cluster.log` at the root of the repository.
