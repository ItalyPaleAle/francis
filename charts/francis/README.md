# Francis Helm chart

Runs the **Francis runtime**, the standalone control plane used by the [remote topology](https://francis.italypaleale.me/docs/topologies/), on Kubernetes.

The runtime owns the data store and coordinates placement, state, and alarms for a fleet of stateless worker hosts. Your workers are ordinary `host/remote` hosts in your own application deployments (which are not deployed by this chart).

> If you're using the [local topology](https://francis.italypaleale.me/docs/topologies/#local-topology), where Francis is embedded in your app, you don't need the runtime and you don't need this chart.

## Requirements

- Kubernetes 1.21 or newer
- Helm 3.9 or newer
- A PostgreSQL database, or a StorageClass that can provision a volume for SQLite

## Installing

Every release publishes the chart as an OCI artifact to the GitHub Container Registry, alongside the runtime image:

JWT bootstrap is enabled by default. Find your Kubernetes cluster's issuer and JWKS endpoint with `kubectl get --raw /.well-known/openid-configuration | jq '{issuer, jwks_uri}'`, then provide them when you install:

```sh
helm install francis oci://ghcr.io/italypaleale/charts/francis \
  --version <version> \
  --namespace francis --create-namespace \
  --set-string 'runtimePSKs[0]=<a long random string>' \
  --set-string 'bootstrap.jwt.issuer=<your Kubernetes OIDC issuer>' \
  --set-string 'bootstrap.jwt.audience=francis-runtime' \
  --set-string 'bootstrap.jwt.jwksURL=<your Kubernetes OIDC JWKS URL>'
```

The chart's version always matches the runtime version it deploys, and `helm show chart oci://ghcr.io/italypaleale/charts/francis` lists what's available. To install from a checkout of the repository instead, point Helm at the directory:

```sh
helm install francis ./charts/francis \
  --namespace francis --create-namespace \
  --set-string 'runtimePSKs[0]=<a long random string>' \
  --set-string 'bootstrap.jwt.issuer=<your Kubernetes OIDC issuer>' \
  --set-string 'bootstrap.jwt.audience=francis-runtime' \
  --set-string 'bootstrap.jwt.jwksURL=<your Kubernetes OIDC JWKS URL>'
```

That gives you a single replica backed by SQLite on a persistent volume, which is the smallest useful deployment.

The runtime PSK is a cluster secret. Generate it with something like `openssl rand -base64 32`, and see [Secrets](#secrets) below for how to keep it out of your values files. The [JWT bootstrap section](#jwt-bootstrap-with-kubernetes-service-account-tokens) covers clusters whose JWKS endpoint is not publicly reachable.

### Multiple replicas with PostgreSQL

```sh
helm install francis oci://ghcr.io/italypaleale/charts/francis \
  --version <version> \
  --namespace francis --create-namespace \
  --set replicaCount=3 \
  --set database.type=postgres \
  --set-string 'database.postgres.connectionString=postgres://francis:password@postgres.databases.svc.cluster.local:5432/francis' \
  --set-string 'runtimePSKs[0]=<a long random string>' \
  --set-string 'bootstrap.jwt.issuer=<your Kubernetes OIDC issuer>' \
  --set-string 'bootstrap.jwt.audience=francis-runtime' \
  --set-string 'bootstrap.jwt.jwksURL=<your Kubernetes OIDC JWKS URL>'
```

## Replicas and databases

`replicaCount` accepts **1 to 4** replicas, and which values are legal depends on the database:

| `database.type` | Replicas | Notes |
|---|---|---|
| `sqlite` | 1 only | A SQLite database can be owned by a single runtime. |
| `postgres` | 1 to 4 | Replicas share the database and the runtime PSKs, so they form one certificate issuer and workers fail over between them. |

All replicas are interchangeable: they share the same `runtimePSKs`, so they derive the same cluster CA, and they coordinate through the database.

Do not put the SQLite file on a networked filesystem such as NFS or SMB. Use a block-backed `ReadWriteOnce` volume, which is what the chart requests by default.

## Connecting workers

The chart creates a **headless Service** that gives every replica a stable DNS name. With the release installed as `francis` in namespace `francis`, they are:

```
francis-0.francis-headless.francis.svc.cluster.local:7400
francis-1.francis-headless.francis.svc.cluster.local:7400
…
```

The install notes print the exact addresses for your release. List all of them in `remote.WithRuntimeAddresses(…)`. A host connects to one runtime at a time and rolls over to another on failure, so it needs to be able to address each replica individually:

```go
h, err := remote.NewHost(
    remote.WithAddress(peerAddress),
    remote.WithRuntimeAddresses(
        "francis-0.francis-headless.francis.svc.cluster.local:7400",
        "francis-1.francis-headless.francis.svc.cluster.local:7400",
        "francis-2.francis-headless.francis.svc.cluster.local:7400",
    ),
    remote.WithHostBootstrapJWTFile("/var/run/secrets/francis/token"),
    remote.WithPinnedCA(caPEM),
)
```

A regular Service (`service.enabled`, on by default) also puts a single virtual IP in front of all replicas. It's convenient for a single-replica install, but with several replicas prefer the per-pod names above.

> **The runtime's port is UDP.** The runtime speaks WebTransport over HTTP/3. Every Service, NetworkPolicy, and firewall rule in front of it must allow **UDP**, not TCP.

### Pinning the cluster CA

Workers should pin the cluster CA so they can verify the runtime on their very first connection. Print it from a running pod:

```sh
kubectl exec -n francis francis-0 -- /bin/francis print-ca
```

Pass the PEM to `remote.WithPinnedCA(caPEM)`. See [Security](https://francis.italypaleale.me/docs/security/) for the full model.

### JWT bootstrap with Kubernetes service account tokens

JWT bootstrap is the chart default. Workers present a projected service account token that the runtime validates against an OIDC issuer, which avoids distributing or rotating a long-lived shared host secret. No issuer or key source works on every Kubernetes cluster, so you must fill in the settings for yours.

**Read [the caveat below](#who-can-join-with-jwt-bootstrap) before choosing this on a shared cluster.**

Find your cluster's issuer and JWKS endpoint:

```sh
kubectl get --raw /.well-known/openid-configuration | jq '{issuer, jwks_uri}'
```

**If the issuer is a public URL** — which is the case on EKS, GKE, and AKS — the runtime can fetch the keys directly and there is nothing else to configure:

```yaml
bootstrap:
  method: jwt
  jwt:
    issuer: https://oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLE
    audience: francis-runtime
    jwksURL: https://oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLE/keys
```

**If the issuer is the in-cluster one** (`https://kubernetes.default.svc.cluster.local`), the JWKS endpoint sits behind the API server, which serves it with the cluster CA and, by default, does not expose discovery to unauthenticated callers. The simplest way around both is to inline the keys, since they're public:

```sh
kubectl get --raw /openid/v1/jwks
```

```yaml
bootstrap:
  method: jwt
  jwt:
    issuer: https://kubernetes.default.svc.cluster.local
    audience: francis-runtime
    staticJWKS: '<the JSON document printed above>'
```

Re-run the command and upgrade the release if the cluster's signing keys are ever rotated. To fetch them at runtime instead, grant anonymous discovery and point the runtime at the cluster CA:

```sh
kubectl create clusterrolebinding service-account-issuer-discovery \
  --clusterrole=system:service-account-issuer-discovery \
  --group=system:unauthenticated
```

```yaml
bootstrap:
  method: jwt
  jwt:
    issuer: https://kubernetes.default.svc.cluster.local
    audience: francis-runtime
    jwksURL: https://kubernetes.default.svc.cluster.local/openid/v1/jwks

extraVolumes:
  - name: kube-ca
    configMap:
      name: kube-root-ca.crt
extraVolumeMounts:
  - name: kube-ca
    mountPath: /etc/kube-ca
    readOnly: true
extraEnv:
  # SSL_CERT_DIR is read in addition to the image's CA bundle, so the cluster CA is trusted alongside the public roots
  - name: SSL_CERT_DIR
    value: /etc/kube-ca
```

The runtime needs to reach the JWKS endpoint when it starts, so keep any NetworkPolicy in front of it open to the API server.

#### On the worker side

Project a token with the matching audience and hand its path to the host, which re-reads the file on each bootstrap so rotated tokens are picked up:

```yaml
volumes:
  - name: francis-token
    projected:
      sources:
        - serviceAccountToken:
            path: token
            audience: francis-runtime
            # The runtime rejects tokens with more than an hour of remaining lifetime, and a
            # shorter expiry also narrows the window in which a captured token can be replayed
            expirationSeconds: 600
```

```go
remote.WithHostBootstrapJWTFile("/var/run/secrets/francis/token")
```

Pin the cluster CA (`remote.WithPinnedCA`) especially here: the token is a bearer credential, so a meddler-in-the-middle on a worker's first connection could capture it.

#### Who can join with JWT bootstrap

The runtime validates a bootstrap token's **issuer, audience, signature, and expiry**. It reads the `sub` claim and logs it, but it does not check it against an allowlist, so **any token from that issuer with that audience is accepted, whatever service account it belongs to**.

That matters when the issuer is your Kubernetes cluster, because any pod can request a projected token for any audience — there's no RBAC gate on the audience field. On a shared cluster, JWT bootstrap against the cluster's own issuer therefore lets **any workload in the cluster** join as a Francis host, and a host can place actors and read and write state. A `hostPSK` is narrower: only the workloads you hand it to can join.

Pick JWT when the cluster is dedicated to this system, or when your issuer mints identities scoped to these workers. Pick `psk` on a shared cluster, and keep the key in a Secret that only your worker pods can mount.

## Secrets

The runtime is configured with a single YAML file, which the chart renders into a Secret. That file holds the runtime PSKs, the host bootstrap PSK, and the PostgreSQL connection string, so **anything you pass in `runtimePSKs`, `bootstrap.hostPSK`, or `database.postgres.connectionString` ends up in a Helm release**.

Choose whichever fits your setup:

- Pass them at install time with `--set-string`, from your CI system's secret store.
- Keep them in a values file that never reaches source control, and pass it with `-f`.
- Render the whole config file yourself into a Secret (with External Secrets, Sealed Secrets, SOPS, or your platform's equivalent), and point the chart at it:

  ```yaml
  existingConfigSecret: francis-runtime-config
  existingConfigSecretKey: config.yaml
  ```

  With `existingConfigSecret` set the chart stops rendering a config file, so `database` (except its persistence settings), `runtimePSKs`, `runtimeId`, `bootstrap`, `tuning`, and `log` are all ignored. The Secret must hold a complete [runtime configuration](https://francis.italypaleale.me/docs/deploying-the-runtime/#configuration), and it must bind to `0.0.0.0` on the port in `service.port`.

Treat the runtime PSKs as your most sensitive cluster secret: anyone holding a current one can mint a trusted certificate and join the cluster.

## Verifying the install

```sh
helm test francis -n francis
```

The test runs the image's own `francis healthcheck` against the first replica, which completes a full QUIC, TLS 1.3, HTTP/3, and WebTransport handshake and verifies the runtime's certificate against the cluster CA. The same command backs the pods' startup, liveness, and readiness probes.

## Values

### Deployment

| Key | Type | Default | Description |
|---|---|---|---|
| `replicaCount` | int | `1` | Number of runtime replicas, between 1 and 4. Only 1 is allowed with SQLite. |
| `image.repository` | string | `ghcr.io/italypaleale/francis` | Container image repository. |
| `image.tag` | string | `""` | Image tag, defaulting to the chart's `appVersion`. |
| `image.digest` | string | `""` | Pin the image by digest, taking precedence over the tag. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | list | `[]` | Image pull secrets. |
| `nameOverride` | string | `""` | Overrides the chart name in resource names. |
| `fullnameOverride` | string | `""` | Overrides the full resource name. |
| `commonLabels` | object | `{}` | Extra labels for every resource. |
| `commonAnnotations` | object | `{}` | Extra annotations for every resource. |

### Database

| Key | Type | Default | Description |
|---|---|---|---|
| `database.type` | string | `sqlite` | `postgres` or `sqlite`. SQLite works only with `replicaCount: 1`. |
| `database.sqlite.path` | string | `/data/francis.db` | Path of the SQLite file, which must be inside the mounted volume. |
| `database.sqlite.persistence.enabled` | bool | `true` | Store the database on a PersistentVolumeClaim. Disabling it loses all state on restart. |
| `database.sqlite.persistence.existingClaim` | string | `""` | Mount an existing PVC instead of the chart's volume claim template. |
| `database.sqlite.persistence.mountPath` | string | `/data` | Directory the volume is mounted at. |
| `database.sqlite.persistence.storageClass` | string | `""` | StorageClass, where empty uses the cluster default and `-` disables dynamic provisioning. |
| `database.sqlite.persistence.accessModes` | list | `[ReadWriteOnce]` | Access modes for the volume. |
| `database.sqlite.persistence.size` | string | `8Gi` | Size of the volume. |
| `database.sqlite.persistence.annotations` | object | `{}` | Extra annotations for the volume claim. |
| `database.postgres.connectionString` | string | `""` | PostgreSQL connection string, which must begin with `postgres://` or `postgresql://`. |
| `database.queryLog.enabled` | bool | `false` | Log every SQL statement at debug level with its duration. |
| `database.queryLog.includeParameters` | bool | `false` | Include parameter values in traces and SQL logs, which may expose sensitive data. |
| `database.queryLog.slowThreshold` | string | `""` | Warn about statements at least this slow, for example `250ms`. |
| `database.operationLog.enabled` | bool | `false` | Log every provider operation at debug level with its duration. |
| `database.operationLog.slowThreshold` | string | `""` | Warn about provider operations at least this slow. |

### Cluster identity and bootstrap

| Key | Type | Default | Description |
|---|---|---|---|
| `runtimePSKs` | list | `[]` | **Required.** Pre-shared keys the cluster CA is derived from. The first is the primary, the rest stay trusted so keys can be rotated. |
| `runtimeId` | string | `""` | Optional identifier recorded in the runtime's server certificate. |
| `bootstrap.method` | string | `jwt` | How workers authenticate when joining: `psk` or `jwt`. |
| `bootstrap.hostPSK` | string | `""` | Shared secret workers present, required with `method: psk`. |
| `bootstrap.jwt.issuer` | string | `""` | Expected JWT issuer, required with `method: jwt`. |
| `bootstrap.jwt.audience` | string | `""` | Expected JWT audience, required with `method: jwt`. |
| `bootstrap.jwt.jwksURL` | string | `""` | JWKS endpoint used to validate tokens. |
| `bootstrap.jwt.staticJWKS` | string | `""` | Inline JWKS document, used instead of `jwksURL`. |
| `existingConfigSecret` | string | `""` | Mount a Secret holding a complete config file instead of rendering one. |
| `existingConfigSecretKey` | string | `config.yaml` | Key inside that Secret. |

### Runtime behavior

Empty values fall back to the runtime's own defaults.

| Key | Type | Default | Description |
|---|---|---|---|
| `tuning.workloadCertTTL` | string | `""` | Lifetime of the workload certificates issued to hosts (runtime default `1h`). |
| `tuning.healthCheckDeadline` | string | `""` | Maximum interval between host health pings (runtime default `20s`). |
| `tuning.alarmsPollInterval` | string | `""` | How often the runtime polls for due alarms (runtime default `1500ms`). |
| `tuning.alarmsLeaseDuration` | string | `""` | How long an alarm lease is held while it executes (runtime default `20s`). |
| `tuning.shutdownGracePeriod` | string | `""` | Grace period for a clean shutdown (runtime default `30s`). |
| `tuning.maxHosts` | int | `0` | Maximum hosts allowed in the cluster, where 0 means unlimited. |
| `log.level` | string | `info` | `debug`, `info`, `warn`, or `error`. |
| `log.json` | bool | `true` | Log in structured JSON. |

### Observability

| Key | Type | Default | Description |
|---|---|---|---|
| `openTelemetry.enabled` | bool | `false` | Export traces, metrics, and logs over OTLP. |
| `openTelemetry.endpoint` | string | `""` | Collector endpoint, required when enabled. |
| `openTelemetry.protocol` | string | `http/protobuf` | `http/protobuf` or `grpc`. |
| `openTelemetry.traces` | string | `otlp` | Traces exporter: `otlp`, `console`, or `none`. |
| `openTelemetry.metrics` | string | `otlp` | Metrics exporter. |
| `openTelemetry.logs` | string | `otlp` | Logs exporter. |
| `openTelemetry.sampler` | string | `""` | Head sampler, for example `parentbased_traceidratio`. |
| `openTelemetry.samplerArg` | string | `""` | Sampler argument, for example `0.1`. |

Anything else can be set through `extraEnv`, since the runtime reads the standard [`OTEL_*` variables](https://francis.italypaleale.me/docs/observability/#standalone-runtime).

### Networking

| Key | Type | Default | Description |
|---|---|---|---|
| `service.port` | int | `7400` | UDP port the runtime listens on. |
| `service.headless.annotations` | object | `{}` | Extra annotations for the headless Service. |
| `service.enabled` | bool | `true` | Also create a Service with a single virtual IP in front of all replicas. |
| `service.type` | string | `ClusterIP` | Type of that Service. |
| `service.nodePort` | int | `null` | Node port, for `NodePort` and `LoadBalancer`. |
| `service.loadBalancerIP` | string | `""` | Requested load balancer IP. |
| `service.loadBalancerSourceRanges` | list | `[]` | Source ranges allowed through the load balancer. |
| `service.externalTrafficPolicy` | string | `""` | `Cluster` or `Local`, for `NodePort` and `LoadBalancer`. |
| `service.annotations` | object | `{}` | Extra annotations for that Service. |

### Scheduling and pod settings

| Key | Type | Default | Description |
|---|---|---|---|
| `serviceAccount.create` | bool | `true` | Create a ServiceAccount. |
| `serviceAccount.name` | string | `""` | Name of the ServiceAccount, generated when empty. |
| `serviceAccount.annotations` | object | `{}` | Extra annotations for the ServiceAccount. |
| `serviceAccount.automountServiceAccountToken` | bool | `false` | Mount a token in the pods. The runtime does not talk to the Kubernetes API. |
| `podDisruptionBudget.enabled` | bool | `true` | Create a PodDisruptionBudget, which only happens with more than one replica. |
| `podDisruptionBudget.minAvailable` | int/string | `1` | Minimum available pods. |
| `podDisruptionBudget.maxUnavailable` | int/string | `null` | Maximum unavailable pods, taking precedence over `minAvailable`. |
| `podAntiAffinity` | string | `soft` | Spread replicas across nodes: `soft`, `hard`, or `""` to disable. Ignored when `affinity` is set. |
| `podAnnotations` | object | `{}` | Extra pod annotations. |
| `podLabels` | object | `{}` | Extra pod labels. |
| `podSecurityContext` | object | runs as UID/GID 65532 | Pod-level security context. |
| `securityContext` | object | no privilege escalation, read-only root, all capabilities dropped | Container-level security context. |
| `resources` | object | `{}` | Resource requests and limits. |
| `livenessProbe` / `readinessProbe` / `startupProbe` | object | enabled | Probe settings. Each probe runs `francis healthcheck`. |
| `terminationGracePeriodSeconds` | int | `60` | Should stay comfortably above `tuning.shutdownGracePeriod`. |
| `updateStrategy` | object | `{type: RollingUpdate}` | StatefulSet update strategy. |
| `podManagementPolicy` | string | `Parallel` | `Parallel` or `OrderedReady`. |
| `nodeSelector` | object | `{}` | Node selector. |
| `tolerations` | list | `[]` | Tolerations. |
| `affinity` | object | `{}` | Full affinity rules, taking precedence over `podAntiAffinity`. |
| `topologySpreadConstraints` | list | `[]` | Topology spread constraints. |
| `priorityClassName` | string | `""` | Priority class. |
| `extraEnv` | list | `[]` | Extra environment variables. |
| `extraEnvFrom` | list | `[]` | Extra `envFrom` sources. |
| `extraVolumes` | list | `[]` | Extra volumes. |
| `extraVolumeMounts` | list | `[]` | Extra volume mounts. |
| `extraArgs` | list | `[]` | Extra arguments for the runtime binary. |

## Operations

### Backups

The runtime binary can export the whole data store to a portable file, which also works to migrate between SQLite and PostgreSQL:

```sh
kubectl exec -n francis francis-0 -- /bin/francis backup -f - > francis-backup.bin
```

See [Backup and restore](https://francis.italypaleale.me/docs/backup-and-restore/).

### Rotating the runtime PSKs

Because the cluster CA is derived from the PSKs, trust is rotated by rotating them. Add the new key as the primary while keeping the old one trusted, roll out the change everywhere, then drop the old key:

```sh
helm upgrade francis <chart> -n francis --reuse-values \
  --set-string 'runtimePSKs[0]=<new key>' \
  --set-string 'runtimePSKs[1]=<old key>'
```

Workload certificates are short-lived, so members re-issue under the new primary quickly. Once they have, remove the old key with another upgrade.

### Migrating from SQLite to PostgreSQL

Back up the SQLite store, restore it into PostgreSQL, and then upgrade the release:

```sh
helm upgrade francis <chart> -n francis --reuse-values \
  --set database.type=postgres \
  --set-string 'database.postgres.connectionString=postgres://…' \
  --set replicaCount=3
```

The chart leaves the old PersistentVolumeClaim in place, since StatefulSet volume claim templates are not deleted automatically. Remove it by hand once you're confident in the migration.

## Documentation

- [Deploying the runtime](https://francis.italypaleale.me/docs/deploying-the-runtime/)
- [Topologies](https://francis.italypaleale.me/docs/topologies/)
- [Security model](https://francis.italypaleale.me/docs/security/)
- [Observability](https://francis.italypaleale.me/docs/observability/)
