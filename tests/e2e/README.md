# Helm chart end-to-end tests

These tests deploy the local Francis Helm chart and each cataloged test application to Kubernetes, then drive the applications through HTTP from tagged Go tests.

The suite has two runtime variants:

- `sqlite` installs one Francis runtime replica with a persistent SQLite volume
- `postgres` starts one disposable PostgreSQL Deployment and installs three Francis runtime replicas

GitHub Actions runs both variants in parallel, each in its own Docker-backed Kind cluster.

Both variants use the chart's default JWT bootstrap. The runner reads the current cluster's OIDC issuer and public JWKS through `kubectl`, projects short-lived service-account tokens into the application pods, and pins the Francis CA before the apps connect.

Each catalog entry is an independent application and tagged Go test package. The test files carry the `e2e` build tag, so ordinary `go test` runs do not select them.

[`tests.txt`](tests.txt) is the shared test catalog used by GitHub Actions and the test runner. Each entry names a folder containing an `app` Go package and a tagged Go test package.

For each catalog entry, the Go helper under [`testhelper`](testhelper) performs one isolated lifecycle:

1. Cross-compile the test application with `go build`
2. Build its container with Docker or Podman using [`app.Dockerfile`](app.Dockerfile)
3. Optionally push the image or load it into Kind
4. Create a dedicated ServiceAccount, three-replica Deployment, and Service with client-go
5. Wait for every replica, port-forward a ready pod, and run that test's tagged Go package
6. Print pod logs on failure and delete that test's Kubernetes resources before the next test starts

## Prerequisites

- Go matching [`go.mod`](../../go.mod)
- Docker or Podman
- Helm 4
- `kubectl`
- `jq`
- A current Kubernetes context on which you can create a namespace and read `/.well-known/openid-configuration` and `/openid/v1/jwks`
- A default StorageClass for the SQLite variant

The target cluster must be able to pull the runtime image. The helper handles each test application image immediately before deploying that test.

## Build the runtime image

Build the runtime image from the repository root before starting the suite:

```sh
mkdir -p .bin/linux-amd64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -o .bin/linux-amd64/francis ./cmd/runtime

docker build --build-arg TARGETARCH=amd64 \
  -t registry.example.com/francis-e2e:local .
```

Replace the registry and architecture for your cluster. Push only the runtime image before using a remote cluster:

```sh
docker push registry.example.com/francis-e2e:local
```

## Run against any Kubernetes cluster

The runner uses the current Kubernetes context, creates the namespace selected by `--namespace`, and installs the local chart. It refuses to reuse an existing namespace and removes the namespace when the run finishes.

For a remote cluster, tell the helper to push each just-built test image to a registry the cluster can read:

```sh
tests/e2e/run.sh \
  --database postgres \
  --namespace francis-e2e-postgres \
  --runtime-image-repository registry.example.com/francis-e2e \
  --runtime-image-tag local \
  --test-image-prefix registry.example.com/francis-e2e- \
  --test-image-tag local \
  --container-engine docker \
  --push-test-images
```

For Docker-backed Kind, load the runtime once and let the helper load every test image after building it:

```sh
kind_cluster=actors-e2e

kind load docker-image --name "$kind_cluster" francis-e2e:local
tests/e2e/run.sh \
  --database sqlite \
  --namespace francis-e2e-sqlite \
  --runtime-image-repository francis-e2e \
  --runtime-image-tag local \
  --test-image-prefix francis-e2e- \
  --test-image-tag local \
  --image-pull-policy Never \
  --container-engine docker \
  --kind-cluster "$kind_cluster"
```

For Podman-backed Kind, use explicit `localhost/` image names. The helper automatically uses an image archive so Kind does not depend on Podman's local image-name discovery:

```sh
kind_cluster=actors-e2e

podman save --format docker-archive -o /tmp/francis-e2e-runtime.tar \
  localhost/francis-e2e:local
KIND_EXPERIMENTAL_PROVIDER=podman \
  kind load image-archive --name "$kind_cluster" /tmp/francis-e2e-runtime.tar

tests/e2e/run.sh \
  --database sqlite \
  --namespace francis-e2e-sqlite \
  --runtime-image-repository localhost/francis-e2e \
  --runtime-image-tag local \
  --test-image-prefix localhost/francis-e2e- \
  --test-image-tag local \
  --image-pull-policy Never \
  --container-engine podman \
  --kind-cluster "$kind_cluster"
```

Pass `--keep-namespace` to preserve the runtime namespace for inspection after success or failure. Per-test application resources are always removed after their tagged test completes. Run `tests/e2e/run.sh --help` for every option and its default.
