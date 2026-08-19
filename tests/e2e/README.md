# Helm chart end-to-end tests

These tests deploy the local Francis Helm chart and each discovered test application to Kubernetes, then drive the applications through HTTP from tagged Go tests.

The suite has two database variants:

- `sqlite` installs one Francis runtime replica with a persistent SQLite volume
- `postgres` starts one disposable PostgreSQL Deployment and installs three Francis runtime replicas

GitHub Actions runs SQLite with JWT authentication and PostgreSQL with both JWT and PSK authentication in parallel, each in its own Docker-backed Kind cluster.

Each test folder is an independent application and tagged Go test package. The test files carry the `e2e` build tag, so ordinary `go test` runs do not select them.

The Go app under [`testhelper`](testhelper) owns the complete suite lifecycle. It creates the namespace, starts PostgreSQL when selected, discovers Kubernetes JWT metadata when needed, invokes Helm 4 to install and test the local chart, publishes the runtime trust configuration, and removes the namespace when finished.

The app discovers immediate folders through a `test.json` marker, then requires both an `app` Go package and at least one tagged `_test.go` file. Infrastructure folders without that marker are ignored even when they contain their own unit tests.

Each marker configures settings owned by that test, starting with its application replica count:

```json
{
  "replicas": 3
}
```

Unknown settings, invalid replica counts, and incomplete test folders are rejected before cluster setup.

For each discovered test, the app then performs one isolated lifecycle:

1. Cross-compile the test application with `go build`
2. Build its container with Docker or Podman using [`app.Dockerfile`](app.Dockerfile)
3. Optionally push the image or load it into Kind
4. Create a dedicated ServiceAccount, metadata-configured Deployment, and Service with client-go
5. Wait for every replica, port-forward every application pod, and run that test's tagged Go package with the complete endpoint list
6. Print every app replica's pod logs and delete that test's Kubernetes resources before the next test starts

The helper exports the comma-separated replica endpoints as `E2E_<TEST-NAME>_URLS`, after uppercasing the test name and replacing hyphens with underscores.

## Prerequisites

- Go matching the E2E [`go.mod`](go.mod)
- Docker or Podman
- Helm 4
- A current Kubernetes context on which you can create a namespace
- Permission to read `/.well-known/openid-configuration` and `/openid/v1/jwks`
- A default StorageClass for the SQLite variant

The target cluster must be able to pull the runtime image. The Go app handles each discovered test application image immediately before deploying that test.

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

The Go app uses the current Kubernetes context, creates the namespace selected by `--namespace`, and installs the local chart. It refuses to reuse an existing namespace and removes the namespace when the run finishes.

For a remote cluster, tell the app to push each just-built test image to a registry the cluster can read:

```sh
go run ./tests/e2e/testhelper \
  --database postgres \
  --namespace francis-e2e-postgres \
  --runtime-image-repository registry.example.com/francis-e2e \
  --runtime-image-tag local \
  --test-image-prefix registry.example.com/francis-e2e- \
  --test-image-tag local \
  --container-engine docker \
  --push-test-images
```

Select the PSK variant with `--authentication psk`:

```sh
go run ./tests/e2e/testhelper \
  --database postgres \
  --authentication psk \
  --namespace francis-e2e-postgres-psk \
  --runtime-image-repository registry.example.com/francis-e2e \
  --runtime-image-tag local \
  --test-image-prefix registry.example.com/francis-e2e- \
  --test-image-tag local \
  --container-engine docker \
  --push-test-images
```

For Docker-backed Kind, load the runtime once and let the app load every test image after building it:

```sh
kind_cluster=actors-e2e

kind load docker-image --name "$kind_cluster" francis-e2e:local
go run ./tests/e2e/testhelper \
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

For Podman-backed Kind, use explicit `localhost/` image names. The app automatically uses an image archive so Kind does not depend on Podman's local image-name discovery:

```sh
kind_cluster=actors-e2e

podman save --format docker-archive -o /tmp/francis-e2e-runtime.tar \
  localhost/francis-e2e:local
KIND_EXPERIMENTAL_PROVIDER=podman \
  kind load image-archive --name "$kind_cluster" /tmp/francis-e2e-runtime.tar

go run ./tests/e2e/testhelper \
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

Pass `--keep-namespace` to preserve the runtime namespace for inspection after success or failure. Per-test application resources are always removed after their tagged test completes. Run `go run ./tests/e2e/testhelper --help` for every option and its default.
