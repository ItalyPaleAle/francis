#!/usr/bin/env bash

set -euo pipefail

usage() {
  printf '%s\n' \
    "Usage: $0 [options]" \
    "" \
    "Options:" \
    "  --database <sqlite|postgres>        Database variant to deploy (default: sqlite)" \
    "  --namespace <name>                  Namespace to create (default: francis-e2e-<database>)" \
    "  --runtime-image-repository <name>   Runtime image repository (default: francis-e2e)" \
    "  --runtime-image-tag <tag>           Runtime image tag (default: local)" \
    "  --test-image-prefix <prefix>        Test application image prefix (default: francis-e2e-)" \
    "  --test-image-tag <tag>              Test application image tag (default: local)" \
    "  --image-pull-policy <policy>        Always, IfNotPresent, or Never (default: IfNotPresent)" \
    "  --container-engine <docker|podman>  Container engine used by the test helper (default: docker)" \
    "  --target-arch <arch>                Linux application architecture (default: go env GOARCH)" \
    "  --kind-cluster <name>               Load each test image into this Kind cluster" \
    "  --push-test-images                  Push each test image after building it" \
    "  --keep-namespace                    Preserve the namespace after the run" \
    "  -h, --help                          Show this help"
}

require_value() {
  local option=$1
  local option_value=${2-}
  if [[ -z "$option_value" || "$option_value" == --* ]]; then
    echo "$option requires a value" >&2
    exit 1
  fi
}

database=sqlite
namespace=""
runtime_image_repository=francis-e2e
runtime_image_tag=local
test_image_prefix=francis-e2e-
test_image_tag=local
image_pull_policy=IfNotPresent
container_engine=docker
target_arch=""
kind_cluster=""
push_test_images=false
keep_namespace=false

# Parse all user-facing configuration before creating any cluster resources
while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)
      require_value "$1" "${2-}"
      database=$2
      shift 2
      ;;
    --namespace)
      require_value "$1" "${2-}"
      namespace=$2
      shift 2
      ;;
    --runtime-image-repository)
      require_value "$1" "${2-}"
      runtime_image_repository=$2
      shift 2
      ;;
    --runtime-image-tag)
      require_value "$1" "${2-}"
      runtime_image_tag=$2
      shift 2
      ;;
    --test-image-prefix)
      require_value "$1" "${2-}"
      test_image_prefix=$2
      shift 2
      ;;
    --test-image-tag)
      require_value "$1" "${2-}"
      test_image_tag=$2
      shift 2
      ;;
    --image-pull-policy)
      require_value "$1" "${2-}"
      image_pull_policy=$2
      shift 2
      ;;
    --container-engine)
      require_value "$1" "${2-}"
      container_engine=$2
      shift 2
      ;;
    --target-arch)
      require_value "$1" "${2-}"
      target_arch=$2
      shift 2
      ;;
    --kind-cluster)
      require_value "$1" "${2-}"
      kind_cluster=$2
      shift 2
      ;;
    --push-test-images)
      push_test_images=true
      shift
      ;;
    --keep-namespace)
      keep_namespace=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$namespace" ]]; then
  namespace="francis-e2e-${database}"
fi

case "$database" in
  sqlite)
    runtime_replicas=1
    ;;
  postgres)
    runtime_replicas=3
    ;;
  *)
    echo "--database must be either sqlite or postgres" >&2
    exit 1
    ;;
esac

case "$image_pull_policy" in
  Always | IfNotPresent | Never)
    ;;
  *)
    echo "--image-pull-policy must be Always, IfNotPresent, or Never" >&2
    exit 1
    ;;
esac

case "$container_engine" in
  docker | podman)
    ;;
  *)
    echo "--container-engine must be docker or podman" >&2
    exit 1
    ;;
esac

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
temp_dir=$(mktemp -d)
namespace_created=false

cleanup() {
  status=$?
  trap - EXIT

  rm -rf "$temp_dir"

  if [[ "$namespace_created" == "true" && "$keep_namespace" != "true" ]]; then
    kubectl delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
  fi

  exit "$status"
}
trap cleanup EXIT

required_commands=(go helm jq kubectl "$container_engine")
if [[ -n "$kind_cluster" ]]; then
  required_commands+=(kind)
fi
for command_name in "${required_commands[@]}"; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: $command_name" >&2
    exit 1
  fi
done

if [[ -z "$target_arch" ]]; then
  target_arch=$(go env GOARCH)
fi

# Create an isolated namespace without taking ownership of any pre-existing resources
kubectl create namespace "$namespace"
namespace_created=true

# Discover the current cluster's workload identity configuration for the chart's default JWT bootstrap
kubectl get --raw /.well-known/openid-configuration >"$temp_dir/openid-configuration.json"
issuer=$(jq -er '.issuer' "$temp_dir/openid-configuration.json")
kubectl get --raw /openid/v1/jwks >"$temp_dir/jwks.json"

# Start an ephemeral PostgreSQL server only for the highly available runtime variant
database_args=()
if [[ "$database" == "postgres" ]]; then
  kubectl --namespace "$namespace" apply -f "$root_dir/tests/e2e/kubernetes/postgres.yaml"
  kubectl --namespace "$namespace" rollout status deployment/francis-e2e-postgres --timeout=3m
  database_args+=(
    --set database.type=postgres
    --set-string 'database.postgres.connectionString=postgres://francis:francis@francis-e2e-postgres:5432/francis?sslmode=disable'
  )
else
  database_args+=(
    --set database.type=sqlite
    --set database.sqlite.persistence.size=1Gi
  )
fi

# Install the local chart with the selected provider and every runtime replica addressable by stable DNS
helm upgrade --install francis "$root_dir/charts/francis" \
  --namespace "$namespace" \
  --wait \
  --timeout 5m \
  --set replicaCount="$runtime_replicas" \
  --set-string image.repository="$runtime_image_repository" \
  --set-string image.tag="$runtime_image_tag" \
  --set image.pullPolicy="$image_pull_policy" \
  --set-string 'runtimePSKs[0]=francis-e2e-runtime-psk-that-is-long-enough' \
  --set-string bootstrap.jwt.issuer="$issuer" \
  --set-string bootstrap.jwt.audience=francis-runtime \
  --set-file bootstrap.jwt.staticJWKS="$temp_dir/jwks.json" \
  --set-string tuning.alarmsPollInterval=250ms \
  "${database_args[@]}"

# Exercise the chart's own QUIC health-check hook before adding application hosts
helm test francis --namespace "$namespace" --logs --timeout 2m

# Pin the runtime-generated CA in every test app before any JWT can be presented
kubectl --namespace "$namespace" exec francis-0 -- /bin/francis print-ca >"$temp_dir/ca.pem"
kubectl --namespace "$namespace" create configmap francis-e2e-ca --from-file=ca.pem="$temp_dir/ca.pem"

# Publish the exact runtime replica list without relying on headless Service DNS selection
runtime_addresses=""
for ((replica = 0; replica < runtime_replicas; replica++)); do
  address="francis-${replica}.francis-headless.${namespace}.svc:7400"
  if [[ -n "$runtime_addresses" ]]; then
    runtime_addresses+=","
  fi
  runtime_addresses+="$address"
done
kubectl --namespace "$namespace" create configmap francis-e2e-config --from-literal=runtime-addresses="$runtime_addresses"

# Build the lifecycle helper from the isolated E2E module so client-go remains outside the runtime module
cd "$root_dir/tests/e2e"
test_helper="$temp_dir/francis-e2e-test-helper"
go build -trimpath -o "$test_helper" ./testhelper
helper_args=(
  --namespace "$namespace"
  --root "$root_dir"
  --image-prefix "$test_image_prefix"
  --image-tag "$test_image_tag"
  --image-pull-policy "$image_pull_policy"
  --container-engine "$container_engine"
  --target-arch "$target_arch"
)
if [[ -n "$kind_cluster" ]]; then
  helper_args+=(--kind-cluster "$kind_cluster")
fi
if [[ "$push_test_images" == "true" ]]; then
  helper_args+=(--push)
fi

# Give each catalog test an isolated build, deployment, execution, and teardown lifecycle
while IFS= read -r test_name; do
  "$test_helper" --test "$test_name" "${helper_args[@]}"
done <"$root_dir/tests/e2e/tests.txt"
