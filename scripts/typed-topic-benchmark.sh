#!/usr/bin/env bash
set -euo pipefail

# By default this creates an isolated MinIO + five-node Camu cluster. Set
# CAMU_URL to use an existing endpoint instead (the caller then owns its lifecycle).
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
working_dir="$(pwd -P)"
tmp_dir=""
network=""
minio_id=""
camu_ids=()
node_count="${CAMU_NODES:-5}"

resolve_output_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$working_dir" "$1" ;;
  esac
}

export OUTPUT="$(resolve_output_path "${OUTPUT:-typed-topic-benchmark.json}")"
benchmark_log="$(resolve_output_path "${BENCHMARK_LOG:-typed-topic-benchmark.log}")"
benchmark_errors="$(resolve_output_path "${BENCHMARK_ERRORS:-typed-topic-benchmark.errors.log}")"
: >"$benchmark_log"
: >"$benchmark_errors"
exec > >(tee -a "$benchmark_log") 2> >(tee -a "$benchmark_errors" >&2)

cleanup() {
  set +e
  echo "[benchmark] cleaning up local benchmark resources" >&2
  for camu_id in "${camu_ids[@]}"; do
    docker rm -f "$camu_id" >/dev/null 2>&1
  done
  [[ -n "$minio_id" ]] && docker rm -f "$minio_id" >/dev/null 2>&1
  [[ -n "$network" ]] && docker network rm "$network" >/dev/null 2>&1
  [[ -n "$tmp_dir" ]] && rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

echo "[benchmark] result: $OUTPUT" >&2
echo "[benchmark] log: $benchmark_log" >&2
echo "[benchmark] errors: $benchmark_errors" >&2
echo "[benchmark] nodes: $node_count" >&2

if [[ -z "${CAMU_URL:-}" ]]; then
  echo "[benchmark] starting isolated local MinIO and Camu cluster" >&2
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/camu-benchmark.XXXXXX")"
  network="camu-benchmark-$RANDOM"
  docker network create "$network" >/dev/null
  docker build -t camu-benchmark:local "$repo_dir" >/dev/null
  minio_id="$(docker run -d --network "$network" --network-alias minio --name "${network}-minio" -v "$tmp_dir/minio:/data" -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data)"
  bucket_ready=0
  for _ in {1..60}; do if docker run --rm --entrypoint sh --network "$network" minio/mc -c 'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && mc mb --ignore-existing local/camu-data >/dev/null 2>&1'; then bucket_ready=1; break; fi; sleep 1; done
  [[ "$bucket_ready" == 1 ]] || { echo "MinIO did not become ready" >&2; exit 1; }
  host_port="${CAMU_PORT:-18080}"
  node_urls=""
  for node in $(seq 1 "$node_count"); do
    node_urls+="http://127.0.0.1:$((host_port + node - 1)),"
  done
  export NODE_URLS="${node_urls%,}"
  for node in $(seq 1 "$node_count"); do
    cat >"$tmp_dir/camu-$node.yaml" <<EOF
server:
  address: "camu-$node:8080"
  internal_address: "camu-$node:8081"
  replication_address: "camu-$node:8082"
  instance_id: "benchmark-$node"
  kafka_port: 9092
  mode: stream
sql:
  enabled: true
storage:
  bucket: camu-data
  region: us-east-1
  endpoint: http://minio:9000
  credentials:
    access_key: minioadmin
    secret_key: minioadmin
segments:
  max_size: 67108864
  max_age: 1m
cache:
  directory: /var/lib/camu/cache-$node
  max_size: 1073741824
EOF
    node_args=(--network "$network" --network-alias "camu-$node" --name "${network}-camu-$node" -e CAMU_LOG_LEVEL=error -e CAMU_REQUEST_LOG=0 -v "$tmp_dir/cache-$node:/var/lib/camu/cache-$node" -v "$tmp_dir/camu-$node.yaml:/etc/camu.yaml:ro")
    node_args+=(-p "127.0.0.1:$((host_port + node - 1)):8080")
    camu_ids+=("$(docker run -d "${node_args[@]}" camu-benchmark:local serve --config /etc/camu.yaml)")
  done
  export CAMU_URL="http://127.0.0.1:$host_port"
  for node in $(seq 1 "$node_count"); do
    node_port=$((host_port + node - 1))
    ready=0
    for _ in {1..120}; do
      if curl -fsS "http://127.0.0.1:$node_port/v1/ready" >/dev/null; then ready=1; break; fi
      if ! docker ps --format '{{.ID}}' | grep -q "^${camu_ids[$((node - 1))]:0:12}$"; then
        echo "Camu node $node exited before readiness" >&2
        docker logs "${camu_ids[$((node - 1))]}" >&2 || true
        exit 1
      fi
      sleep 1
    done
    [[ "$ready" == 1 ]] || { echo "Camu node $node did not become ready" >&2; docker logs "${camu_ids[$((node - 1))]}" >&2 || true; exit 1; }
  done
  sleep 5
fi

set +e
echo "[benchmark] running typed topic benchmark against $CAMU_URL" >&2
go run "$repo_dir/cmd/typed-topic-benchmark" "$@"
status=$?
echo "[benchmark] benchmark exited with status $status" >&2
exit "$status"
