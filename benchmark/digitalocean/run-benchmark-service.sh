#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$script_dir/.env" ]]; then
  set -a
  . "$script_dir/.env"
  set +a
fi

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }

# ---- defaults ----
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
rate="${BENCHMARK_RATE:-10000}"
partitions="${PARTITIONS:-12}"
message_bytes="${MESSAGE_BYTES:-1024}"
diskless_topic="${DISKLESS_TOPIC:-benchmark-diskless-${run_id}}"
classic_topic="${CLASSIC_TOPIC:-benchmark-classic-${run_id}}"
export_enabled="${EXPORT_ENABLED:-true}"
stats_interval="${STATS_INTERVAL:-5m}"
node_count="${NODE_COUNT:-2}"

# ---- build & push benchmark image ----
registry_name="$(terraform output -raw registry_name)"
registry_endpoint="$(terraform output -raw registry_endpoint)"
image_tag="benchmark-service-$(git -C "$repo_dir" rev-parse --short HEAD)"
image="${registry_endpoint}/camu:${image_tag}"

echo "==> Building benchmark service image ${image}..." >&2
docker build --platform linux/amd64 -t "$image" "$repo_dir" >&2
registry_token="${DIGITALOCEAN_ACCESS_TOKEN:-${TF_VAR_digitalocean_token:-}}"
if [[ -z "$registry_token" ]]; then
  echo "DIGITALOCEAN_ACCESS_TOKEN or TF_VAR_digitalocean_token is required" >&2
  exit 1
fi
echo "$registry_token" | docker login "$registry_endpoint" --username "$registry_token" --password-stdin >&2
docker push "$image" >&2

# ---- create topics on the cluster ----
ips_json="$(terraform output -json public_ips)"
endpoint="$(echo "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[0].value')"
camu_url="http://${endpoint}:8080"

kafka_brokers=""
for ip in $(echo "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value'); do
  kafka_brokers+="${ip}:9092,"
done
kafka_brokers="${kafka_brokers%,}"

echo "==> Creating topics..." >&2
auth_header=""
if [[ -n "${CAMU_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token:-}}" ]]; then
  auth_header="-H 'Authorization: Bearer ${CAMU_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token}}'"
fi

create_topic() {
  local topic="$1" mode="$2"
  echo "  Creating $topic ($mode)..." >&2
  local body
  body=$(jq -n \
    --arg name "$topic" \
    --argjson partitions "$partitions" \
    --argjson export "$export_enabled" \
    --arg mode "$mode" \
    '{
      name: $name,
      partitions: $partitions,
      replication_factor: 5,
      min_insync_replicas: 3,
      retention: "24h",
      export_enabled: $export,
      storage_mode: $mode
    }')
  curl -fsS -X POST "${camu_url}/v1/topics" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${CAMU_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token:-}}" \
    -d "$body" >/dev/null || true  # topic may already exist
}

create_topic "$diskless_topic" "diskless"
create_topic "$classic_topic" "classic"

# ---- deploy benchmark nodes ----
echo "==> Applying Terraform for benchmark nodes..." >&2
terraform apply -auto-approve \
  -var="benchmark_image=$image" \
  -var="benchmark_rate=$rate" \
  -var="benchmark_topics=${diskless_topic},${classic_topic}" \
  -var="benchmark_storage_modes=diskless,classic" \
  -var="benchmark_partitions=$partitions" \
  -var="benchmark_message_bytes=$message_bytes" \
  >&2

echo ""
echo "==> Benchmark service deployed =="
echo "    Run ID:     $run_id"
echo "    Topics:     $diskless_topic (diskless), $classic_topic (classic)"
echo "    Rate:       $rate records/sec"
echo "    Partitions: $partitions"
echo "    Stats:      s3://camu/benchmark-stats/${run_id}/"
echo ""
echo "    Monitor:    terraform output -json benchmark_client_ips"
echo "    Stop:       terraform destroy -target=digitalocean_droplet.benchmark_client -auto-approve"
