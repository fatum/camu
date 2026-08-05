#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$script_dir/.env" ]]; then
  set -a
  . "$script_dir/.env"
  set +a
fi

operation="${1:-}"
case "$operation" in
  produce|consume|sql|all) ;;
  *) echo "usage: $0 {produce|consume|sql|all} [target_bytes]" >&2; exit 2 ;;
esac
target_arg="${2:-1073741824}"
target_bytes="${TARGET_BYTES:-$target_arg}"
message_bytes="${MESSAGE_BYTES:-1024}"
partitions="${PARTITIONS:-4}"
output="${OUTPUT:-/tmp/camu-digitalocean-benchmark.json}"
benchmark_api="${BENCHMARK_API:-http}"
telemetry_output="${TELEMETRY_OUTPUT:-/tmp/camu-digitalocean-telemetry.jsonl}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
topic="${TOPIC:-benchmark-typed-${run_id}}"
heap_profiles_dir=""

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }

ips_json="$(terraform output -json public_ips)"
ips=()
while IFS= read -r ip; do
  ips[${#ips[@]}]="$ip"
done < <(printf '%s' "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value')
[[ "${#ips[@]}" -eq 5 ]] || { echo "expected five droplet IPs" >&2; exit 1; }

node_urls=""
kafka_brokers=""
for ip in "${ips[@]}"; do
  node_urls+="http://${ip}:8080,"
  kafka_brokers+="${ip}:9092,"
done

echo "Waiting for five Camu nodes..." >&2
for ip in "${ips[@]}"; do
  ready=0
  for _ in {1..60}; do
    if curl -fsS "http://${ip}:8080/v1/ready" >/dev/null; then ready=1; break; fi
    sleep 5
  done
  [[ "$ready" == 1 ]] || { echo "Camu node $ip did not become ready" >&2; exit 1; }
done

echo "Running benchmark topic: $topic" >&2

capture_heap_profiles() {
  [[ "${HEAP_PROFILE:-0}" == "1" ]] || return 0
  local profile_token="${HEAP_PROFILE_TOKEN:-${CAMU_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token:-}}}"
  if [[ -z "$profile_token" ]]; then
    echo "Heap profile capture skipped: set benchmark_auth_token and HEAP_PROFILE=1" >&2
    return 0
  fi
  heap_profiles_dir="$(mktemp -d "${TMPDIR:-/tmp}/camu-heap-profiles.${run_id}.XXXXXX")"
  for ip in "${ips[@]}"; do
    profile="$heap_profiles_dir/$ip.heap.pb.gz"
    if curl -fsS --max-time "${HEAP_PROFILE_TIMEOUT:-30}" -H "Authorization: Bearer $profile_token" "http://${ip}:8080/v1/debug/heap" -o "$profile"; then
      echo "Heap profile captured: $profile" >&2
    else
      rm -f "$profile"
      echo "Heap profile capture failed for $ip" >&2
    fi
  done
}

CAMU_URL="http://${ips[0]}:8080" \
NODE_URLS="${node_urls%,}" \
TOPIC="$topic" \
BENCHMARK_OPERATION="$operation" \
BENCHMARK_API="$benchmark_api" \
BENCHMARK_RUN_ID="$run_id" \
KAFKA_BROKERS="${KAFKA_BROKERS:-${kafka_brokers%,}}" \
REPLICATION_FACTOR=5 \
MIN_IN_SYNC_REPLICAS=3 \
TARGET_BYTES="$target_bytes" \
MESSAGE_BYTES="$message_bytes" \
PARTITIONS="$partitions" \
EXPORT_ENABLED="${EXPORT_ENABLED:-true}" \
STORAGE_MODE="${STORAGE_MODE:-}" \
CAMU_AUTH_TOKEN="${CAMU_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token:-}}" \
OUTPUT="$output" \
  "$repo_dir/scripts/typed-topic-benchmark.sh" &
benchmark_pid=$!

NODE_URLS="${node_urls%,}" "$repo_dir/scripts/collect-telemetry.sh" "$benchmark_pid" "$telemetry_output" &
collector_pid=$!
set +e
wait "$benchmark_pid"
status=$?
kill "$collector_pid" >/dev/null 2>&1 || true
wait "$collector_pid" >/dev/null 2>&1 || true
set -e
echo "Telemetry written to $telemetry_output" >&2
capture_heap_profiles

if [[ "${TELEMETRY_UPLOAD:-1}" == "1" && -s "$telemetry_output" && -n "${TF_VAR_spaces_access_key:-}" && -n "${TF_VAR_spaces_secret_key:-}" ]] && command -v aws >/dev/null 2>&1; then
  spaces_region="${TF_VAR_spaces_region:-sfo3}"
  spaces_bucket="${SPACES_BUCKET:-camu}"
  spaces_endpoint="https://${spaces_region}.digitaloceanspaces.com"
  aws_env=(AWS_ACCESS_KEY_ID="$TF_VAR_spaces_access_key" AWS_SECRET_ACCESS_KEY="$TF_VAR_spaces_secret_key" AWS_DEFAULT_REGION="$spaces_region")
  env "${aws_env[@]}" aws s3 cp "$telemetry_output" "s3://${spaces_bucket}/telemetry/${run_id}/telemetry.jsonl" --endpoint-url "$spaces_endpoint" >&2
  if [[ -f "$output" ]]; then
    env "${aws_env[@]}" aws s3 cp "$output" "s3://${spaces_bucket}/telemetry/${run_id}/benchmark-result.json" --endpoint-url "$spaces_endpoint" >&2
  fi
  if [[ -n "$heap_profiles_dir" ]]; then
    for profile in "$heap_profiles_dir"/*.heap.pb.gz; do
      [[ -f "$profile" ]] || continue
      env "${aws_env[@]}" aws s3 cp "$profile" "s3://${spaces_bucket}/telemetry/${run_id}/heap-profiles/$(basename "$profile")" --endpoint-url "$spaces_endpoint" >&2
    done
  fi
  echo "Telemetry uploaded to s3://${spaces_bucket}/telemetry/${run_id}/" >&2
else
  echo "Telemetry upload skipped; set TELEMETRY_UPLOAD=1 and configure Spaces credentials" >&2
fi
exit "$status"
