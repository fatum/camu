#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$script_dir/.env" ]]; then
  set -a
  . "$script_dir/.env"
  set +a
fi

target_arg="${1:-1073741824}"
case "$target_arg" in
  1GiB) target_arg=1073741824 ;;
esac
target_bytes="${TARGET_BYTES:-$target_arg}"
message_bytes="${MESSAGE_BYTES:-1024}"
output="${OUTPUT:-/tmp/camu-digitalocean-benchmark.json}"
benchmark_api="${BENCHMARK_API:-http}"
telemetry_output="${TELEMETRY_OUTPUT:-/tmp/camu-digitalocean-telemetry.jsonl}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
topic="${TOPIC:-benchmark-typed-${run_id}}"

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

CAMU_URL="http://${ips[0]}:8080" \
NODE_URLS="${node_urls%,}" \
TOPIC="$topic" \
BENCHMARK_API="$benchmark_api" \
KAFKA_BROKERS="${KAFKA_BROKERS:-${kafka_brokers%,}}" \
REPLICATION_FACTOR=5 \
MIN_IN_SYNC_REPLICAS=3 \
TARGET_BYTES="$target_bytes" \
MESSAGE_BYTES="$message_bytes" \
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

if [[ "${TELEMETRY_UPLOAD:-1}" == "1" && -s "$telemetry_output" && -n "${TF_VAR_spaces_access_key:-}" && -n "${TF_VAR_spaces_secret_key:-}" ]] && command -v aws >/dev/null 2>&1; then
  spaces_region="${TF_VAR_spaces_region:-sfo3}"
  spaces_bucket="${SPACES_BUCKET:-camu}"
  spaces_endpoint="https://${spaces_region}.digitaloceanspaces.com"
  aws_env=(AWS_ACCESS_KEY_ID="$TF_VAR_spaces_access_key" AWS_SECRET_ACCESS_KEY="$TF_VAR_spaces_secret_key" AWS_DEFAULT_REGION="$spaces_region")
  env "${aws_env[@]}" aws s3 cp "$telemetry_output" "s3://${spaces_bucket}/telemetry/${run_id}/telemetry.jsonl" --endpoint-url "$spaces_endpoint" >&2
  if [[ -f "$output" ]]; then
    env "${aws_env[@]}" aws s3 cp "$output" "s3://${spaces_bucket}/telemetry/${run_id}/benchmark-result.json" --endpoint-url "$spaces_endpoint" >&2
  fi
  echo "Telemetry uploaded to s3://${spaces_bucket}/telemetry/${run_id}/" >&2
else
  echo "Telemetry upload skipped; set TELEMETRY_UPLOAD=1 and configure Spaces credentials" >&2
fi
exit "$status"
