#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if [[ -f .env ]]; then
  set -a
  . ./.env
  set +a
fi

ssh_user="${SSH_USER:-root}"
benchmark_auth_token="${BENCHMARK_AUTH_TOKEN:-${TF_VAR_benchmark_auth_token:-}}"
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

if [[ -n "$benchmark_auth_token" && ! "$benchmark_auth_token" =~ ^[A-Za-z0-9._~-]+$ ]]; then
  echo "BENCHMARK_AUTH_TOKEN must contain only letters, digits, dot, underscore, tilde, or hyphen" >&2
  exit 2
fi

wait_for_ssh() {
  local ip="$1"
  for _ in {1..60}; do
    if ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" 'test -f /etc/camu/benchmark.yaml && command -v docker >/dev/null'; then
      return 0
    fi
    sleep 5
  done
  echo "Camu node ${ip} did not become available for deployment" >&2
  return 1
}

wait_for_ready() {
  local ip="$1"
  for _ in {1..60}; do
    if curl -fsS "http://${ip}:8080/v1/ready" >/dev/null; then
      return 0
    fi
    sleep 5
  done
  echo "Camu node ${ip} did not become ready after deployment" >&2
  return 1
}

restart_camu() {
  local ip="$1"
  local image="$2"
  local auth_token="$3"
  echo "Deploying ${image} to ${ip}..." >&2
  wait_for_ssh "$ip"
  ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" /bin/sh -s -- "$image" "$auth_token" <<'REMOTE'
set -eu
image="$1"
auth_token="$2"
config=/etc/camu/benchmark.yaml
config_tmp="$(mktemp)"
awk -v auth_token="$auth_token" '
  /^      auth_token:/ { next }
  /^      heap_profile_enabled:/ { next }
  {
    print
    if ($0 ~ /^      mode: stream$/) {
      print "      auth_token: \"" auth_token "\""
      print "      heap_profile_enabled: true"
    }
  }
' "$config" >"$config_tmp"
mv "$config_tmp" "$config"
docker pull gcr.io/cadvisor/cadvisor:v0.49.2
docker rm -f cadvisor 2>/dev/null || true
docker run -d --name cadvisor --restart unless-stopped --privileged --device=/dev/kmsg -p 8083:8080 \
  -v /:/rootfs:ro -v /var/run:/var/run:ro -v /sys:/sys:ro \
  -v /var/lib/docker:/var/lib/docker:ro \
  gcr.io/cadvisor/cadvisor:v0.49.2
docker pull "$image"
docker rm -f camu 2>/dev/null || true
docker run -d --name camu --restart unless-stopped --network host \
  --label=io.camu.component=server \
  -e CAMU_LOG_LEVEL=info -e CAMU_REQUEST_LOG=0 -e GOMEMLIMIT=6GiB \
  -v /etc/camu/benchmark.yaml:/etc/camu.yaml:ro \
  -v /var/lib/camu:/var/lib/camu \
  "$image" serve --config /etc/camu.yaml
REMOTE
  wait_for_ready "$ip"
}

git_revision="$(git -C "$script_dir/../.." rev-parse --short HEAD)"
image_tag="${IMAGE_TAG:-benchmark-${git_revision}-$(date -u +%Y%m%d%H%M%S)}"
image="$(IMAGE_TAG="$image_tag" ./build-image.sh)"
echo "Using published image: ${image}" >&2

terraform init
ips_json="$(terraform output -json public_ips)"
while IFS= read -r ip; do
  restart_camu "$ip" "$image" "$benchmark_auth_token"
done < <(printf '%s' "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value')
