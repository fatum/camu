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
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

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
  echo "Deploying ${image} to ${ip}..." >&2
  wait_for_ssh "$ip"
  ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" /bin/sh -s -- "$image" <<'REMOTE'
set -eu
image="$1"
docker pull "$image"
docker rm -f camu 2>/dev/null || true
docker run -d --name camu --restart unless-stopped --network host \
  -e CAMU_LOG_LEVEL=error -e CAMU_REQUEST_LOG=0 \
  -v /etc/camu/benchmark.yaml:/etc/camu.yaml:ro \
  -v /var/lib/camu:/var/lib/camu \
  "$image" serve --config /etc/camu.yaml
REMOTE
  wait_for_ready "$ip"
}

terraform init
terraform apply -auto-approve -target=digitalocean_container_registry.benchmark
git_revision="$(git -C "$script_dir/../.." rev-parse --short HEAD)"
image_tag="${IMAGE_TAG:-benchmark-${git_revision}-$(date -u +%Y%m%d%H%M%S)}"
image="$(IMAGE_TAG="$image_tag" ./build-image.sh)"
echo "Using published image: ${image}" >&2
terraform apply -auto-approve -var="camu_image=$image"
if [[ "${SKIP_CONTAINER_RESTART:-0}" != "1" ]]; then
  ips_json="$(terraform output -json public_ips)"
  while IFS= read -r ip; do
    restart_camu "$ip" "$image"
  done < <(printf '%s' "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value')
fi
./ips.sh
