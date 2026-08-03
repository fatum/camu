#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if [[ -f .env ]]; then
  set -a
  . ./.env
  set +a
fi

terraform_retry() {
  local attempts="${TERRAFORM_RETRY_ATTEMPTS:-5}"
  local attempt terraform_status output
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    output="$(mktemp)"
    if terraform "$@" 2>&1 | tee "$output"; then
      rm -f "$output"
      return 0
    fi
    terraform_status="${PIPESTATUS[0]}"
    if ! grep -q 'mTLS verification failed' "$output" || ((attempt == attempts)); then
      rm -f "$output"
      return "$terraform_status"
    fi
    rm -f "$output"
    echo "Terraform hit a transient DigitalOcean mTLS error; retrying (${attempt}/${attempts})..." >&2
    sleep $((attempt * 5))
  done
}

terraform init
terraform_retry apply -auto-approve -target=digitalocean_container_registry.benchmark
git_revision="$(git -C "$script_dir/../.." rev-parse --short HEAD)"
image_tag="${IMAGE_TAG:-benchmark-${git_revision}-$(date -u +%Y%m%d%H%M%S)}"
image="$(IMAGE_TAG="$image_tag" ./build-image.sh)"
echo "Using published image: ${image}" >&2
terraform apply -auto-approve -var="camu_image=$image"
./ips.sh
