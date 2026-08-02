#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if [[ -f .env ]]; then
  set -a
  . ./.env
  set +a
fi

terraform init
terraform apply -auto-approve -target=digitalocean_container_registry.benchmark
git_revision="$(git -C "$script_dir/../.." rev-parse --short HEAD)"
image_tag="${IMAGE_TAG:-benchmark-${git_revision}-$(date -u +%Y%m%d%H%M%S)}"
image="$(IMAGE_TAG="$image_tag" ./build-image.sh)"
echo "Using published image: ${image}" >&2
terraform apply -auto-approve -var="camu_image=$image"
./ips.sh
