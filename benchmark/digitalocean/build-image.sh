#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
registry_name="$(terraform output -raw registry_name)"
registry_endpoint="$(terraform output -raw registry_endpoint)"
tag="${IMAGE_TAG:-benchmark-$(git -C "$repo_dir" rev-parse --short HEAD)}"
image="${IMAGE_NAME:-${registry_endpoint}/camu}:${tag}"
platform="${PLATFORM:-linux/amd64}"

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }

echo "Logging in to ${registry_endpoint}..." >&2
if command -v doctl >/dev/null; then
  doctl registry login "$registry_name" >&2
else
  registry_token="${DIGITALOCEAN_ACCESS_TOKEN:-${TF_VAR_digitalocean_token:-}}"
  [[ -n "$registry_token" ]] || {
    echo "doctl or DIGITALOCEAN_ACCESS_TOKEN/TF_VAR_digitalocean_token is required" >&2
    exit 1
  }
  printf '%s' "$registry_token" | docker login "$registry_endpoint" \
    --username "$registry_token" --password-stdin >&2
fi
echo "Building ${image} for ${platform}..." >&2
docker build --platform "$platform" -t "$image" "$repo_dir" >&2
echo "Pushing ${image}..." >&2
docker push "$image" >&2
printf '%s\n' "$image"
