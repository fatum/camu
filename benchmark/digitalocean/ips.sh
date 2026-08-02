#!/usr/bin/env bash
set -euo pipefail

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

terraform output -json node_endpoints | jq -r '
  to_entries
  | sort_by(.key)
  | .[]
  | [.key, .value.public_ip, .value.private_ip, .value.public_http_url]
  | @tsv
' | {
  printf 'NODE\tPUBLIC IP\tPRIVATE IP\tHTTP URL\n'
  cat
}
