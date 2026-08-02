#!/usr/bin/env bash
set -euo pipefail

pid="${1:?benchmark pid is required}"
output="${2:?telemetry output path is required}"
interval="${TELEMETRY_INTERVAL:-5}"
IFS=',' read -r -a nodes <<< "${NODE_URLS:-}"
[[ "${#nodes[@]}" -gt 0 && -n "${nodes[0]}" ]] || { echo "NODE_URLS is required" >&2; exit 1; }

: >"$output"
trap 'exit 0' INT TERM
while kill -0 "$pid" 2>/dev/null; do
  at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for node in "${nodes[@]}"; do
    [[ -n "$node" ]] || continue
    metrics="$(curl -fsS --max-time 5 "${node%/}/metrics" 2>/dev/null || true)"
    jq -cn --arg at "$at" --arg node "$node" --arg metrics "$metrics" \
      '{at:$at,node:$node,metrics:$metrics}' >>"$output"
  done
  sleep "$interval"
done
