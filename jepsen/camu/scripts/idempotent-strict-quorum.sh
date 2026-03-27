#!/bin/bash
set -euo pipefail
# Strict quorum (min-ISR=3) — every ack means all replicas confirmed.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=3 "$SCRIPT_DIR/../run.sh" kill "${1:-60}"
