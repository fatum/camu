#!/bin/bash
set -euo pipefail
# Full leave + rejoin cycle — tests local recovery and checkpoint recovery after membership change.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 "$SCRIPT_DIR/../run.sh" membership "${1:-60}"
