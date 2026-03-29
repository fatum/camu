#!/bin/bash
set -euo pipefail
# SIGSTOP/SIGCONT — simulates long GC pauses, tests client timeout+retry dedup.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 "$SCRIPT_DIR/../run.sh" pause "${1:-60}"
