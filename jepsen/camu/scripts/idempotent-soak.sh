#!/bin/bash
set -euo pipefail
# 5-minute soak test with combined faults — long-running stability check.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 CONCURRENCY=15 SEGMENT_MAX_AGE=2s "$SCRIPT_DIR/../run.sh" leader-kill,partition,pause "${1:-300}"
