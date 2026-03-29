#!/bin/bash
set -euo pipefail
# 25 concurrent producers — stresses per-thread sequence tracking and S3 counter.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 CONCURRENCY=25 "$SCRIPT_DIR/../run.sh" leader-kill "${1:-60}"
