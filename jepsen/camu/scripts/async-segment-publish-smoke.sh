#!/bin/bash
set -euo pipefail

# Force frequent sealing while exercising the asynchronous segment publisher.
# Run both a leader crash and an object-store isolation fault: each must retain
# acknowledged records and converge every replica during the final drains.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIME_LIMIT="${1:-45}"

run_case() {
  local fault="$1"
  echo "=== Async segment publish: fault=$fault time-limit=$TIME_LIMIT ==="
  RF=3 \
  MIN_ISR=2 \
  READ_MODE=replica \
  WORKLOAD=replica-flushed-reads \
  SEGMENT_MAX_SIZE=65536 \
  SEGMENT_MAX_AGE=2s \
  "$SCRIPT_DIR/../run.sh" "$fault" "$TIME_LIMIT"
}

run_case leader-kill
run_case s3-partition
