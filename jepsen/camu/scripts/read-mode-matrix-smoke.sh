#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAULTS="${1:-kill}"
TIME_LIMIT="${2:-20}"

run_case() {
  local mode="$1"
  local workload="${2:-mixed}"
  local faults="${3:-$FAULTS}"
  local time_limit="${4:-$TIME_LIMIT}"

  echo
  echo "=== Read-mode smoke: mode=$mode workload=$workload faults=$faults time-limit=$time_limit ==="
  RF="${RF:-3}" \
  MIN_ISR="${MIN_ISR:-2}" \
  READ_MODE="$mode" \
  WORKLOAD="$workload" \
  "$SCRIPT_DIR/../run.sh" "$faults" "$time_limit"
}

run_case leader mixed "$FAULTS" "$TIME_LIMIT"
run_case replica mixed "$FAULTS" "$TIME_LIMIT"
run_case any mixed "$FAULTS" "$TIME_LIMIT"
run_case replica replica-flushed-reads leave "${LEAVE_TIME_LIMIT:-30}"
