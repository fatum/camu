#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIME_LIMIT="${1:-30}"

run_case() {
  local faults="$1"
  local workload="${2:-mixed}"
  local read_mode="${3:-leader}"
  local time_limit="$4"

  echo
  echo "=== Recovery smoke: faults=$faults workload=$workload read-mode=$read_mode time-limit=$time_limit ==="
  RF="${RF:-3}" \
  MIN_ISR="${MIN_ISR:-2}" \
  WORKLOAD="$workload" \
  READ_MODE="$read_mode" \
  "$SCRIPT_DIR/../run.sh" "$faults" "$time_limit"
}

run_case leave mixed leader "${LEAVE_TIME_LIMIT:-30}"
run_case rejoin mixed leader "${REJOIN_TIME_LIMIT:-40}"
run_case membership mixed leader "${MEMBERSHIP_TIME_LIMIT:-60}"
run_case leave replica-flushed-reads replica "${FLUSHED_LEAVE_TIME_LIMIT:-40}"
