#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIME_LIMIT="${1:-20}"

run_case() {
  local faults="$1"
  local time_limit="$2"

  echo
  echo "=== Fault smoke: faults=$faults workload=mixed read-mode=leader time-limit=$time_limit ==="
  RF="${RF:-3}" \
  MIN_ISR="${MIN_ISR:-2}" \
  WORKLOAD=mixed \
  READ_MODE=leader \
  "$SCRIPT_DIR/../run.sh" "$faults" "$time_limit"
}

run_case kill "${KILL_TIME_LIMIT:-$TIME_LIMIT}"
run_case pause "${PAUSE_TIME_LIMIT:-$TIME_LIMIT}"
run_case partition "${PARTITION_TIME_LIMIT:-$TIME_LIMIT}"
run_case leader-kill "${LEADER_KILL_TIME_LIMIT:-30}"
run_case s3-partition "${S3_PARTITION_TIME_LIMIT:-$TIME_LIMIT}"
run_case kill,partition "${COMBINED_TIME_LIMIT:-40}"
