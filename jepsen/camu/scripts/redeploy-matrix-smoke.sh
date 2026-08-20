#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIME_LIMIT="${1:-40}"

run_case() {
  local faults="$1"
  local workload="${2:-mixed}"
  local read_mode="${3:-leader}"
  local time_limit="$4"

  echo
  echo "=== Redeploy/recovery smoke: faults=$faults workload=$workload read-mode=$read_mode time-limit=$time_limit ==="
  RF="${RF:-3}" \
  MIN_ISR="${MIN_ISR:-2}" \
  WORKLOAD="$workload" \
  READ_MODE="$read_mode" \
  "$SCRIPT_DIR/../run.sh" "$faults" "$time_limit"
}

# Node restart with wiped local state: the restarted node must recover all
# topics, assignments, ISR, and committed segments from S3 alone, without
# losing acknowledged writes or serving ghosts.
run_case restart-wipe mixed leader "${RESTART_WIPE_TIME_LIMIT:-$TIME_LIMIT}"
run_case restart-wipe large-requests leader "${RESTART_WIPE_LARGE_TIME_LIMIT:-45}"

# Re-deploy a node under a fresh instance identity into an existing cluster:
# the S3 bucket already holds the topic, committed segments, and coordination
# state from the previous incarnation. The new node must adopt that state, be
# assigned partitions, and serve committed data without loss.
run_case redeploy mixed leader "${REDEPLOY_TIME_LIMIT:-$TIME_LIMIT}"
run_case redeploy large-requests leader "${REDEPLOY_LARGE_TIME_LIMIT:-45}"

# Combined: kill + wiped local state with concurrent membership churn.
run_case "restart-wipe,membership" mixed leader "${RESTART_WIPE_MEMBERSHIP_TIME_LIMIT:-55}"
