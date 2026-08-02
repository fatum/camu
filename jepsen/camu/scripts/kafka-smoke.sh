#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

faults="${1:-kill}"
time_limit="${2:-10}"

echo "=== Kafka smoke: faults=$faults workload=mixed time-limit=$time_limit ==="
API=kafka "$SCRIPT_DIR/../run.sh" "$faults" "$time_limit"
