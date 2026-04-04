#!/bin/bash
set -euo pipefail

faults="${1:-kill}"
time_limit="${2:-10}"

echo "=== Kafka smoke: faults=$faults workload=mixed time-limit=$time_limit ==="
API=kafka ./run.sh "$faults" "$time_limit"
