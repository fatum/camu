#!/bin/bash
set -euo pipefail

# Verifies that all acked data is eventually readable from replicas
# after leader election. Kills the busiest leader repeatedly while
# producing, then drains from replicas to check convergence.

cd "$(dirname "$0")/.."
RF="${RF:-3}" MIN_ISR="${MIN_ISR:-2}" READ_MODE=replica WORKLOAD=mixed ./run.sh "${1:-leader-kill}" "${2:-10}"
