#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
RF="${RF:-3}" MIN_ISR="${MIN_ISR:-2}" READ_MODE=replica WORKLOAD=replica-flushed-reads ./run.sh "${1:-leave}" "${2:-10}"
