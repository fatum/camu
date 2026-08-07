#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 SEGMENT_MAX_AGE=2s "$SCRIPT_DIR/../run.sh" partition "${1:-60}"
