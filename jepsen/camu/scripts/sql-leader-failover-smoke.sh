#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

WORKLOAD=sql SEGMENT_MAX_AGE=1s "$SCRIPT_DIR/../run.sh" "${1:-leader-kill}" "${2:-35}"
