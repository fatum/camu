#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CONCURRENCY=25 WORKLOAD=large-requests "$SCRIPT_DIR/../run.sh" "${1:-kill}" "${2:-10}"
