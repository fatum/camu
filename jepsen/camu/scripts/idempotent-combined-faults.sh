#!/bin/bash
set -euo pipefail
# Combined leader-kill + network partition — most aggressive fault mix.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 "$SCRIPT_DIR/../run.sh" leader-kill,partition "${1:-60}"
