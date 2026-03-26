#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RF=3 MIN_ISR=3 "$SCRIPT_DIR/../run.sh" "${1:-kill}" "${2:-10}"
