#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

API=kafka "$SCRIPT_DIR/../run.sh" "${1:-leader-kill}" "${2:-10}"
