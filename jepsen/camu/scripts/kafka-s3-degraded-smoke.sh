#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

API=kafka "$SCRIPT_DIR/../run.sh" "${1:-s3-partition}" "${2:-10}"
