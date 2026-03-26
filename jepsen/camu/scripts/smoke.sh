#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
./run.sh "${1:-kill}" "${2:-10}"
