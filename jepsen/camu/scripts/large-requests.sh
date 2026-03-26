#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
WORKLOAD=large-requests ./run.sh "${1:-kill}" "${2:-10}"
