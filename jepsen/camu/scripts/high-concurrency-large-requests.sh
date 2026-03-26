#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
CONCURRENCY="${CONCURRENCY:-25}" WORKLOAD=large-requests ./run.sh "${1:-kill}" "${2:-10}"
