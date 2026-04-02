#!/bin/bash
set -euo pipefail

TIME_LIMIT="${1:-10}"

cd "$(dirname "$0")/.."
WORKLOAD=offsets bash run.sh kill "$TIME_LIMIT"
