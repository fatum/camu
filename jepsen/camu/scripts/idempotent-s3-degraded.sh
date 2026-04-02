#!/bin/bash
set -euo pipefail
# Block MinIO — tests behavior when S3 checkpoint upload/download fails.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 "$SCRIPT_DIR/../run.sh" s3-partition "${1:-60}"
