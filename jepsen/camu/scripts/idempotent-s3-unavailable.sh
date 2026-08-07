#!/bin/bash
set -euo pipefail
# Block MinIO on EVERY node — verifies durability and gaplessness across a full
# object-store outage (not just a single-node S3 partition). The idempotent
# workload must pause rather than drop records while S3 is down, and after S3
# returns every acknowledged record must be durable with no gaps, duplicates,
# or data loss.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent RF=3 MIN_ISR=2 SEGMENT_MAX_AGE=2s "$SCRIPT_DIR/../run.sh" s3-unavailable "${1:-60}"
