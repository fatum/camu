#!/bin/bash
set -euo pipefail
# Block MinIO on EVERY node against a DISKLESS topic — the strongest gap
# scenario: diskless produce depends on S3 for every flush/commit, so a full
# object-store outage must pause produces (never drop records), and after S3
# returns every acknowledged record must be durable with no gaps, duplicates,
# or data loss.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD=idempotent STORAGE_MODE=diskless RF=1 MIN_ISR=1 SEGMENT_MAX_AGE=2s \
  "$SCRIPT_DIR/../run.sh" s3-unavailable "${1:-60}"
