#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# A short topic retention removes native segments while leader-kill interrupts
# exporting. The SQL checker proves acknowledged rows remain queryable; the
# retention checker proves source removal followed checkpoint coverage.
WORKLOAD=sql RETENTION_LIFECYCLE=true NUM_PARTITIONS=1 TOPIC_RETENTION=2s SEGMENT_MAX_SIZE=1024 SEGMENT_MAX_AGE=1s "$SCRIPT_DIR/../run.sh" "${1:-leader-kill}" "${2:-45}"
