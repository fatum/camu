#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIME_LIMIT="${1:-900}"
MINIO_BUCKET="${MINIO_BUCKET:-camu-data}"

WORKLOAD="${WORKLOAD:-mixed}" \
RF="${RF:-3}" \
MIN_ISR="${MIN_ISR:-3}" \
CONCURRENCY="${CONCURRENCY:-20}" \
READ_MODE="${READ_MODE:-leader}" \
NUM_PARTITIONS="${NUM_PARTITIONS:-500}" \
WAL_CHUNK_SIZE="${WAL_CHUNK_SIZE:-104857600}" \
SEGMENT_MAX_SIZE="${SEGMENT_MAX_SIZE:-104857600}" \
SEGMENT_MAX_AGE="${SEGMENT_MAX_AGE:-30s}" \
CAMU_DISABLE_NODE_LOGS="${CAMU_DISABLE_NODE_LOGS:-1}" \
CAMU_QUIET_CLIENT_LOGS="${CAMU_QUIET_CLIENT_LOGS:-1}" \
"$ROOT_DIR/run.sh" kill "$TIME_LIMIT"

LATEST_DIR="$ROOT_DIR/store/latest"
RESULTS_FILE="$LATEST_DIR/results.edn"
JEP_LOG="$LATEST_DIR/jepsen.log"

if [ ! -f "$RESULTS_FILE" ]; then
  echo "missing results file: $RESULTS_FILE" >&2
  exit 1
fi

if [ ! -f "$JEP_LOG" ]; then
  echo "missing jepsen log: $JEP_LOG" >&2
  exit 1
fi

TOPIC="$(python3 - "$RESULTS_FILE" "$JEP_LOG" "$NUM_PARTITIONS" <<'PY'
import re
import sys
from pathlib import Path

results_path = Path(sys.argv[1])
log_path = Path(sys.argv[2])
expected_partitions = int(sys.argv[3])

results = results_path.read_text()
if ":valid? true" not in results:
    raise SystemExit(f"jepsen result is not valid: {results_path}")

log_text = log_path.read_text()
match = re.search(r"Topic (jepsen-test-[a-f0-9\\-]+) created or already present", log_text)
if not match:
    raise SystemExit(f"could not determine topic from {log_path}")

print(match.group(1))
PY
)"

STATE_COUNT="$(cd "$ROOT_DIR" && docker compose run --rm --entrypoint sh setup-minio -c "
  mc alias set local http://minio:9000 \${MINIO_USER:-minioadmin} \${MINIO_PASS:-minioadmin} >/dev/null 2>&1
  mc find local/$MINIO_BUCKET/$TOPIC --name 'state.json' | wc -l | tr -d ' '
")"

SEGMENT_COUNT="$(cd "$ROOT_DIR" && docker compose run --rm --entrypoint sh setup-minio -c "
  mc alias set local http://minio:9000 \${MINIO_USER:-minioadmin} \${MINIO_PASS:-minioadmin} >/dev/null 2>&1
  mc find local/$MINIO_BUCKET/$TOPIC --name '*.segment' | wc -l | tr -d ' '
")"

META_COUNT="$(cd "$ROOT_DIR" && docker compose run --rm --entrypoint sh setup-minio -c "
  mc alias set local http://minio:9000 \${MINIO_USER:-minioadmin} \${MINIO_PASS:-minioadmin} >/dev/null 2>&1
  mc find local/$MINIO_BUCKET/$TOPIC --name '*.meta.json' | wc -l | tr -d ' '
")"

INDEX_COUNT="$(cd "$ROOT_DIR" && docker compose run --rm --entrypoint sh setup-minio -c "
  mc alias set local http://minio:9000 \${MINIO_USER:-minioadmin} \${MINIO_PASS:-minioadmin} >/dev/null 2>&1
  mc find local/$MINIO_BUCKET/$TOPIC --name '*.offset.idx' | wc -l | tr -d ' '
")"

echo "Post-run verification:"
echo "  topic: $TOPIC"
echo "  state.json objects: $STATE_COUNT"
echo "  segment objects: $SEGMENT_COUNT"
echo "  segment metadata objects: $META_COUNT"
echo "  segment index objects: $INDEX_COUNT"

if [ "$STATE_COUNT" -ne "$NUM_PARTITIONS" ]; then
  echo "expected $NUM_PARTITIONS state.json objects, found $STATE_COUNT" >&2
  exit 1
fi

if [ "$SEGMENT_COUNT" -le 0 ]; then
  echo "expected at least one flushed segment in S3 for topic $TOPIC" >&2
  exit 1
fi

if [ "$SEGMENT_COUNT" -ne "$META_COUNT" ] || [ "$SEGMENT_COUNT" -ne "$INDEX_COUNT" ]; then
  echo "segment asset mismatch: segments=$SEGMENT_COUNT meta=$META_COUNT index=$INDEX_COUNT" >&2
  exit 1
fi

echo "Strict 500-partition long-run verification passed."
