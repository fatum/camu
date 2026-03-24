#!/bin/bash
set -e

FAULTS="${1:-kill}"
TIME_LIMIT="${2:-120}"
REPLICATION_FACTOR="${RF:-3}"
MIN_INSYNC_REPLICAS="${MIN_ISR:-2}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-camu-data}"

echo "Building camu for Linux..."
cd "$(dirname "$0")/../.."
GOOS=linux GOARCH=amd64 go build -o jepsen/camu/camu ./cmd/camu/
cd jepsen/camu

echo "Starting infrastructure (minio, nodes)..."
docker-compose up -d minio setup-minio n1 n2 n3 n4 n5

echo "Waiting for MinIO to be ready..."
until docker-compose run --rm setup-minio sh -c "mc alias set local http://minio:9000 $MINIO_USER $MINIO_PASS >/dev/null 2>&1"; do
  sleep 1
done

echo "Clearing existing S3 state from bucket $MINIO_BUCKET..."
docker-compose run --rm setup-minio sh -c "mc alias set local http://minio:9000 $MINIO_USER $MINIO_PASS >/dev/null 2>&1 && mc rm --recursive --force local/$MINIO_BUCKET >/dev/null 2>&1 || true"

echo "Rebuilding Jepsen control image..."
docker-compose build control

echo "Waiting for services to be ready..."
sleep 10

echo "Running Jepsen tests (faults=$FAULTS, time-limit=$TIME_LIMIT, rf=$REPLICATION_FACTOR, minISR=$MIN_INSYNC_REPLICAS)..."
docker-compose run --rm control bash -c "
  echo 'Distributing SSH keys to nodes...' &&
  for n in n1 n2 n3 n4 n5; do
    until ssh-keyscan \$n >> /root/.ssh/known_hosts 2>/dev/null; do sleep 1; done
    sshpass -p root ssh-copy-id -o StrictHostKeyChecking=no root@\$n 2>/dev/null || true
  done &&
  echo 'Running Jepsen test...' &&
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --ssh-private-key /root/.ssh/id_rsa \
    --time-limit $TIME_LIMIT \
    --s3-endpoint http://minio:9000 \
    --camu-binary /jepsen/camu/camu \
    --faults $FAULTS \
    --replication-factor $REPLICATION_FACTOR \
    --min-insync-replicas $MIN_INSYNC_REPLICAS
"

echo "Results in store/latest/"
