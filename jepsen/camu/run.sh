#!/bin/bash
set -e

FAULTS="${1:-kill}"
TIME_LIMIT="${2:-120}"

echo "Building camu for Linux..."
cd "$(dirname "$0")/../.."
GOOS=linux GOARCH=amd64 go build -o jepsen/camu/camu ./cmd/camu/
cd jepsen/camu

echo "Starting infrastructure (minio, nodes)..."
docker-compose up -d minio setup-minio n1 n2 n3 n4 n5

echo "Waiting for services to be ready..."
sleep 10

echo "Running Jepsen tests (faults=$FAULTS, time-limit=$TIME_LIMIT)..."
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
    --faults $FAULTS
"

echo "Results in store/latest/"
