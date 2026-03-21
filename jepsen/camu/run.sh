#!/bin/bash
set -e

echo "Building camu for Linux..."
cd "$(dirname "$0")/../.."
GOOS=linux GOARCH=amd64 go build -o jepsen/camu/camu ./cmd/camu/
cd jepsen/camu

echo "Starting Jepsen environment..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 10

echo "Running Jepsen tests..."
docker-compose exec control lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --time-limit 120 \
    --s3-endpoint http://minio:9000 \
    --camu-binary /jepsen/camu/camu

echo "Results in store/latest/"
