#!/bin/sh
set -eu

BENCH_REGEX=${BENCH_REGEX:-BenchmarkConsume}
BENCHTIME=${BENCHTIME:-3s}
COUNT=${COUNT:-1}
GOCACHE_DIR=${GOCACHE_DIR:-/tmp/camu-go-cache}

exec env GOCACHE="$GOCACHE_DIR" go test \
  -tags integration \
  -run '^$' \
  -bench "$BENCH_REGEX" \
  -benchtime "$BENCHTIME" \
  -count "$COUNT" \
  ./test/bench
