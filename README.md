# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

**An S3-native event log with an HTTP API.** Camu keeps durable log state in object storage, uses a local WAL for fast acknowledgements, and coordinates multi-instance ownership through S3 leases instead of a broker quorum.

```bash
# Create a topic
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":4,"retention":"168h"}'

# Produce
curl -X POST http://localhost:8080/v1/topics/events/messages \
  -H 'Content-Type: application/json' \
  -d '{"key":"user-123","value":"clicked"}'

# Consume
curl "http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=100"
```

## Why Camu

- S3-native storage for segments, indexes, topic metadata, offsets, and coordination state
- One Go binary, no ZooKeeper, no Raft cluster, no external metadata service
- HTTP-first API for produce, consume, and SSE streaming
- Kafka-style replicated topics with `replication_factor` and `min_insync_replicas`
- Idempotent produce with producer IDs and sequence tracking
- Repository-local Jepsen evidence for failover, partition, pause, churn, and storage-isolation faults

## What An Ack Means

- `rf=1`, `minISR=1`: a successful produce is durable in the local WAL on the owning node
- `rf>1`: a successful produce is durable in the leader WAL and only acknowledged after the configured ISR quorum confirms it
- Flush to S3 is asynchronous. Cross-instance visibility for non-replica reads follows segment flush timing, not ack timing
- Reads are capped by the readable high watermark, so consumers do not observe uncommitted replicated writes

## Quick Start

### Docker + MinIO

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/camu-data

mkdir -p /tmp/camu
cat >/tmp/camu/camu.yaml <<'EOF'
server:
  address: ":8080"
  internal_address: ":8081"
storage:
  bucket: "camu-data"
  region: "us-east-1"
  endpoint: "http://localhost:9000"
  credentials:
    access_key: "minioadmin"
    secret_key: "minioadmin"
wal:
  directory: "/var/lib/camu/wal"
  fsync: true
  chunk_size: 67108864
segments:
  max_size: 8388608
  max_age: "5s"
  compression: "none"
  record_batch_target_size: 16384
  index_interval_bytes: 4096
cache:
  directory: "/var/lib/camu/cache"
  max_size: 10737418240
coordination:
  lease_ttl: "30s"
  heartbeat_interval: "10s"
  rebalance_delay: "5s"
EOF

docker run -d --name camu --net=host \
  -v /tmp/camu:/var/lib/camu \
  -v /tmp/camu/camu.yaml:/etc/camu/camu.yaml:ro \
  ghcr.io/fatum/camu serve --config /etc/camu/camu.yaml
```

### From Source

```bash
git clone https://github.com/fatum/camu
cd camu

go build -o camu ./cmd/camu
cp camu.yaml.example camu.yaml
# edit storage.* to point at your bucket

./camu serve --config camu.yaml
```

Check readiness:

```bash
curl http://localhost:8080/v1/ready
```

## API Overview

### Topics

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"orders",
    "partitions":4,
    "retention":"168h",
    "replication_factor":3,
    "min_insync_replicas":2
  }'

curl http://localhost:8080/v1/topics
curl http://localhost:8080/v1/topics/orders
curl -X DELETE http://localhost:8080/v1/topics/orders
```

### Produce

High-level produce routes by key. The same key always maps to the same partition; messages without a key use round-robin routing.

```bash
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '{"key":"user-123","value":"order placed","headers":{"trace-id":"abc"}}'

curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"u1","value":"m1"},{"key":"u2","value":"m2"}]'

curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '{"value":"direct-partition-write"}'
```

If a request lands on a non-owner, Camu either proxies internally to the leader or returns `421 Misdirected Request` with the current routing map:

```bash
curl http://localhost:8080/v1/topics/orders/routing
```

### Idempotent Produce

Idempotent produce is tracked per `(producer_id, topic, partition)` and is only supported on the partition-specific produce endpoint:

```text
POST /v1/topics/{topic}/partitions/{id}/messages
```

The high-level routed endpoint rejects idempotent batch bodies with `400 Bad Request`.

The client normally allocates a producer ID once, then sends a monotonically increasing `sequence` for that partition stream.

```bash
curl -X POST http://localhost:8080/v1/producers/init
# {"producer_id":42}

curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "producer_id":42,
    "sequence":0,
    "messages":[
      {"key":"u1","value":"hello"},
      {"key":"u2","value":"world"}
    ]
  }'

curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "producer_id":42,
    "sequence":2,
    "messages":[
      {"key":"u3","value":"next batch"}
    ]
  }'
```

Retrying the first batch with the same `producer_id`, `sequence`, and routed messages is safe:

```bash
curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "producer_id":42,
    "sequence":0,
    "messages":[
      {"key":"u1","value":"hello"},
      {"key":"u2","value":"world"}
    ]
  }'
```

Current behavior:

- duplicate sequence: `200 OK` with `{"duplicate":true}`
- sequence gap: `422`
- unknown producer with non-zero sequence: `422`
- omit `producer_id` and `sequence` for regular at-least-once produce
- a fresh producer ID with `sequence=0` is auto-registered, but `POST /v1/producers/init` is the intended path

Operational details:

- sequence advances by the batch size for that partition
- duplicate retries do not append again
- on replicated topics, duplicate retries wait for the original batch to reach the replicated commit point before returning success
- the manager stores the last offset of the accepted batch so duplicate retries can join that commit wait
- idempotency state is checkpointed during flush and rebuilt from WAL metadata on recovery
- inactive producer state is eventually evicted, so producer IDs are not meant to live forever

Practical rules:

- retry the exact same request body when re-sending a batch
- keep one sequence stream per `(producer_id, topic, partition)`
- use `POST /v1/producers/init` to get a unique producer ID
- send idempotent batches only to `POST /v1/topics/{topic}/partitions/{id}/messages`

This gives exactly-once produce deduplication for retried batches on a partition stream. It is not a cross-topic transaction system and it does not make consumer-side processing exactly-once by itself.

### Consume

```bash
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=100"

curl -N \
  -H 'Accept: text/event-stream' \
  "http://localhost:8080/v1/topics/orders/partitions/0/stream?offset=0"
```

Polling returns JSON with `messages` and `next_offset`. SSE uses `id: {offset}` and resumes from `Last-Event-ID + 1`.

### Offsets

```bash
curl -X POST http://localhost:8080/v1/groups/my-group/commit \
  -H 'Content-Type: application/json' \
  -d '{"offsets":{"0":100,"1":200}}'

curl http://localhost:8080/v1/groups/my-group/offsets

curl -X POST http://localhost:8080/v1/topics/orders/offsets/consumer-1 \
  -H 'Content-Type: application/json' \
  -d '{"offsets":{"0":100}}'

curl http://localhost:8080/v1/topics/orders/offsets/consumer-1
```

## Multi-Instance Model

Multiple Camu instances can share one bucket.

- Topic config lives in `_meta/topics/{topic}.json`
- Partition ownership lives in `_coordination/assignments/{topic}.json`
- Instance liveness lives in `_coordination/instances/{instanceID}.json`
- Segments live under `{topic}/{partition}/`
- Offsets live in `_coordination/groups/` and `_coordination/consumers/`

Ownership is fenced in three places:

- local ownership checks reject writes on stale owners
- every flush re-verifies ownership from S3 before uploading a segment
- segment filenames include the leader epoch, so stale uploads are distinguishable

Replicated topics use ISR-style follower fetch from the leader over the internal h2c listener on `server.internal_address`.

## Configuration

`camu.yaml.example` is the canonical starting point. Current defaults from the code path:

| Setting | Default | Notes |
|---------|---------|-------|
| `server.address` | `:8080` | Public HTTP API |
| `server.internal_address` | `:8081` | Internal h2c listener for replication and leader proxying |
| `server.instance_id` | auto-generated UUID | Stable IDs are useful for fixed deployments |
| `storage.bucket` | required | S3 or compatible object store bucket |
| `storage.region` | empty unless set | Required by many S3 providers |
| `storage.endpoint` | empty | Set this for MinIO, R2, Backblaze, etc. |
| `wal.directory` | `/var/lib/camu/wal` | Local durability path |
| `wal.fsync` | `true` | Ack waits for WAL fsync |
| `wal.chunk_size` | `64 MiB` | Active chunk rotation threshold |
| `segments.max_size` | `8 MiB` | Flush-by-size threshold |
| `segments.max_age` | `5s` | Flush-by-time threshold |
| `segments.compression` | `none` | `none`, `snappy`, `zstd` |
| `segments.record_batch_target_size` | `16 KiB` | Batch framing target inside a segment |
| `segments.index_interval_bytes` | `4096` | Sparse segment offset-index cadence |
| `cache.directory` | `/var/lib/camu/cache` | Local segment cache |
| `cache.max_size` | `10 GiB` | Disk LRU cap |
| `coordination.lease_ttl` | `30s` | Lease expiry window |
| `coordination.instance_ttl` | `lease_ttl * 3` | Defaults to `90s` when omitted |
| `coordination.heartbeat_interval` | `10s` | Lease renewal interval |
| `coordination.rebalance_delay` | `5s` | Delay before publishing new assignments |
| `coordination.isr_expansion_threshold` | `1000` | Follower catch-up threshold before ISR rejoin |
| `coordination.replication_timeout` | `30s` | Produce wait timeout for replicated topics |

## Reliability

The checked-in Jepsen harness currently verifies:

- replicated mode: `rf=3`, `minISR=2`
- strict quorum: `rf=3`, `minISR=3`
- control mode: `rf=1`, `minISR=1`

As of March 23, 2026, the repository documents 21 passing fault scenarios across kill, leader kill, pause, partition, rejoin, membership churn, S3 isolation, and combined-fault runs. See [docs/reliability.md](docs/reliability.md) for the matrix and [jepsen/camu/README.md](jepsen/camu/README.md) for the harness.

## Project Layout

```text
cmd/camu/          CLI entry point
internal/config/   Config loading and validation
internal/log/      WAL, segments, indexes, cache, retention
internal/server/   HTTP handlers, partition manager, routing
internal/replication/ ISR replication and follower fetch
internal/coordination/ S3 leases, registry, assignment store
internal/meta/     Topic metadata
internal/storage/  S3 client and offset persistence
pkg/camutest/      Multi-instance test helpers
test/integration/  Real-server integration tests
test/bench/        Benchmarks
jepsen/camu/       Jepsen fault-injection harness
docs/              Architecture, reliability, and design notes
```

## Development

```bash
go test ./internal/...
go test -tags integration ./test/integration/ -timeout 120s
go test -tags integration ./test/bench/ -bench=. -benchtime=10s

cd jepsen/camu
./run.sh
./run.sh leader-kill 45
RF=3 MIN_ISR=3 ./run.sh leader-kill,s3-partition 45
```

## More Docs

- [docs/architecture.md](docs/architecture.md)
- [docs/reliability.md](docs/reliability.md)
- [docs/multiple-indexes.md](docs/multiple-indexes.md)
- [jepsen/camu/README.md](jepsen/camu/README.md)

## License

AGPL-3.0. See [LICENSE](LICENSE).
