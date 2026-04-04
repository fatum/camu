# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

**A Jepsen-verified, S3-native event log with HTTP and Kafka wire protocol support.** Camu stores native Kafka RecordBatch data in local active segments, seals immutable segments to object storage, and coordinates multi-instance ownership through S3 conditional writes — no ZooKeeper, no Raft, no external metadata service.

```
                   +-----------+     +-----------+
                   | Producer  |     | Producer  |
                   +-----+-----+     +-----+-----+
                         |                 |
                   HTTP / Kafka        HTTP / Kafka
                         |                 |
                 +-------v-----------------v-------+
                 |          Camu Cluster            |
                 |  +------+  +------+  +------+   |
                 |  | n1   |  | n2   |  | n3   |   |
                 |  | lead |<-| foll |<-| foll |   |
                 |  +--+---+  +------+  +------+   |
                 +-----|---------------------------+
                       | seal + flush
                 +-----v-----+
                 |  S3 / R2  |
                 |  MinIO    |
                 +-----------+
```

```bash
# Create a topic
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":4,"retention":"168h"}'

# Produce
curl -X POST http://localhost:8080/v1/topics/events/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"user-123","value":"clicked"}]'

# Consume
curl "http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=100"
```

## Why Camu

| | |
|---|---|
| **S3-native** | Segments, indexes, topic metadata, offsets, and coordination state all live in object storage. No local disk is required for durability. |
| **Single binary** | One Go binary. No ZooKeeper, no Raft cluster, no external metadata service. Deploy as a static binary, a container, or a systemd unit. |
| **Dual protocol** | HTTP-first API for produce, consume, SSE streaming, and topic management. Full Kafka wire protocol (v0-v12) for drop-in client compatibility. |
| **ISR replication** | Kafka-style replicated topics with configurable `replication_factor` and `min_insync_replicas`. Writes are only acknowledged after the ISR quorum confirms. |
| **Exactly-once produce** | Idempotent produce with producer IDs and sequence tracking. Duplicate retries on replicated topics wait for the original batch commit before confirming. |
| **Jepsen-verified** | 22 passing fault scenarios across kill, leader-kill, pause, partition, rejoin, membership churn, S3 isolation, clock skew, and combined-fault runs. Every claim has a reproducible artifact. |

## What An Ack Means

| Mode | Durability guarantee |
|------|---------------------|
| `rf=1`, `minISR=1` | A successful produce is durable in the local active segment on the owning node. |
| `rf>1` | A successful produce is durable on the leader **and** only acknowledged after the configured ISR quorum confirms it. |

Flush to S3 is asynchronous. Cross-instance visibility for non-replica reads follows segment flush timing, not ack timing. Reads are capped by the readable high watermark, so consumers never observe uncommitted replicated writes.

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
  kafka_port: 9092
storage:
  bucket: "camu-data"
  region: "us-east-1"
  endpoint: "http://localhost:9000"
  credentials:
    access_key: "minioadmin"
    secret_key: "minioadmin"
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
  -d '[{"key":"user-123","value":"order placed","headers":{"trace-id":"abc"}}]'

curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"u1","value":"m1"},{"key":"u2","value":"m2"}]'

curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '[{"value":"direct-partition-write"}]'
```

Both produce endpoints are batch-shaped: send a JSON array for regular produce, even for a single message. The partition-specific endpoint also accepts the idempotent batch object shown below.

If a request lands on a non-owner, Camu either proxies internally to the leader or returns `421 Misdirected Request` with the current routing map:

```bash
curl http://localhost:8080/v1/topics/orders/routing
```

### Kafka Wire Protocol

Camu implements the Kafka wire protocol for seamless client compatibility. Any Kafka client (librdkafka, franz-go, kafka-python, etc.) can produce to and consume from Camu without modification.

```bash
# Using kcat
echo "hello" | kcat -b localhost:9092 -t orders -P
kcat -b localhost:9092 -t orders -C -e
```

Supported Kafka APIs: Produce, Fetch, Metadata, ListOffsets, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups, DeleteGroups, CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs, IncrementalAlterConfigs, DescribeCluster, ApiVersions, InitProducerID, and ACL operations.

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
- idempotency state is checkpointed during flush and rebuilt from native batch metadata on recovery
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
| `server.kafka_port` | `9092` | Kafka wire protocol listener |
| `server.instance_id` | auto-generated UUID | Stable IDs are useful for fixed deployments |
| `storage.bucket` | required | S3 or compatible object store bucket |
| `storage.region` | empty unless set | Required by many S3 providers |
| `storage.endpoint` | empty | Set this for MinIO, R2, Backblaze, etc. |
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

## Jepsen Verification

Camu ships with a repository-local Jepsen harness that runs a five-node Docker cluster against MinIO-backed storage, injects adversarial faults, then drains all partitions and verifies the observed history against acknowledged operations.

**22 passing fault scenarios** across three replication modes:

| Mode | Faults tested |
|------|---------------|
| `rf=3`, `minISR=2` | kill, partition, pause, leader-kill, leave, membership, rejoin, s3-partition, clock-skew, kill+partition, leader-kill+s3-partition (10s and 45s durations) |
| `rf=3`, `minISR=3` | kill, leader-kill, membership, s3-partition, leader-kill+s3-partition |
| `rf=1`, `minISR=1` | kill |

**9 correctness checkers** verify every run:

| Checker | What it proves |
|---------|----------------|
| committed-durability | Acknowledged writes survive to final drain |
| single-leader | No conflicting values at the same (partition, offset) |
| total-order | Partition histories remain ordered and contiguous |
| offset-monotonicity | Offsets never duplicate or regress |
| truncation-safety | Committed suffixes are not lost after failover |
| hw-monotonicity | Observed high watermarks do not go backward |
| no-ghost-reads | Reads do not invent unacknowledged data |
| availability | Successful operation ratio during faults |
| recovery-time | Latency from injected fault to next success |

See [docs/reliability.md](docs/reliability.md) for the full matrix and [jepsen/camu/README.md](jepsen/camu/README.md) for the harness.

## Project Layout

```text
cmd/camu/                        CLI entry point
internal/
  config/                        Config loading and validation
  log/                           Active segments, sealed segments, indexes, cache, retention
  server/
    handlers_produce.go          HTTP produce handlers (high-level + partition-specific)
    handlers_consume.go          HTTP consume handlers (polling + SSE streaming)
    handlers_producers.go        Producer ID allocation
    produce_types.go             Produce request/response DTOs
    produce_parse.go             Body parsing and decoding utilities
    produce_append.go            RecordBatch append fast-path, replication wait
    produce_leadership.go        Leadership proxy/reject helpers
    consume_types.go             Consume response DTOs
    consume_iterator.go          Multi-source merge iterator
    consume_stream.go            Streaming JSON response writer
    consume_helpers.go           High watermark, message merge, encoding
    kafka_types.go               Kafka server types, config, interfaces
    kafka_wire.go                Kafka wire protocol: conn, decode, encode, routing
    kafka_handlers_admin.go      Kafka admin API handlers (25 APIs)
    kafka_handlers_data.go       Kafka produce + fetch handlers
    kafka_codec.go               RecordBatch encode/decode, compression
    kafka_helpers.go             Partition lookup, error mapping, adapters
    kafka_groups.go              Kafka consumer group coordinator (S3-backed)
    server.go                    HTTP server, routing, lifecycle
    partition_manager.go         Partition state, flush, recovery
  replication/                   ISR replication and follower fetch
  coordination/                  S3 leases, registry, assignment store
  meta/                          Topic metadata
  storage/                       S3 client and offset persistence
  consumer/                      SSE streaming support
  producer/                      Backpressure and producer utilities
  idempotency/                   Producer sequence tracking and deduplication
pkg/camutest/                    Multi-instance test helpers
test/integration/                Real-server integration tests
test/bench/                      Benchmarks
jepsen/camu/                     Jepsen fault-injection harness (Clojure)
docs/                            Architecture, reliability, and design notes
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

- [docs/architecture.md](docs/architecture.md) -- System architecture and data path
- [docs/architecture/coordination.md](docs/architecture/coordination.md) -- Coordination, failover, and consumer groups
- [docs/reliability.md](docs/reliability.md) -- Correctness claims and verification layers
- [jepsen/camu/README.md](jepsen/camu/README.md) -- Jepsen harness, fault matrix, and checkers

## License

AGPL-3.0. See [LICENSE](LICENSE).
