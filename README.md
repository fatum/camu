# Camu

A stateless, S3-backed commit log server. Kafka-style partitioned topics with a simple HTTP/REST API.

## Why Camu

- **Stateless** — all durable state lives in S3. No ZooKeeper, no Raft, no cluster state to manage.
- **Simple API** — HTTP/REST with JSON. `curl` is your client.
- **S3-compatible** — works with AWS S3, MinIO, Cloudflare R2, Backblaze B2.
- **Crash-safe** — local WAL with fsync guarantees durability before acknowledging writes.
- **Multi-instance** — S3-based lease coordination with epoch fencing for partition ownership.

## Quick Start

```bash
# Build
go build -o camu ./cmd/camu

# Start MinIO (or use any S3-compatible storage)
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data

# Create bucket
docker exec <container> mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec <container> mc mb local/camu-data

# Configure
cp camu.yaml.example camu.yaml
# Edit camu.yaml with your S3 endpoint and credentials

# Run
./camu serve --config camu.yaml
```

## API

All endpoints return JSON. Every response includes `X-Camu-Instance-ID` header.

### Topics

```bash
# Create topic with 4 partitions and 7-day retention
curl -X POST http://localhost:8080/v1/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "partitions": 4, "retention": "168h"}'

# List topics
curl http://localhost:8080/v1/topics

# Get topic details
curl http://localhost:8080/v1/topics/orders

# Delete topic
curl -X DELETE http://localhost:8080/v1/topics/orders
```

### Producing

```bash
# Produce a single message (routed by key)
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "user-123", "value": "order placed"}'

# Produce a batch
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '[{"key": "u1", "value": "msg1"}, {"key": "u2", "value": "msg2"}]'

# Response: {"offsets": [{"partition": 3, "offset": 0}]}

# Produce to a specific partition
curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "k", "value": "direct"}'
```

Messages with the same key always go to the same partition (FNV-32a hash). No key = round-robin.

### Consuming

```bash
# Poll messages from a partition
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=100"
# Response: {"messages": [...], "next_offset": 5}

# Stream via Server-Sent Events
curl -N http://localhost:8080/v1/topics/orders/partitions/0/stream?offset=0 \
  -H "Accept: text/event-stream"
# id: 0
# data: {"offset":0,"timestamp":1234,"key":"k","value":"hello"}
```

SSE supports `Last-Event-ID` header for reconnection.

### Offset Checkpoints

Consumers track their own position via named offset checkpoints stored durably in S3.

```bash
# Commit offsets for a named group
curl -X POST http://localhost:8080/v1/groups/my-group/commit \
  -H "Content-Type: application/json" \
  -d '{"offsets": {"0": 100, "1": 200}}'

# Get group offsets
curl http://localhost:8080/v1/groups/my-group/offsets

# Per-consumer offsets
curl -X POST http://localhost:8080/v1/topics/orders/offsets/consumer-1 \
  -H "Content-Type: application/json" \
  -d '{"offsets": {"0": 42}}'

curl http://localhost:8080/v1/topics/orders/offsets/consumer-1
```

### Routing

```bash
# Get partition-to-instance mapping (for multi-instance deployments)
curl http://localhost:8080/v1/topics/orders/routing
# Response: {"partitions": {"0": {"instance_id": "abc", "address": "..."}, ...}}

# Cluster status
curl http://localhost:8080/v1/cluster/status
```

## Architecture

```
Producer Request
       |
  [HTTP Server]
       |
  [Partition Router]  -- hash(key) % N or round-robin
       |
  [WAL Append + fsync]  -- crash-safe, durable on local disk
       |
  [In-Memory Buffer]  -- serves real-time consumer reads
       |
  [Batcher]  -- flushes to S3 on size (8MB) or time (5s) threshold
       |
  [S3 Segment Upload]  -- immutable segment + index update
       |
  [Disk Cache]  -- cached locally for fast consumer reads
```

**Read path (tiered):**
1. In-memory buffer (unflushed data, owner instance only)
2. Disk segment cache (local, no S3 round-trip)
3. S3 fetch (cached on disk for next read)

Any instance can serve reads. Non-owners see data up to the last flush (5s delay).

## Multi-Instance

Multiple camu instances share the same S3 bucket. Partitions are owned via S3-based leases with epoch fencing.

- Lease TTL: 10s (configurable), heartbeat every 3s
- On instance failure: lease expires, another instance takes over
- Epoch fencing prevents stale instances from writing after recovery
- Clients discover partition owners via `GET /v1/topics/{topic}/routing`

Writes to a non-owning instance return `421 Misdirected Request` with the current routing map.

## Configuration

See [`camu.yaml.example`](camu.yaml.example) for all options. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `server.address` | `:8080` | HTTP listen address |
| `storage.bucket` | (required) | S3 bucket name |
| `storage.endpoint` | (AWS) | Custom S3 endpoint for MinIO/R2/B2 |
| `wal.fsync` | `true` | Fsync on every write |
| `segments.max_size` | 8MB | Segment flush size threshold |
| `segments.max_age` | 5s | Segment flush time threshold |
| `segments.compression` | `none` | `none`, `snappy`, or `zstd` |
| `cache.max_size` | 10GB | Disk cache size limit |
| `coordination.lease_ttl` | 10s | Partition lease TTL |

## Testing

```bash
# Unit tests
go test ./internal/...

# Integration tests (spins up in-memory S3 + camu instances)
go test -tags integration ./test/integration/ -timeout 120s

# Against a running deployment
./camu test --endpoint http://camu.example.com

# Benchmarks
./camu test --bench

# Chaos tests (random instance kills)
./camu test --chaos

# Jepsen (consistency verification with fault injection)
cd jepsen/camu && docker compose up --abort-on-container-exit control
```

### Jepsen Results

Verified on a 5-node cluster with process kill faults over 300 seconds. Mixed workload: 70% produce, 30% consume with kill/restart cycles every ~25s.

**Consistency checkers:**

| Checker | Result | Description |
|---------|--------|-------------|
| No split-brain | VALID | No two writers at same (partition, offset) |
| Total order | VALID | Offsets contiguous 0,1,2,...N per partition |
| Offset monotonicity | VALID | No gaps or duplicates |
| Lease fencing | VALID | Epoch fencing prevents stale writes after rejoin |

**Operational metrics:**

| Metric | Value |
|--------|-------|
| Total operations | 2,916 |
| Produce attempts / succeeded | 2,034 / 729 (36% availability under faults) |
| Consume attempts / succeeded | 878 / 789 |
| Drain (post-recovery verification) | 4/4 partitions drained |
| Recovery events measured | 50 |
| Min recovery time | 3.5 ms |
| Max recovery time | 20 s |
| Mean recovery time | 979 ms |

**Expected behavior:** Acked writes on killed instances may be lost if the WAL wasn't flushed to S3 before the kill. This is the documented `acks=1` trade-off — equivalent to Kafka with a single replica.

**Test configuration:** 5 nodes, 4 partitions, 10s lease TTL, `--faults kill`, `--time-limit 300`

## Project Structure

```
camu/
├── cmd/camu/              CLI (serve + test)
├── internal/
│   ├── config/            YAML config parsing
│   ├── consumer/          Fetcher, SSE streaming
│   ├── coordination/      S3-based leases, rebalancer
│   ├── log/               WAL, segments, index, cache, retention, GC
│   ├── meta/              Topic metadata in S3
│   ├── producer/          Partition router, batcher
│   ├── server/            HTTP handlers, partition manager
│   └── storage/           S3 client, offset storage
├── pkg/camutest/          Test environment (real HTTP, in-memory S3)
├── test/
│   ├── integration/       Integration + durability + chaos tests
│   └── bench/             Throughput benchmarks
├── jepsen/camu/           Jepsen consistency tests (Clojure)
└── docs/superpowers/      Design spec + implementation plan
```

## Trade-offs

- **Unflushed data loss on crash** — if an instance dies before flushing to S3, unflushed WAL data is lost when another instance takes over (equivalent to Kafka `acks=1`). The WAL is recoverable if the same instance restarts.
- **5s visibility delay on non-owner reads** — consumers reading from a non-owning instance only see data up to the last S3 flush.
- **S3 latency for offset commits** — offset checkpoints go to S3 (acceptable since commits are infrequent).

## License

MIT
