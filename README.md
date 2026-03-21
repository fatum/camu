# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A Kafka-like commit log that stores everything in S3.** No brokers to manage, no disks to provision, no replication to configure. One binary, one S3 bucket, `curl` is your client.

```bash
# Produce
curl -X POST http://localhost:8080/v1/topics/events/messages \
  -d '{"key": "user-123", "value": "clicked button"}'

# Consume
curl "http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0"
```

## When to Use Camu

**Use camu when:**
- You want Kafka-style ordered partitioned logs without operating Kafka
- Your storage costs matter more than sub-millisecond latency
- You need a message bus that scales to zero when idle (S3 costs only)
- You want any language to produce/consume via HTTP — no client libraries needed
- You're already on AWS/MinIO/R2 and want to keep infrastructure simple

**Use Kafka/Redpanda when:**
- You need sub-millisecond end-to-end latency (camu has ~5s flush delay)
- You need exactly-once semantics
- You need millions of messages/sec from a single partition
- You need the Kafka protocol for existing ecosystem compatibility

## How It Compares

| | Kafka | Redpanda | **Camu** |
|---|---|---|---|
| Storage | Local disk + replication | Local disk + Raft | **S3** ($0.023/GB/mo) |
| Ops dependencies | ZooKeeper or KRaft | None | **None** (just S3) |
| Protocol | Custom binary | Kafka-compatible | **HTTP/REST** |
| Min deployment | 3 nodes | 1 node | **1 binary + S3 bucket** |
| Cost at rest | Disk on every broker | Disk on every broker | **S3 only** (~free) |
| Latency (p99) | <10ms | <10ms | **~5s** (flush interval) |
| Throughput | 1M+ msgs/sec | 1M+ msgs/sec | **89K msgs/sec** (batched) |
| Scaling | Add brokers | Add brokers | **Add instances** (stateless) |
| Client libraries | Required | Required | **Any HTTP client** |
| Consistency | ISR replication | Raft | **S3 + epoch fencing** |

## Quick Start

### Docker (fastest)

```bash
# Start MinIO + camu in one command
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

# Create bucket
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/camu-data

# Run camu
docker run -d --name camu --net=host \
  -v /tmp/camu:/var/lib/camu \
  -e CAMU_STORAGE_BUCKET=camu-data \
  -e CAMU_STORAGE_ENDPOINT=http://localhost:9000 \
  -e CAMU_STORAGE_CREDENTIALS_ACCESS_KEY=minioadmin \
  -e CAMU_STORAGE_CREDENTIALS_SECRET_KEY=minioadmin \
  ghcr.io/fatum/camu serve
```

### From Source

```bash
go install github.com/fatum/camu/cmd/camu@latest

# Or build locally
git clone https://github.com/fatum/camu && cd camu
go build -o camu ./cmd/camu
cp camu.yaml.example camu.yaml  # edit with your S3 credentials
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
```

SSE supports `Last-Event-ID` header for automatic reconnection.

### Offset Checkpoints

Consumers track their own position via named checkpoints stored durably in S3.

```bash
# Commit offsets for a named group
curl -X POST http://localhost:8080/v1/groups/my-group/commit \
  -H "Content-Type: application/json" \
  -d '{"offsets": {"0": 100, "1": 200}}'

# Get group offsets
curl http://localhost:8080/v1/groups/my-group/offsets
```

### Routing

```bash
# Get partition-to-instance mapping (for multi-instance deployments)
curl http://localhost:8080/v1/topics/orders/routing

# Cluster status
curl http://localhost:8080/v1/cluster/status
```

## Client Examples

Camu uses plain HTTP — no client library needed. Use whatever language you want.

### Python

```python
import requests

# Produce
requests.post("http://localhost:8080/v1/topics/events/messages",
    json={"key": "user-1", "value": "signup"})

# Consume
resp = requests.get("http://localhost:8080/v1/topics/events/partitions/0/messages",
    params={"offset": 0, "limit": 50})
for msg in resp.json()["messages"]:
    print(f"offset={msg['offset']} key={msg['key']} value={msg['value']}")
```

### Node.js

```javascript
// Produce
await fetch("http://localhost:8080/v1/topics/events/messages", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({key: "user-1", value: "signup"})
});

// Consume via SSE
const es = new EventSource(
  "http://localhost:8080/v1/topics/events/partitions/0/stream?offset=0"
);
es.onmessage = (e) => console.log(JSON.parse(e.data));
```

### Go

```go
// Produce
body := `{"key": "user-1", "value": "signup"}`
http.Post("http://localhost:8080/v1/topics/events/messages",
    "application/json", strings.NewReader(body))

// Consume
resp, _ := http.Get("http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=50")
```

## Architecture

### Write Path

```
Producer HTTP Request
       |
  [Ownership Check]     local lease expiry check (zero I/O)
       |                  expired -> 421 Misdirected
       |
  [Partition Router]     hash(key) % N  or  round-robin
       |
  [WAL Append + fsync]   durable on local disk before ack
       |
  [HTTP Response]        write is durable
       |
  [Batcher]              tracks size counters (no message copies)
       |                  threshold: 8MB or 5s
       |
  [Flush]
       |
  [Verify Lease from S3]  re-check ownership before uploading
       |                    lost -> revoke partition, skip flush
       |
  [WAL Replay]           read unflushed messages from disk
       |
  [S3 Segment Upload]    immutable segment with epoch in filename
       |
  [Disk Cache Write]     cached locally at flush time
       |
  [Index Update]         conditional put to S3 index.json
       |
  [WAL Truncate]         remove flushed entries
```

**Write flow per message:**
1. HTTP request arrives — local lease check (no I/O), reject with 421 if not owned
2. Message routed to partition, appended to WAL + fsynced (~30us)
3. HTTP response sent — write is durable on local disk
4. Batcher tracks accumulated size; when threshold hit (8MB or 5s):
   - **Re-verifies lease from S3** — if ownership lost, revokes partition and skips flush
   - Reads unflushed messages from WAL (WAL is the single source of truth)
   - Serializes into immutable segment (binary format, optional snappy/zstd)
   - Uploads segment to S3 with epoch in filename (`{offset}-{epoch}.segment`)
   - Writes segment to local disk cache
   - Updates partition index in S3 (conditional put)
   - Truncates WAL

**No in-memory message buffering.** The batcher only tracks size counters — at flush time it reads directly from the WAL. Ownership is validated at two points: locally on every write (zero I/O) and from S3 on every flush.

### Read Path

```
Consumer HTTP Request
       |
  [Partition Index]     lookup offset -> segment key
       |
  [Disk Cache]          local segment file?
       |                  yes -> read from disk
       |                  no  -> fetch from S3, cache to disk
       |
  [Segment Parse]       deserialize from offset, return messages
```

**Read flow:**
1. Look up which segment contains the requested offset (from index)
2. Check local disk cache for the segment
3. Cache miss -> fetch from S3, write to disk cache
4. Parse segment binary, return messages from requested offset

All instances use the same read path. Consumers see data after it's flushed to S3 (default 5s).

### Performance

Benchmarked on Apple M3 Pro, single instance, in-memory S3:

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Produce (single message) | ~14,000 msgs/sec | 71 us/msg |
| Produce (batch of 100) | ~89,000 msgs/sec | 11 us/msg |
| Consume (100 msgs/poll) | ~320,000 msgs/sec | 3 us/msg |

**Bottlenecks:** Single produce is bound by HTTP round-trip + WAL fsync. Batching amortizes both -- 6x improvement. Consume is fast because segments are served from disk cache.

**With real S3:** Produce throughput is unchanged (WAL is local). Consume cold-reads add ~50-100ms per segment, then cached.

**Scaling:** Throughput scales linearly with partitions x instances. 3 instances with 12 partitions -> ~270K single msgs/sec or ~1M batched msgs/sec.

## Multi-Instance

Multiple camu instances share the same S3 bucket. Partitions are distributed evenly via the rebalancer.

**Partition ownership:**
- Round-robin assignment across sorted instance IDs
- S3-based leases with epoch fencing (10s TTL, 3s heartbeat)
- Automatic rebalance when nodes join or leave (~6-10s convergence)

**Node join:**
1. New node starts, discovers active instances from existing leases
2. Rebalancer computes fair partition assignment
3. New node acquires leases for its share
4. Existing nodes release excess partitions on next renewal cycle (~3s)

**Node failure:**
1. Instance dies, lease expires after TTL (10s)
2. Surviving instances detect expired leases on renewal
3. Rebalancer redistributes orphaned partitions

**Epoch fencing:** Each lease acquisition increments an epoch stored in S3. The epoch is embedded in segment filenames (`{offset}-{epoch}.segment`) and validated at multiple points:

| Check | When | Action on failure |
|-------|------|-------------------|
| Local lease expiry | Every produce request (zero I/O) | Reject with 421 |
| S3 lease verification | Every batcher flush (~5s) | Revoke partition, skip flush, reject future writes |
| WAL epoch sidecar | Partition init after restart | Discard stale WAL if epoch advanced |

**Ownership revocation flow:**
1. Batcher flush triggers S3 lease check
2. Another instance owns the partition -> `revokePartition()` deletes from local map
3. All subsequent produce requests fail instantly via local check (no I/O)
4. Stale WAL entries are never uploaded to S3

Clients discover partition owners via `GET /v1/topics/{topic}/routing` (returns instance addresses from lease data). Writes to a non-owning instance return `421 Misdirected Request` with the current routing map.

## Configuration

See [`camu.yaml.example`](camu.yaml.example) for all options.

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

**Consistency:**

| Checker | Result | Description |
|---------|--------|-------------|
| No split-brain | VALID | No two writers at same (partition, offset) |
| Total order | VALID | Offsets contiguous 0,1,2,...N per partition |
| Offset monotonicity | VALID | No gaps or duplicates |
| Lease fencing | VALID | Epoch fencing prevents stale writes after rejoin |

**Operations:** 2,976 total | 975 produce ok (47% availability under faults) | 779 consume ok | Mean recovery: 944ms

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

| Decision | Benefit | Cost |
|----------|---------|------|
| S3 as sole storage | Unlimited, cheap, no replication to manage | ~5s visibility delay (flush interval) |
| WAL on local disk | Fast acks (~30us), crash-safe | Unflushed data lost on failover (`acks=1`) |
| HTTP/REST API | Any language, no client library, curl-debuggable | Higher per-message overhead than binary protocols |
| Stateless instances | Scale to zero, no cluster state, easy deploys | Rebalance takes 6-10s on topology change |
| No in-memory buffer | Simple, WAL is single source of truth | All reads go through disk/S3, no real-time reads |

## Contributing

Contributions welcome! Here's how to get started:

```bash
git clone https://github.com/fatum/camu && cd camu

# Run all tests
go test ./internal/...
go test -tags integration ./test/integration/ -timeout 120s

# Run benchmarks
go test -tags integration ./test/bench/ -bench=. -benchtime=10s
```

**Areas we'd love help with:**
- Client libraries (Python, Node, Java)
- Terraform/Helm deployment templates
- Grafana dashboard for monitoring
- S3-compatible storage provider testing (R2, B2, GCS via S3 compat)
- Performance optimization (segment format, compression tuning)

Please open an issue before starting large changes.

## License

MIT
