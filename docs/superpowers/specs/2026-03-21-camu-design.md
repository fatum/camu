# Camu — Stateless S3-Backed Commit Log

## Overview

Camu is an open-source, Kafka-like commit log server written in Go. It is deployed as a single binary, exposes a simple HTTP/REST API, and uses S3-compatible object storage as its persistent backend. A local write-ahead log (WAL) provides crash-safe, low-latency writes while S3 provides durable long-term storage.

The system supports Kafka-style partitioned topics, consumer groups with server-managed offsets, and horizontal scaling via partition-level lease coordination stored in S3.

## Goals

- Simple HTTP/REST API — no gRPC, no custom protocols
- Stateless by default — all durable state lives in S3
- Open-source library quality — clean API, good docs, broad compatibility
- High throughput via batching and local WAL
- Pluggable storage backends with S3-compatible as default

## Core Data Model

### Topic

A named, logical stream of messages. Created via API. Configured with a partition count and a retention duration.

### Partition

A single ordered, append-only log within a topic. Identified by `{topic}/{partition_id}`. Partitions are the unit of parallelism — each is consumed independently. Each partition is owned by exactly one camu instance at a time.

### Message

The unit of data. Contains:

- `Key` (optional bytes) — used for partition routing via `hash(key) % num_partitions`
- `Value` (bytes) — the payload
- `Offset` (uint64) — assigned by the server, monotonically increasing per partition
- `Timestamp` (unix nanos) — server-assigned on write
- `Headers` (map[string]string) — optional metadata

If `Key` is omitted, messages are round-robined across partitions.

### Segment

The storage unit. A partition's log is split into segments, each stored as a single S3 object. A segment contains a batch of messages serialized together. Segments are immutable once flushed to S3.

S3 key: `{bucket}/{topic}/{partition_id}/{base_offset}-{epoch}.segment`

The `epoch` is the lease epoch under which the segment was written (see Lease Fencing below).

### Partition Index

An index object per partition tracks segment boundaries for fast offset-to-segment lookup.

S3 key: `{bucket}/{topic}/{partition_id}/index.json`

### Instance

A running camu server process. Identified by a unique instance ID (auto-generated UUID on startup). Registers itself in the coordination store and heartbeats to maintain liveness.

### Lease

An S3 object per partition for ownership coordination.

S3 key: `{bucket}/_coordination/leases/{topic}/{partition_id}.lease`

Contains the owning instance ID, lease epoch, lease expiry timestamp, and an ETag for conditional updates. Each lease acquisition increments the epoch. Instances renew leases before expiry; expired leases are claimable by any instance.

## Storage Layer

### Storage Interface

All persistence goes through a pluggable `Storage` interface:

```go
type Storage interface {
    Put(ctx context.Context, key string, data []byte, opts PutOpts) error
    Get(ctx context.Context, key string) ([]byte, error)
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
    ConditionalPut(ctx context.Context, key string, data []byte, etag string) (newEtag string, err error)
}
```

`ConditionalPut` is critical for lease coordination — it maps to S3's `If-Match` conditional writes.

### Default Implementation

S3-compatible client using the AWS SDK v2. Works with AWS S3, MinIO, Cloudflare R2, Backblaze B2 — anything that speaks the S3 API. Users provide bucket name, region, endpoint URL, and credentials.

### S3 Object Layout

```
{bucket}/
  {topic}/
    {partition_id}/
      index.json
      {base_offset}.segment
  _coordination/
    leases/
      {topic}/{partition_id}.lease
    groups/
      {group_id}/offsets.json
    consumers/
      {consumer_id}/offsets.json
  _meta/
    topics/{topic}.json
```

### Segment Format

Binary format:

- 4-byte magic number
- 1-byte version
- Repeated message frames: `[8-byte offset][8-byte timestamp][4-byte key len][key bytes][4-byte value len][value bytes][headers count][header pairs]`
- Optional segment-level compression: snappy or zstd (configurable per topic)

### Segment Lifecycle

1. Active segment lives in memory, accumulating writes from the WAL
2. Flushed to S3 when it hits a size threshold (default 8MB) or time threshold (default 5s)
3. After flush, the partition index is updated via conditional put
4. Flushed segments are immutable

### Retention Cleanup

A background goroutine periodically scans partition indexes, identifies segments older than the retention duration, deletes them from S3, and updates the index.

## Write-Ahead Log (WAL)

Every message is appended to a local WAL file and fsynced before the client receives a response. This provides crash-safe, low-latency writes decoupled from S3 round-trips.

- One WAL file per owned partition, stored at `{wal_directory}/{topic}/{partition_id}.wal`
- Background flusher reads from the WAL, batches entries into segments, uploads to S3, and truncates the WAL up to the flushed offset
- On startup or lease acquisition, the WAL is replayed to recover unflushed messages
- Every acknowledged write is durable on local disk

## Producer Path

### Write Flow

1. Producer sends `POST /v1/topics/{topic}/messages` with message(s) containing key, value, headers
2. Server hashes each message's key to determine the target partition (round-robin if no key)
3. If this instance owns the target partition: append to local WAL, fsync, assign offset, respond to client
4. If this instance does not own the target partition: return `421 Misdirected Request` with the current routing map so the client can update its routing table and retry to the correct instance
5. Background flusher batches WAL entries into segments, uploads to S3, updates index, truncates WAL

### Batching

Messages are batched in the WAL and flushed to S3 as segments. Flush triggers:

- Buffer size threshold (configurable, default 8MB)
- Time threshold (configurable, default 5s)
- Whichever comes first

### Backpressure

If the in-memory buffer exceeds a high-water mark, the server returns `503 Service Unavailable` with a `Retry-After` header.

## Consumer Path

### Polling (Default)

```
GET /v1/topics/{topic}/partitions/{id}/messages?offset=N&limit=100
```

Server resolves the requested offset through a tiered read path:

1. **In-memory buffer** — if this instance owns the partition and the offset is in the active (unflushed) buffer, serve directly from memory
2. **Local segment cache** — check the LRU cache for the segment containing the offset
3. **S3 fetch** — fetch the segment from S3, cache it locally, then serve

Response includes `next_offset`.

### SSE (Opt-in)

```
GET /v1/topics/{topic}/partitions/{id}/stream?offset=N
Accept: text/event-stream
```

Server holds the connection open and pushes messages as SSE events. For historical messages, reads from S3 segments. For live messages, taps into the in-memory buffer. Client reconnects with `Last-Event-ID` on disconnect.

### Segment Cache

A local LRU cache holds recently-read segments in memory. Segments fetched from S3 for consumer reads are automatically cached so subsequent reads of the same offset range are served locally. Since segments are immutable once flushed, no invalidation is needed. Cache size is configurable (default 512MB).

### Consumer Groups

- `POST /v1/groups/{group_id}/join` — join a group for a topic, server assigns partitions
- `POST /v1/groups/{group_id}/heartbeat` — maintain membership
- `POST /v1/groups/{group_id}/commit` — commit offsets for assigned partitions
- `GET /v1/groups/{group_id}/offsets` — get committed offsets
- `POST /v1/groups/{group_id}/leave` — leave group, trigger rebalance

Partition assignment uses range or round-robin strategy. Missed heartbeats trigger rebalance.

### Consumer-Specific Offsets

Individual consumers can manage their own offsets independently of consumer groups:

```
POST /v1/topics/{topic}/offsets/{consumer_id}   — commit offsets for a standalone consumer
GET  /v1/topics/{topic}/offsets/{consumer_id}   — get committed offsets for a standalone consumer
```

This allows consumers to track their position without joining a group — useful for single consumers, reprocessing pipelines, or custom offset management.

### Offset Storage

Committed offsets (both group and consumer-specific) go through a pluggable storage interface. Default: S3 at `_coordination/groups/{group_id}/offsets.json` for groups and `_coordination/consumers/{consumer_id}/offsets.json` for standalone consumers. Users can plug in Redis or DynamoDB for faster commits.

## HTTP API

### Topic Management

```
POST   /v1/topics                                          — create topic
GET    /v1/topics                                          — list topics
GET    /v1/topics/{topic}                                  — get topic details
DELETE /v1/topics/{topic}                                  — delete topic
```

### Producing (High-Level)

```
POST   /v1/topics/{topic}/messages                         — produce, routed by key
```

### Producing (Low-Level)

```
POST   /v1/topics/{topic}/partitions/{id}/messages         — produce to specific partition
```

### Consuming (Partition-Transparent)

```
GET    /v1/topics/{topic}/consume?group={g}                — poll with group
GET    /v1/topics/{topic}/stream?group={g}                 — SSE with group
```

### Consuming (Low-Level)

```
GET    /v1/topics/{topic}/partitions/{id}/messages?offset=N&limit=100  — poll
GET    /v1/topics/{topic}/partitions/{id}/stream?offset=N              — SSE
```

### Consumer Groups

```
POST   /v1/groups/{group_id}/join                          — join group
POST   /v1/groups/{group_id}/heartbeat                     — heartbeat
POST   /v1/groups/{group_id}/commit                        — commit offsets
GET    /v1/groups/{group_id}/offsets                        — get offsets
POST   /v1/groups/{group_id}/leave                         — leave group
```

### Consumer-Specific Offsets

```
POST   /v1/topics/{topic}/offsets/{consumer_id}            — commit offsets
GET    /v1/topics/{topic}/offsets/{consumer_id}             — get offsets
```

### Routing

```
GET    /v1/topics/{topic}/routing                          — partition → instance mapping
```

Response:
```json
{
  "partitions": {
    "0": {"instance_id": "abc-123", "address": "http://10.0.1.5:8080"},
    "1": {"instance_id": "abc-123", "address": "http://10.0.1.5:8080"},
    "2": {"instance_id": "def-456", "address": "http://10.0.1.6:8080"}
  }
}
```

### Cluster

```
GET    /v1/cluster/status                                  — instance list, assignments, health
```

All responses are JSON. Errors use standard HTTP status codes with `{"error": "message"}` body.

Every response includes the `X-Camu-Instance-ID` header identifying which instance served the request.

## Multi-Instance Coordination

### Partition Ownership

Each camu instance owns a subset of partitions via S3-based leases. Only the owner can write to a partition. Leases use S3 conditional writes (`If-Match`) for conflict-free acquisition.

### Lease Lifecycle

- On startup, instance registers itself and attempts to acquire leases for unowned partitions
- Leases have a configurable TTL (default 5-10s), renewed before expiry via heartbeat
- If an instance dies, its leases expire and other instances claim them during rebalance (unavailability window equals the remaining TTL)
- Rebalance uses round-robin or consistent hashing across live instances

### Lease Fencing

Each lease acquisition increments an epoch number stored in the lease object. The epoch prevents conflicts when a failed instance recovers:

1. Instance A owns partition 3 at epoch 5, has unflushed WAL entries
2. Instance A dies, lease expires
3. Instance B acquires partition 3 at epoch 6, reads the index, starts assigning offsets from the last flushed offset
4. Instance A recovers — for each previously owned partition, it checks the current lease epoch
5. Epoch has advanced (6 > 5) → Instance A discards its local WAL for that partition (data is stale, those offsets have been reassigned)
6. If epoch has NOT advanced (instance recovered before lease expired) → replay WAL normally

The epoch is embedded in segment filenames (`{base_offset}-{epoch}.segment`) and validated during index updates via conditional put.

**Trade-off:** unflushed data on a dead instance is lost if another instance takes over. This is accepted behavior for the initial implementation — equivalent to Kafka with `acks=1`. Standby replicas (future work) would replicate the WAL to eliminate this risk.

### Write Routing

Clients fetch the partition-to-instance routing map via `GET /v1/topics/{topic}/routing` and cache it locally. They send writes directly to the owning instance.

If a write hits the wrong instance (stale routing), the server returns `421 Misdirected Request` with the current routing map. The client updates its cache and retries.

## Configuration

Single YAML file or environment variables:

```yaml
server:
  address: ":8080"
  instance_id: ""

storage:
  backend: "s3"
  bucket: "camu-data"
  region: "us-east-1"
  endpoint: ""
  credentials:
    access_key: ""
    secret_key: ""

wal:
  directory: "/var/lib/camu/wal"
  fsync: true

segments:
  max_size: 8388608
  max_age: "5s"
  compression: "snappy"

cache:
  segment_cache_size: 536870912

coordination:
  lease_ttl: "10s"
  heartbeat_interval: "3s"
  rebalance_delay: "5s"

groups:
  offset_store: "s3"
```

## Deployment

- Single binary: `camu serve --config camu.yaml`
- Multiple instances point to the same S3 bucket — they discover each other via coordination leases
- No external dependencies beyond S3-compatible storage and local disk for WAL
- All instances must be directly network-reachable to clients since clients route writes to specific instances based on the routing map
- Health check at `GET /v1/cluster/status`

### Graceful Shutdown

1. Stop accepting new writes
2. Flush all in-memory buffers to S3
3. Release all partition leases
4. Exit

## Integration Testing

### `camutest` Package

A public Go package (`pkg/camutest`) that spins up a complete test environment:

- Starts a MinIO container via testcontainers-go as the S3 backend
- Boots one or more real camu server instances on random ports
- Provides an HTTP test client that makes real HTTP requests
- Handles full teardown after tests

```go
// Local test environment
env := camutest.New(t,
    camutest.WithInstances(3),
    camutest.WithMinIO(),
)
defer env.Cleanup()
client := env.Client()

// Remote deployment
client := camutest.NewClient("https://camu.staging.example.com")
```

### Design Constraints

- All tests make real HTTP requests to real camu server processes — no in-process mocking
- MinIO runs as a real Docker container — no mocked S3
- Tests are self-contained: each creates unique topics, produces its own data, cleans up after itself
- No assumptions about empty state — tests tolerate pre-existing data
- Same test suite works against local test clusters and remote deployments

### Test Categories

| Category | Local | Remote |
|---|---|---|
| Topic CRUD | yes | yes |
| Produce/Consume | yes | yes |
| Consumer groups | yes | yes |
| SSE streaming | yes | yes |
| Multi-instance routing | yes | yes |
| Durability (crash/WAL) | yes | skip |
| Chaos | yes | opt-in |
| Benchmarks | yes | yes |

### CLI

```bash
camu test                                    # all integration tests locally
camu test --category durability              # specific category
camu test --instances 5                      # scale up cluster
camu test --bench                            # benchmarks
camu test --chaos --duration 5m              # chaos tests
camu test --endpoint https://camu.example.com  # against remote
camu test --endpoint https://camu.example.com --auth-token $TOKEN
```

CI compatible via `go test` with build tags. Skips container-based tests gracefully if Docker is unavailable.

## Project Structure

```
camu/
├── cmd/
│   └── camu/
│       └── main.go
├── internal/
│   ├── server/
│   │   ├── server.go
│   │   ├── routes.go
│   │   ├── handlers_topic.go
│   │   ├── handlers_produce.go
│   │   ├── handlers_consume.go
│   │   ├── handlers_group.go
│   │   └── handlers_cluster.go
│   ├── log/
│   │   ├── partition.go
│   │   ├── segment.go
│   │   ├── wal.go
│   │   └── cache.go
│   ├── producer/
│   │   ├── batcher.go
│   │   └── router.go
│   ├── consumer/
│   │   ├── fetcher.go
│   │   ├── sse.go
│   │   └── group.go
│   ├── coordination/
│   │   ├── lease.go
│   │   ├── registry.go
│   │   └── rebalancer.go
│   ├── storage/
│   │   ├── storage.go
│   │   ├── s3.go
│   │   └── offset_store.go
│   ├── config/
│   │   └── config.go
│   └── meta/
│       └── topic.go
├── pkg/
│   └── camutest/
│       ├── env.go
│       ├── client.go
│       └── options.go
├── test/
│   ├── integration/
│   │   ├── topic_test.go
│   │   ├── produce_test.go
│   │   ├── consume_test.go
│   │   ├── group_test.go
│   │   ├── multiinstance_test.go
│   │   ├── durability_test.go
│   │   └── chaos_test.go
│   └── bench/
│       └── throughput_test.go
├── camu.yaml.example
├── go.mod
└── go.sum
```
