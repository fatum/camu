# Camu Architecture

Camu is a Kafka-like commit log that uses S3 as its sole persistent backend. A single binary exposes an HTTP/REST API, writes to a local WAL for fast acks, and flushes segments to S3 in the background. Multiple instances coordinate via S3 conditional writes — no Raft, no ZooKeeper, no external consensus system.

```
                          ┌─────────────────────────────┐
                          │        HTTP Server          │
                          │  /v1/topics/*/messages      │
                          └──────────┬──────────────────┘
                                     │
                  ┌──────────────────┼──────────────────┐
                  │                  │                   │
                  ▼                  ▼                   ▼
         ┌────────────┐    ┌────────────────┐   ┌────────────┐
         │  Producer   │    │   Consumer     │   │  Cluster   │
         │  Router     │    │   Fetcher      │   │  Routing   │
         │  Batcher    │    │   SSE Stream   │   │  Status    │
         └──────┬─────┘    └───────┬────────┘   └────────────┘
                │                  │
         ┌────────────▼────────────┐    ┌───────▼────────┐
         │ Chunked WAL             │    │   Disk Cache   │
         │ active + sealed +       │    │   (LRU, local) │
         │ flushed-retained chunks │    │                │
         └──────┬─────┘    └───────┬────────┘
                │                  │
                │           ┌──────▼──────┐
                │           │ Segment Index │
                │           │ + Range Plan  │
                │           └──────┬──────┘
                └────────┬─────────┘
                         ▼
                ┌────────────────┐
                │       S3       │
                │  (segments,    │
                │   index,       │
                │   coordination)│
                └────────────────┘
```

## Core data model

| Concept   | Description |
|-----------|-------------|
| **Topic** | Named logical stream. Created via API with a partition count and retention duration. |
| **Partition** | Ordered, append-only log within a topic. Unit of parallelism and ownership. Identified by `{topic}/{partition_id}`. |
| **Message** | Key (optional bytes), Value (bytes), Offset (uint64, server-assigned), Timestamp (unix nanos, server-assigned), Headers (map[string]string). |
| **Segment** | Immutable batch of messages stored as a single S3 object. Binary format with optional compression. |
| **Index** | Per-partition JSON file in S3 (`{topic}/{partition}/index.json`) mapping offset ranges to segment keys. |
| **Instance** | A running camu process. Owns a subset of partitions via S3-based leases. |

## S3 object layout

```
{bucket}/
├── _meta/topics/{topic}.json                          # TopicConfig
├── _coordination/
│   ├── leader.json                                    # LeaderLease
│   ├── instances/{instanceID}.json                    # InstanceInfo + heartbeat
│   ├── assignments/{topic}.json                       # partition → instance mapping + version
│   ├── groups/{groupID}/offsets.json                  # consumer group offsets
│   └── consumers/{consumerID}/offsets.json            # standalone consumer offsets
└── {topic}/{partition}/
    ├── index.json                                     # sorted SegmentRef array
    └── {baseOffset}-{epoch}.segment                   # immutable segment blob
```

---

# Write path

```
HTTP POST /v1/topics/{topic}/messages
  │
  ▼
Parse JSON body ──► Route by key ──► Ownership check ──► Assign offsets ──► WAL append + fsync ──► HTTP 200
                                         │                                        │
                                    421 if not                              Batcher tracks
                                    owned                                  buffered size
                                                                                  │
                                                                    ┌─────────────┤
                                                                    ▼             ▼
                                                              Size ≥ 8MB    Timer fires (5s)
                                                                    │             │
                                                                    └──────┬──────┘
                                                                           ▼
                                                                    Flush to S3
```

### 1. Routing

The producer routes each message to a partition:

- **Key present:** `FNV-32a(key) % numPartitions` — deterministic, same key always lands on the same partition.
- **Key absent:** atomic round-robin counter across partitions.

### 2. Ownership check (zero I/O)

Before accepting a write, the server checks a local in-memory map to verify it owns the target partition. If not, it returns `421 Misdirected Request` with the current routing map so the client can retry against the correct instance. This check involves no network calls.

### 3. Offset assignment

Under a partition-level lock, each message gets the next sequential offset and a timestamp (batch of messages shares a single `time.Now()` call). Offsets are uint64, monotonically increasing per partition.

### 4. WAL append

All messages in the batch are serialized into framed WAL entries and appended to a partition-local chunked WAL with a single append critical section. The active chunk rotates when it reaches `wal.chunk_size` (default 64 MB). Each WAL entry is:

```
[4B entry length (uint32 BE)] [message frame bytes] [4B CRC32-IEEE (uint32 BE)]
```

The CRC protects against partial writes from crashes. On replay, an invalid CRC terminates the replay for that chunk — all intact entries before it are recovered.

The HTTP response is sent after fsync completes. At this point the write is durable on local disk — equivalent to Kafka's `acks=1`.

The WAL chunk lifecycle is:

- **active chunk**: receives new appends.
- **sealed chunks**: immutable chunks waiting to be flushed to S3.
- **flushed-retained chunks**: already uploaded and indexed, but kept locally so replicas can still fetch them from the WAL hot path until they catch up.

### 5. Batcher

The batcher does **not** buffer messages in memory. It only tracks byte counts per partition. When a threshold is crossed, it triggers a flush:

| Trigger | Behavior |
|---------|----------|
| Partition buffer ≥ `segments.max_size` (default 8 MB) | Synchronous flush — blocks the append call |
| No writes for `segments.max_age` (default 5s) | Timer fires, async flush in background goroutine |
| Global buffer > `HighWaterMark` (disabled by default) | Returns `ErrBackpressure` → HTTP 503 with `Retry-After: 1` |

### 6. Flush to S3

When the batcher triggers a flush:

1. **Verify ownership from S3** — re-reads the partition assignment to confirm this instance still owns the partition. If ownership changed, the partition is revoked locally and the flush is skipped.
2. **Seal the active chunk** — a fresh active chunk is created for new appends while the sealed chunk joins the flush set.
3. **Replay sealed WAL chunks** — reads sealed chunks that are not yet marked flushed. The WAL remains the source of truth for unflushed data.
4. **Serialize segment** — messages are serialized into the binary segment format and optionally compressed (snappy or zstd).
5. **Upload segment to S3** — key: `{topic}/{partition}/{baseOffset}-{epoch}.segment`. The epoch embedded in the filename prevents stale writes after failover.
6. **Write to disk cache** — the flushed segment is written to the local cache so the owning instance never needs to fetch its own segments back from S3.
7. **Update index (CAS)** — the partition's `index.json` is read from S3 with its ETag, the new `SegmentRef` is appended, and a conditional PUT writes it back. If the ETag doesn't match (another instance modified the index concurrently), the operation retries up to 5 times with a fresh read.
8. **Mark chunks flushed-retained** — sealed chunks below the flushed end are renamed into flushed-retained WAL chunks. They stay readable to followers, but are excluded from unflushed replay on restart.
9. **Prune by follower progress** — old flushed-retained chunks are deleted only after all followers have moved past them.

### Segment binary format

```
[4B magic: 0x43414D55 ("CAMU")]
[1B version: 0x01]
[1B compression: 0=none, 1=snappy, 2=zstd]
[compressed payload]:
  repeated message frames:
    [8B offset (uint64 BE)]
    [8B timestamp (int64 BE)]
    [4B key_len][key bytes]
    [4B value_len][value bytes]
    [4B header_count]
      per header: [4B key_len][key bytes][4B val_len][val bytes]
```

Compression is applied to the entire payload (all frames together), not per-message.

### Message frame in WAL vs segment

The WAL and segment use the same message frame format. The WAL wraps each frame with a length prefix and CRC inside chunk files; segments concatenate frames directly and compress the batch.

---

# Read path

```
HTTP GET /v1/topics/{topic}/partitions/{id}/messages?offset=N&limit=100
  │
  ▼
Cap by readable HW ──► Non-owned partition? ──yes──► RefreshIndex from S3
  │
  ▼
Index.SegmentsFrom(offset)    ◄── binary search on cached segment base offsets
  │
  ▼
Fetch up to 4 segment blobs in parallel (cache or S3)
  │
  ▼
Decode segments in offset order ──► owned/readable suffix? ──yes──► Read WAL chunks
  │                                                        │
  └────────────────────────────────────────────────────────┴──► Merge by offset, WAL wins on overlap
```

### 1. Index refresh

Any instance can serve reads for any partition. For non-owned partitions, the handler fetches the latest `index.json` from S3 before looking up segments, so the reader sees segments flushed by the current owner. Owned partitions, and followers with a leader-advertised readable high watermark, can also overlay a readable local WAL suffix on top of segment-backed data. Non-owned non-replica reads still only see flushed segment data.

### 2. Index lookup

The index is a sorted array of `SegmentRef` entries. Lookup uses `sort.Search` (binary search) to find the segment whose `[BaseOffset, EndOffset]` range contains the requested offset. Time complexity: O(log N) where N is the number of segments.

```go
type SegmentRef struct {
    BaseOffset uint64
    EndOffset  uint64
    Epoch      uint64
    Key        string    // S3 key
    CreatedAt  time.Time
}
```

### 3. Disk cache

A flat-file LRU cache on local disk. Segment S3 keys are SHA256-hashed to produce filenames, avoiding path separator issues.

- **Get:** O(1) index lookup + MRU promotion (move-to-front in doubly-linked list). If the file is missing from disk despite being tracked, the entry is cleaned up and treated as a miss.
- **Put:** Write file, push to front of LRU list. If total cache size exceeds `cache.max_size` (default 10 GB), evict entries from the back (least recently used) until under budget.
- **No TTL-based expiration.** Segments are immutable once flushed, so cached copies never become stale. Only size-based LRU eviction.

### 4. Segment fetch

The fetcher plans an ordered segment window from the in-memory index and fetches up to 4 segment blobs in parallel. Each blob is read from the disk cache when possible; on a cache miss it is downloaded from S3 and written back to the disk cache. If a later segment fetch fails after earlier segments already produced messages, a partial result is returned.

### 5. Segment parsing

The segment header is validated (magic number, version), the payload is decompressed if needed, and message frames are read sequentially. Messages with offset < startOffset are skipped. Up to `limit` messages are returned.

### 6. Multi-segment reads

If the requested range spans multiple segments, the fetcher plans a bounded ordered segment batch, fetches the blobs in parallel, then parses them sequentially in offset order. That preserves deterministic ordering while reducing latency for cold multi-segment reads.

### 7. WAL overlay and merge

If the local partition state has a readable high watermark, the server reads matching messages from local WAL chunks starting at `startOffset`, filters them to `< highWatermark`, and merges them with the segment-backed messages by offset. WAL reads can span both unflushed chunks and flushed-retained chunks, so promoted leaders and followers can serve recent committed data without waiting for segment-only catch-up. On overlap, WAL messages win so promoted leaders and followers do not return stale segment copies over newer readable WAL data.

### Response format

```json
{
  "messages": [
    {
      "offset": 0,
      "timestamp": 1234567890123456789,
      "key": "user-123",
      "value": "event payload",
      "headers": {"trace-id": "abc"}
    }
  ],
  "next_offset": 1
}
```

Non-UTF-8 values are base64-encoded.

### SSE streaming

```
GET /v1/topics/{topic}/partitions/{id}/stream?offset=N
Accept: text/event-stream
```

Uses the same fetcher in a loop. When no messages are available, backs off 100ms before retrying. Events are formatted as:

```
id: {offset}
data: {"offset": 123, "key": "...", "value": "..."}

```

Reconnection uses the `Last-Event-ID` header — the server resumes from `Last-Event-ID + 1`.

---

# Coordination

Camu uses **S3 conditional writes** (ETag-based CAS) for all coordination. There is no Raft, Paxos, or external consensus system.

## Leader election

Any instance can attempt to acquire leadership by writing to `_coordination/leader.json` with a conditional PUT:

```go
type LeaderLease struct {
    InstanceID string
    ExpiresAt  time.Time
    ETag       string  // for CAS
}
```

- **Production defaults:** TTL `30s`, renewal interval `10s`
- **Jepsen / test defaults:** TTL `10s`, renewal interval `3s`
- If the lease is expired or missing, any instance can claim it
- S3's atomic conditional write ensures at most one instance succeeds

The leader's only special responsibility is computing and publishing partition assignments. All other operations (produce, consume) work identically on every instance.

## Instance discovery

Each instance registers at `_coordination/instances/{instanceID}.json` with its address and heartbeat timestamp. The registry lists all keys under the prefix and filters by freshness (heartbeat within 90 seconds). Instances re-register every 10 seconds as part of the lease renewal loop.

## Partition assignment

The leader computes assignments using deterministic round-robin:

1. Sort active instance IDs alphabetically
2. For partition `p`: assign to `instances[p % len(instances)]`
3. Write to `_coordination/assignments/{topic}.json` with CAS

The assignment `Version` field is incremented on each write. This version number doubles as the **epoch** for lease fencing.

All instances read their assignments from S3 every renewal cycle and update their local `myPartitions` map.

## Epoch fencing

The epoch prevents stale writes after partition reassignment:

| Check point | Mechanism |
|-------------|-----------|
| **Every produce request** | Local ownership check (zero I/O) — reject with 421 if not owned |
| **Every flush** | Re-read assignment from S3 — skip flush and revoke partition if ownership changed |
| **Startup WAL recovery** | Compare WAL epoch sidecar with lease epoch — discard entire WAL if epoch advanced (another instance already took over) |
| **Segment filename** | Embeds epoch (`{offset}-{epoch}.segment`) — makes stale uploads distinguishable |
| **Index update** | Conditional PUT with ETag — two instances cannot both update the same index concurrently |

## Rebalancing

Happens automatically every ~10 seconds when the leader detects a topology change:

1. Leader heartbeats its own registry entry
2. Leader reads active instances (filters by 90s TTL)
3. If the set changed, leader computes new round-robin assignment and publishes via CAS
4. All instances pick up new assignments on their next renewal cycle

**Convergence time:** depends on lease timing. In production defaults it is materially slower than in tests; in Jepsen and `pkg/camutest` we intentionally use shorter lease timing to make failover observable in bounded test windows.

## Replication notes

For replicated topics, the important durability boundary is the partition high watermark, not just S3 flush completion:

- leaders advance HW only after enough replicas confirm the append
- flush persists the current HW into `index.json`
- WAL truncation is capped by HW, so uncommitted tail entries stay available for follower fetch
- on leader reassignment, the new leader recovers from local WAL and persisted/index state before serving

One subtle bug fixed on March 22, 2026: after leader reassignment, the first flush could write a replacement tail segment that overlapped an older tail segment already present in `index.json`. Readers could then binary-search to the stale segment and return `200 []` even though the new leader had already replicated and flushed the newer suffix. The index layer now replaces overlapping segment refs instead of keeping both.

## Jepsen status

The repository includes a Jepsen harness under [`jepsen/camu`](../jepsen/camu/README.md). As of March 23, 2026, the checked-in code passes 21 fault scenarios across replicated (`rf=3/minISR=2`), strict-quorum (`rf=3/minISR=3`), and control (`rf=1/minISR=1`) modes. See [`docs/reliability.md`](reliability.md) for the full verified matrix.

That should be read as durability evidence, not a blanket availability claim. Hard fault windows still show substantial temporary request failure.

## Graceful shutdown

```
1. Set shuttingDown flag → produce handlers return 503
2. Drain in-flight HTTP requests
3. Batcher flushes all remaining WAL data to S3
4. Stop lease renewal goroutine
5. Deregister from instance registry
```

---

# Trade-offs

| Decision | What you get | What you give up |
|----------|-------------|-----------------|
| **S3 as sole persistent store** | Unlimited capacity, 11 nines durability, no replication to manage, pay-per-use, scales to zero | ~5s visibility delay for cross-instance reads (flush interval). All coordination depends on S3 availability. |
| **Local WAL for acks** | Fast writes (~30µs single, ~11µs/msg batched). Crash-safe on local disk. | In `rf=1`, unflushed data can still be lost on node failure. Replicated topics reduce this risk, but availability and failover timing still matter. |
| **HTTP/REST API** | Any language, no client library needed, curl-debuggable, human-readable. Load balancers and proxies work out of the box. | Higher per-message overhead vs binary protocols (Kafka's TCP, Pulsar's binary). JSON serialization cost on both ends. |
| **S3-based coordination instead of Raft** | No separate consensus cluster. No quorum requirements. No split-brain from consensus bugs. Single dependency (S3). | Lease TTL sets the floor for failure detection. S3 conditional writes have higher latency than in-memory consensus. Cannot coordinate faster than S3 round-trip time. |
| **Stateless instances** | Any instance can be killed and replaced. Scale to zero. No cluster state to corrupt. Recovery = read from S3. | Rebalancing speed depends on lease timing and is still a meaningful availability limiter. |
| **No in-memory message buffer** | WAL is the single source of truth for unflushed data. No memory bloat. Simple code. | Every read goes through disk cache or S3. No sub-millisecond tail reads from memory. |
| **Deterministic round-robin assignment** | Simple, predictable. No coordination overhead for assignment decisions. | No load-aware balancing. A hot partition gets the same resources as a cold one. |
| **Immutable segments** | No compaction complexity. Cache never needs invalidation. Simple garbage collection. | Cannot update or delete individual messages. Retention is segment-granular. |
| **Per-partition WAL files** | Flush and truncation are independent per partition. No cross-partition contention. | Many open file descriptors with high partition counts. Directory can get large. |

### When Camu fits well

- You want a durable message queue without operating Kafka/ZooKeeper/KRaft
- Your visibility delay tolerance is ≥ 5 seconds
- You already have S3 (or MinIO/R2) in your infrastructure
- You want to scale to zero when idle
- Your consumers are HTTP-native (webhooks, serverless functions, browsers via SSE)

### When Camu does not fit

- You need sub-second end-to-end latency
- You need `acks=all` durability guarantees (no data loss on any single node failure)
- You need very high per-partition throughput (HTTP/JSON overhead, per-partition mutex for offset assignment, and per-batch fsync cap single-partition throughput well below what Kafka achieves with its binary protocol, zero-copy reads, and client-side batching)
- You need transactions or exactly-once semantics
- You need log compaction (keep latest value per key)

---

# HTTP API reference

### Topics

```
POST   /v1/topics                                    Create topic
GET    /v1/topics                                    List topics
GET    /v1/topics/{topic}                            Get topic config
DELETE /v1/topics/{topic}                            Delete topic
```

### Producing

```
POST   /v1/topics/{topic}/messages                   Produce (routed by key)
POST   /v1/topics/{topic}/partitions/{id}/messages   Produce to specific partition
```

Request body (single or array):
```json
{"key": "user-123", "value": "event", "headers": {"trace": "abc"}}
```

Response:
```json
{"offsets": [{"partition": 0, "offset": 100}]}
```

### Consuming

```
GET    /v1/topics/{topic}/partitions/{id}/messages?offset=0&limit=100   Poll
GET    /v1/topics/{topic}/partitions/{id}/stream?offset=0               SSE stream
```

### Offset checkpoints

```
POST   /v1/groups/{group_id}/commit                  Commit group offsets
GET    /v1/groups/{group_id}/offsets                  Get group offsets
POST   /v1/topics/{topic}/offsets/{consumer_id}       Commit consumer offsets
GET    /v1/topics/{topic}/offsets/{consumer_id}        Get consumer offsets
```

### Cluster

```
GET    /v1/topics/{topic}/routing                    Partition → instance mapping
GET    /v1/cluster/status                            Instance list and health
```

### Status codes

| Code | Meaning |
|------|---------|
| 200  | Success |
| 400  | Bad request (invalid JSON, missing fields) |
| 404  | Topic or partition not found |
| 409  | Topic already exists |
| 421  | Misdirected — partition owned by a different instance. Response body contains routing map. |
| 503  | Backpressure or shutting down. Check `Retry-After` header. |

Every response includes `X-Camu-Instance-ID` header.

---

# Configuration

Single YAML file or environment variables (env overrides YAML):

```yaml
server:
  address: ":8080"
  instance_id: ""              # auto-generated UUID if empty

storage:
  bucket: "camu-data"
  region: "us-east-1"
  endpoint: ""                 # empty for AWS, set for MinIO/R2
  credentials:
    access_key: ""
    secret_key: ""

wal:
  directory: "/var/lib/camu/wal"
  fsync: true
  chunk_size: 67108864

segments:
  max_size: 8388608            # 8 MB — flush when partition buffer exceeds this
  max_age: "5s"                # flush even if not full
  compression: "none"          # none, snappy, zstd

cache:
  directory: "/var/lib/camu/cache"
  max_size: 10737418240        # 10 GB — LRU eviction above this

coordination:
  lease_ttl: "30s"
  instance_ttl: "90s"
  heartbeat_interval: "10s"
  rebalance_delay: "5s"
```

Environment variable format: `CAMU_SECTION_KEY` (e.g., `CAMU_STORAGE_BUCKET`, `CAMU_WAL_FSYNC`).
