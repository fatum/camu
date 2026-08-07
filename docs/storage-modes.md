# Storage Modes

Camu offers two storage modes — `classic` and `diskless` — exposing identical
HTTP, SSE, and Kafka APIs. The difference is in the data path: how records are
durably stored and what infrastructure each mode requires.

## Classic

**How it works.** The partition leader appends raw `RecordBatch` bytes to a
local active segment on disk. A background flusher seals active segments at
`segments.max_size` or `segments.max_age` and publishes them as immutable sealed
segments to object storage. Produce never waits for an S3 upload. Replication
(ISR quorum) is available for `rf > 1`.

```
Produce → append to local active segment → replicate (if rf>1) → ack
                                                        ↓
                                          background flush to S3 (off hot path)
```

**Configuration.**

| Key | Default | Purpose |
|-----|---------|---------|
| `segments.max_size` | 64 MiB | Seal and flush when the active segment reaches this size. |
| `segments.max_age` | 1 m | Seal and flush after this duration, even if below max_size. |

**Throughput characteristics.**

- **Write latency:** sub-millisecond produce ack (local disk append). For `rf >
  1`, ack latency includes ISR replication.
- **Write throughput:** bounded by local disk I/O; not bottlenecked by object
  store latency because flush is asynchronous.
- **Read latency:** hot data served from memory (in-memory active segment index)
  or local disk (sealed segment cache). Cold reads go to S3.
- **Backpressure:** in-flight flush bytes count toward backpressure. If object
  storage is slow, produce is rejected rather than allowing unbounded local
  growth or blocking on upload.

**Pros.**

- Lowest produce latency — local disk write, no S3 round-trip on the hot path.
- Replication (ISR quorum) with acknowledged durability per the Jepsen matrix.
- Read path serves hot data from in-memory and local-disk indexes without S3
  round-trips.
- Flush is decoupled from produce so object-store latency only gates
  backpressure, not every ack.

**Cons.**

- Requires local disk per partition (one active segment per partition owned).
- Disk provisioning tied to partition count and segment size.
- Node failure loses unflushed active segment data (acks bound this window).
- Disk space, not just object storage, must be managed per node.

## Diskless

**How it works.** Records are buffered in memory and flushed to S3 as a single
object. After the upload succeeds, the metastore atomically assigns logical
offsets and publishes segment references. It is deliberately the inverse of
classic: upload-then-allocate. An upload failure leaves an orphan S3 object, never
a visible offset hole. Produce blocks until the flush commits.

Background compaction merges small flushed segments into `target_bytes`-sized
objects (default 64 MiB), keeping reads efficient as history grows. The S3
metastore uses a bounded hot-head window: old compaction-sized refs are archived
so the head object stays small regardless of history and commits cost O(window)
rather than O(all refs).

```
Produce → buffer (linger + max_batch_bytes) → upload S3 → commit offsets in metastore → ack
                                                              ↓
                                              publish segment refs, advance committed head
                                              → background compaction merges small segments
```

**Configuration.**

| Key | Default | Purpose |
|-----|---------|---------|
| `diskless.linger_ms` | 250 | Max buffer time before flush. |
| `diskless.max_batch_bytes` | 8 MiB | Max buffer size before flush. |
| `diskless.metastore` | `memory` | Offset/segment backend: `memory`, `s3`, or `dynamodb`. |
| `diskless.dynamodb.table_prefix` | `camu` | DynamoDB table name prefix. |
| `diskless.dynamodb.region` | — | AWS region for DynamoDB. |
| `diskless.dynamodb.endpoint` | — | Override endpoint (e.g. DynamoDB Local). |
| `diskless.compaction.enabled` | `false` | Enable background small-segment compaction. |
| `diskless.compaction.target_bytes` | 64 MiB | Approximate merged segment size. |
| `diskless.compaction.min_segments` | 4 | Merge only when at least this many refs are eligible. |
| `diskless.compaction.grace` | 60 s | Minimum age of a ref before it is eligible for compaction. |
| `diskless.compaction.interval` | 2 s | How often compaction work is driven. |

### Metastore backends

The metastore is the coordination layer for diskless topics — it assigns
offsets, publishes segment refs, tracks the committed head, and drives
compaction. The choice of backend directly affects latency, throughput, and
operational complexity.

**`memory` (default).** All state lives in process. Offsets are a monotonic
counter; segment refs and the committed head are in-memory maps. Reads and
writes have no network round-trip, so ack latency is dominated by the S3 upload
alone. Use only for single-node development — a restart loses all offset and
segment metadata, and two instances will diverge immediately.

**`s3`.** Offsets, segment refs, and checkpoints are stored in the same S3
bucket as topic data. Each partition has a bounded *head object*: `_diskless/`
subdirectory per partition. Offset allocation and segment-ref publishes are
atomic S3 conditional writes (CAS) against the head. A background archive job
rolls old compaction-sized refs into immutable checkpoint objects so the head
stays small regardless of history — commits cost O(window), not O(all refs).
Reads query only the head; reads of older data walk the archive checkpoint
chain. No additional infrastructure required.

- **Latency:** one extra S3 conditional PUT per commit (after the data upload,
  but the head read for offset allocation is batched into the same commit call).
  Adds ~1 S3 round-trip to the flush cycle.
- **Throughput:** CAS contention on the head object serializes commits within a
  partition. Multiple partitions and concurrent flushes are not contended.
- **Durability:** shared with the data bucket; S3 consistency model applies.

**`dynamodb`.** Offset allocation and segment publishing are atomic DynamoDB
transactions. Producer idempotency is enforced by conditional writes in the same
transaction that assigns the offset. DynamoDB provides point-in-time recovery
and predictable single-digit-millisecond latencies regardless of object size,
unlike S3 head reads which grow with the object. Configurable read/write
capacity units give deterministic throughput.

- **Latency:** a DynamoDB `TransactWriteItems` call per commit (~single-digit ms
  in-region). Adds one DynamoDB round-trip to the flush cycle, after the S3
  upload.
- **Throughput:** bounded by provisioned WCUs. A transaction consumes 2 WCUs per
  item plus conditional-check overhead. `maxBatchesPerCommit` (25) caps the
  items per transaction, so sustained throughput depends on WCU provisioning.
- **Durability:** DynamoDB tables are replicated across AZs by default.
- **Cost:** additional AWS service. `s3` metastore has no per-operation cost
  beyond the S3 PUTs themselves.

| Backend | Commit latency source | Infrastructure | Use case |
|---------|----------------------|----------------|----------|
| `memory` | None (in-process counter) | None | Single-node dev only |
| `s3` | S3 conditional PUT | Same S3 bucket | Multi-node, same bucket, no extra services |
| `dynamodb` | DynamoDB transaction | DynamoDB tables | Predictable latency, high-throughput production |

**Throughput characteristics.**

- **Write latency:** at minimum one S3 round-trip per flush batch (upload + commit).
  In practice a single produce blocks for `linger_ms` of batching + upload +
  commit. With S3 metastore, commit is a conditional head-object write (~1 S3
  PUT). With DynamoDB, commit is an atomic conditional transaction.
- **Write throughput:** throttled by S3 PUT latency and metastore commit
  latency. Multiple flushes can upload concurrently (4 concurrent flush slots by
  default); commits within a partition are serialized in submission order.
  Throughput improves with larger batch sizes (fewer, bigger flushes).
- **Read latency:** every read is an S3 `GetRange` request. Recent data is in
  small flushes; compacted data is in larger merged objects. Reads walk segment
  refs from the metastore, then fetch byte ranges from S3.
- **Backpressure:** produce is synchronous with the flush cycle. If the object
  store is slow, every produce ack is delayed.

**Pros.**

- No local disk required. Runs on any node with no storage provisioning.
- Shared durable state in object storage: any node can serve any partition by
  reading from S3 and the metastore.
- Upload-then-allocate prevents offset holes from failed uploads.
- Compaction bounds read overhead as history grows.
- Three metastore backends to match your infrastructure: `memory` for dev, `s3`
  for zero-additional-services multi-node, `dynamodb` for predictable
  single-digit-ms commit latency at production throughput.
- Works behind any S3-compatible store (MinIO, R2, B2, etc.).

**Cons.**

- Higher produce latency than classic: at minimum one S3 round-trip per batch
  (upload + commit). With `s3` metastore, commit adds a second S3 round-trip.
  With `dynamodb`, commit is a separate DynamoDB call.
- No replication (writes go to S3 + metastore directly; durability relies on S3
  and the metastore backend, not ISR quorums).
- Linger buffering adds base latency to every produce (default 250 ms).
- Read path always hits S3; no in-memory or local-disk read cache.
- `memory` metastore is not durable across restarts; `s3` metastore commit
  latency scales with head-object size (bounded by archiving); `dynamodb`
  requires provisioning and additional cost.

## Choosing a mode

| Concern | Classic | Diskless |
|---------|---------|---------|
| Produce latency | Sub-millisecond (local disk) | S3 round-trip per batch (~tens of ms) |
| Local disk | Required | Not required |
| Replication (ISR) | Supported | Not supported |
| Object-store dependency | Async flush only | Synchronous produce path |
| Read latency (hot data) | In-memory / local disk | S3 |
| Operational simplicity | Disk provisioning per node | Stateless nodes |
| Throughput ceiling | Local disk bandwidth | S3 + metastore throughput |
| Durability model | ISR quorum | S3 + metastore atomic commit |
| Metastore | None (indexes in memory + disk) | `memory` / `s3` / `dynamodb` (controls commit latency) |
| Jepsen coverage | Full matrix | Not replicated; S3-metastore correctness covered by integration tests |

**Use classic when** you need the lowest produce latency, ISR-quorum durability,
and high-throughput sustained writes — and you are willing to provision local
disks.

**Use diskless when** you want stateless nodes, no disk fleet, and simpler
operations — and can accept S3-latency produce ack times with linger batching
overhead.

**Diskless is not replicated.** Topics created with `storage_mode: diskless`
ignore `replication_factor` and `min_insync_replicas`. Each produce is durable
once the metastore commit succeeds; there is no second copy, no ISR tracking,
and no follower fetch path for diskless partitions.

## Configuration reference

```yaml
# Classic (default)
segments:
max_size: 67108864   # 64 MiB
    max_age: "1m"

# Diskless
diskless:
  linger_ms: 250            # batch window
  max_batch_bytes: 8388608  # 8 MiB
  metastore: "memory"       # memory | s3 | dynamodb
  compaction:
    enabled: false
    target_bytes: 67108864  # 64 MiB
    min_segments: 4
    grace: "60s"
    interval: "2s"
```

The storage mode is set at topic creation and cannot be changed afterward.
Attempting to mutate `camu.storage.mode` via `AlterConfigs` is rejected.
