# Camu Architecture

Camu is an object-storage-backed commit log. It acknowledges writes from a local WAL, flushes immutable segments to S3, and uses S3 conditional writes for leader election, partition assignment, and fencing.

## High-Level Shape

```text
producer
  -> public HTTP API (:8080 by default)
  -> partition router
  -> ownership / leadership checks
  -> WAL append + fsync
  -> replication wait (for replicated topics)
  -> HTTP response

background flush
  -> seal WAL chunks
  -> build segment + sidecars
  -> verify ownership from S3
  -> upload to object storage
  -> CAS-update partition index
  -> retain or prune flushed WAL chunks

consumer
  -> segment index lookup
  -> local cache or S3 fetch
  -> WAL overlay when readable committed suffix exists locally
  -> JSON poll or SSE stream response
```

## Core Objects

| Object | Stored where | Purpose |
|--------|--------------|---------|
| Topic config | `_meta/topics/{topic}.json` | Partition count, retention, replication settings |
| Assignment | `_coordination/assignments/{topic}.json` | Owner, replicas, epoch/version |
| Instance heartbeat | `_coordination/instances/{instanceID}.json` | Liveness and routable addresses |
| Segment | `{topic}/{partition}/{baseOffset}-{epoch}.segment` | Immutable flushed log data |
| Offset index | `{topic}/{partition}/{baseOffset}-{epoch}.offset.idx` | Sparse seek index inside a segment |
| Segment metadata | `{topic}/{partition}/{baseOffset}-{epoch}.meta.json` | Record count, size, compression |
| Partition index | `{topic}/{partition}/index.json` | Ordered list of segment refs and HW |
| WAL | local disk | Fast ack path and recovery source |

## Request Paths

### Produce

1. Parse the request body as a JSON array, or as an idempotent batch on the partition-specific endpoint.
2. Route messages by key with FNV-32a; keyless messages use round-robin.
3. Check local partition ownership.
4. For replicated topics, confirm the node is still the leader for that partition.
5. Append to the partition WAL and fsync.
6. If replicated, wait until the partition high watermark reaches the appended offset.
7. Return assigned offsets.

Important semantics:

- `rf=1`: ack means local-WAL durability only
- `rf>1`: ack means leader WAL durability plus ISR quorum confirmation
- segment flush is not on the critical path for produce
- backpressure returns `503` with `Retry-After: 1`
- replication wait timeout returns `408`

### Leader Proxying And `421`

If a request reaches a node that does not own the destination partition, Camu tries to proxy the request over the internal h2c listener to the current leader. If that path is unavailable, it returns `421 Misdirected Request` with a routing map so the client can retry directly.

This keeps high-level produce usable behind a load balancer while still fencing stale owners.

### Consume

1. Parse `offset` and `limit`.
2. Refresh the segment index from S3 for non-owned partitions.
3. Cap reads at the readable high watermark when one is available.
4. Locate candidate segments with a binary search over the partition index.
5. Fetch segment blobs from local cache or S3.
6. Use the sparse `.offset.idx` sidecar to seek near the requested offset.
7. Overlay a readable local WAL suffix when the node has committed but not yet flushed data.
8. Return JSON or stream SSE events.

The key read-path distinction is:

- any node can serve flushed segment data
- owners and caught-up replicas can also serve committed WAL-backed suffixes
- non-owned, non-replica reads are segment-backed only

## WAL Lifecycle

Each partition has its own WAL. Entries are length-prefixed frames with a CRC so crash recovery can stop cleanly at the first partial write.

WAL chunks move through three states:

- active: accepts new writes
- sealed: waiting to flush
- flushed-retained: already materialized to S3 but kept locally for follower fetch and readable committed suffixes

WAL truncation is follower-aware. Old flushed chunks are only removed after the system determines replicas no longer need them.

## Flush Path

Flush is triggered when either:

- buffered bytes for a partition exceed `segments.max_size`
- the partition has been idle for `segments.max_age`

Flush sequence:

1. Re-read partition ownership from S3.
2. Seal the active WAL chunk.
3. Replay unflushed WAL entries from sealed chunks.
4. Serialize a segment with optional batch-level compression.
5. Build `.offset.idx` and `.meta.json`.
6. Upload segment and sidecars.
7. Write the segment into the local disk cache.
8. CAS-update `index.json`.
9. Mark source WAL chunks as flushed-retained.

If ownership changed before flush, the partition is revoked locally and the flush is skipped. That is one of the core stale-writer fences.

## Segment Format

Segments are immutable and seekable. Compression is applied per batch, not to the whole segment, so readers can seek to a batch boundary and decode only the needed suffix.

```text
[4B magic "CAMU"]
[1B version]
[1B compression]
repeated:
  [4B batch_len]
  [4B message_count]
  [batch payload]
```

Each message frame stores:

- offset
- timestamp
- key
- value
- headers
- optional producer metadata in WAL-derived batches

## Replication

Replicated topics use an ISR model.

- leaders accept writes
- followers fetch leader data over the internal h2c endpoint
- the partition high watermark advances after enough replicas confirm the append
- producers wait on that high watermark when `replication_factor > 1`

Two boundaries matter:

- acknowledged boundary: what the client can rely on after a successful produce
- flushed boundary: what a cold reader can fetch from S3-only state

The architecture deliberately keeps those separate.

## Coordination

Camu uses S3 conditional writes instead of a consensus cluster.

### Leader Election

The cluster leader owns assignment publication, not all data-plane traffic. It renews `_coordination/leader.json` with CAS.

Default timing:

- `coordination.lease_ttl = 30s`
- `coordination.heartbeat_interval = 10s`
- `coordination.instance_ttl = lease_ttl * 3` when omitted

Test and Jepsen configurations use shorter timing to make failover observable within small runs.

### Partition Assignment

Assignments are deterministic round-robin over active instances. The assignment version doubles as the leader epoch used in filenames and stale-writer fencing.

### Fencing

Stale ownership is blocked by:

- local ownership checks on every produce
- leader verification before replicated writes
- S3 ownership re-check before every flush
- epoch-tagged segment filenames
- CAS updates on `index.json`
- WAL recovery that discards old-epoch local state when a newer owner has already taken over

## Operational Notes

### What `GET /v1/cluster/status` Means

This endpoint currently reports the serving node's local identity and public address. It is a lightweight status endpoint, not a full cluster-membership dump.

### What `GET /v1/ready` Means

Readiness only turns true after the node has completed initial coordination and partition initialization. During shutdown it returns `503` with status `shutting_down`.

### Why Cross-Instance Reads Lag Flush

Camu does not coordinate every read through the leader. That keeps the read path simple, but it means a random node can only serve what is already in the segment index unless it is the owner or a replica with readable local state. In practice, that makes `segments.max_age` and flush cadence directly visible in stale-read behavior.

## Trade-Offs

| Decision | Benefit | Cost |
|----------|---------|------|
| S3 as persistent store | Durable, cheap, scale-to-zero storage model | Read visibility and coordination latency depend on object-store timing |
| Local WAL ack path | Fast durable writes without waiting for flush | `rf=1` can still lose unflushed data on node loss |
| HTTP/JSON API | Easy integration, curl-debuggable, load-balancer-friendly | Higher overhead than binary protocols |
| No external consensus system | One fewer cluster to run | Failover speed is bounded by lease timing and S3 round trips |
| Immutable segments | Simple cache and retention behavior | No compaction or in-place mutation |
| Deterministic assignment | Predictable and simple | Not load-aware |

## Current Evidence

The current architecture is backed by unit tests, integration tests, and the Jepsen harness under [jepsen/camu/README.md](../jepsen/camu/README.md). The verified fault matrix is summarized in [docs/reliability.md](reliability.md).
