# Camu Architecture

Camu is an S3-native event log that stores native Kafka `RecordBatch` bytes as its canonical log format and coordinates multi-instance ownership through object storage conditional writes.

## System Overview

```
  Producers                          Consumers
  (HTTP / Kafka)                     (HTTP / Kafka / SSE)
       |                                  ^
       v                                  |
  +--------------------------------------------+
  |              Camu Instance                  |
  |                                            |
  |  +-----------+  +----------+  +---------+  |
  |  | Produce   |  | Consume  |  | Kafka   |  |
  |  | Handlers  |  | Handlers |  | Server  |  |
  |  +-----+-----+  +----+-----+  +----+----+  |
  |        |              |             |       |
  |  +-----v--------------v-------------v----+  |
  |  |         Partition Manager              |  |
  |  |  (active segments, flush, recovery)    |  |
  |  +-----+------------------+--------------+  |
  |        |                  |                 |
  |  +-----v------+    +-----v-----------+     |
  |  | Replication |    | Coordination    |     |
  |  | (ISR fetch) |    | (S3 leases)     |     |
  |  +-----+------+    +-----+-----------+     |
  +--------|------------------|----------------+
           |                  |
     +-----v------------------v-----+
     |      Object Storage          |
     |  (S3 / MinIO / R2 / B2)     |
     +------------------------------+
```

## Data Path

### Write Path

1. **Ingest**: Producers send HTTP JSON or Kafka wire protocol requests.
2. **Encode**: HTTP writes are translated into Kafka `RecordBatch` bytes at the API boundary. Kafka protocol writes pass through as zero-copy raw batches.
3. **Append**: The partition leader appends batch bytes to a local active segment.
4. **Replicate**: For replicated topics, followers fetch raw batches from the leader over the internal h2c listener. The leader advances the high watermark when ISR followers confirm progress.
5. **Acknowledge**: The produce response is sent only after the ISR quorum has confirmed the write.
6. **Flush**: The batcher seals active segments and uploads immutable segment files plus sidecars to object storage.

### Read Path

1. **Route**: The consume request resolves to a partition and starting offset.
2. **Merge**: The streaming iterator merges records from sealed segments (S3) and the in-memory active segment, deduplicating by offset.
3. **Cap**: Reads are capped by the readable high watermark when replication is enabled, ensuring consumers never see uncommitted data.
4. **Encode**: Kafka fetch returns raw RecordBatch bytes. HTTP consume decodes batches into JSON at read time.

## Server Module Structure

The HTTP and Kafka server layer is organized by concern:

```
internal/server/
  Produce pipeline
  ├── handlers_produce.go      High-level + partition-specific HTTP handlers
  ├── produce_types.go         Request/response DTOs
  ├── produce_parse.go         Body parsing (JSON array vs idempotent batch)
  ├── produce_append.go        RecordBatch append fast-path + replication wait
  └── produce_leadership.go    Leadership proxy/reject helpers

  Consume pipeline
  ├── handlers_consume.go      Polling + SSE streaming HTTP handlers
  ├── consume_types.go         Response DTOs
  ├── consume_iterator.go      Multi-source merge iterator (sealed + active)
  ├── consume_stream.go        Streaming JSON writer with offset merge
  └── consume_helpers.go       High watermark, encoding, message utilities

  Kafka wire protocol
  ├── kafka_types.go               Server struct, config, interfaces, error codes
  ├── kafka_wire.go                Connection handling, request routing, codec framing
  ├── kafka_handlers_data.go       Produce + Fetch handlers
  ├── kafka_metadata_discovery.go  Metadata, ListOffsets, FindCoordinator
  ├── kafka_topic_admin.go         Topic admin and topic-config handling
  ├── kafka_acl_admin.go           ACL admin handlers and filter validation
  ├── kafka_offsets.go             OffsetCommit, OffsetFetch, OffsetDelete
  ├── kafka_group_handlers.go      Group handler wrappers
  ├── kafka_groups.go              Consumer group coordinator (S3-backed CAS)
  ├── kafka_codec.go               RecordBatch encode/decode, compression (gzip/snappy/lz4/zstd)
  └── kafka_helpers.go             Partition lookup, error mapping, adapters

  Background cleanup
  ├── topic_deletion.go            Coordination-leader topic deletion workflow
  ├── coordination_leader_service.go Controller-only cleanup/orchestration service
  ├── partition_leader_service.go  Partition-leader maintenance service
  ├── partition_follower_service.go Follower proxying and failover service
  ├── partition_jobs.go            Durable partition maintenance job model
  ├── partition_job_executor.go    Compatibility wrappers for owner maintenance entrypoints
  ├── partition_job_retention.go   Partition-leader retention jobs
  ├── partition_job_merge.go       Classic sealed-segment merge builder, discovery, and executor
  └── coordination_gc.go           Coordination GC, ISR cleanup, topic delete
```

## Stored Objects

| Object | Location | Purpose |
|--------|----------|---------|
| Topic config | `_meta/topics/{topic}.json` | Retention, partitions, replication settings |
| Assignments | `_coordination/assignments/{topic}.json` | Partition leaders and replicas |
| Instance heartbeat | `_coordination/instances/{instanceID}.json` | Liveness and routable addresses |
| Cluster leader lease | `_coordination/leader.json` | Controller ownership |
| ISR state | `_coordination/isr/{topic}/{partition}.json` | ISR membership and high watermark |
| Epoch history | `_coordination/epochs/{topic}/{partition}.json` | Divergence detection and fencing |
| Sealed segment | `segments/{topic}/{partition}/{baseOffset}.log` | Immutable Kafka batch data |
| Segment sidecar | `segments/{topic}/{partition}/{baseOffset}.index` | Offset and timestamp indexes |
| Partition state | `_meta/state/{topic}/{partition}.json` | High watermark and epoch history |
| Producer checkpoint | `{topic}/{partition}/producers.checkpoint` | Idempotent producer recovery |
| Consumer group state | `_coordination/kafka-groups/{group}.json` | Kafka consumer group coordination |
| Topic deletion marker | `_coordination/topic_deletions/{topic}.json` | Async/resumable topic deletion state |
| Partition job | `_coordination/partition_jobs/{topic}/{partition}/{job}.json` | Durable partition-leader maintenance work |

## Local State

Each partition keeps:

- one active segment on local disk
- in-memory offset and timestamp indexes for the active segment
- per-partition producer-sequence state for idempotent produce
- replication state when the topic uses `replication_factor > 1`

The local active segment is the only mutable log file. Recovery truncates partial tail data by scanning RecordBatch boundaries and CRCs.

## Replication

- Leaders append raw RecordBatch bytes locally
- Followers fetch raw RecordBatch bytes from the leader over h2c
- Followers append those bytes without re-encoding (zero-copy replication)
- The leader advances the high watermark when ISR followers confirm progress
- Produce responses wait in a purgatory until the high watermark passes the written offset

## Flush

Flush is triggered by `segments.max_size` or `segments.max_age`.

Flush steps:

1. Verify ownership from S3 (fences stale leaders)
2. Seal the active segment
3. Upload the sealed `.log` and `.index`
4. Persist partition state and producer checkpoint
5. Open the next active segment at the current log end

## Topic Deletion

Topic deletion is asynchronous and resumable:

1. Persist a deletion marker under `_coordination/topic_deletions/`
2. Remove topic metadata immediately so the topic disappears from HTTP and Kafka APIs
3. Background GC deletes topic S3 data
4. For diskless topics, background GC then clears diskless metastore state
5. The deletion marker is removed last

This ordering prevents diskless metastore refs from being removed before the
backing S3 data path is actually gone.

## Retention Cleanup

Time-based retention cleanup is also asynchronous and resumable.

Retention is now partition-leader-executed:

1. The partition-leader maintenance service discovers expired retention work for partitions it owns
2. It persists a partition job under `_coordination/partition_jobs/`
3. For classic segments, it deletes the segment object and index sidecar
4. For classic segments, it invalidates local sealed-segment cache and in-memory index refs
5. For classic segments, it deletes the segment metadata sidecar
6. For diskless files, it deletes the S3 data file first and then removes diskless metastore refs
7. It removes the partition job

Diskless retention remains conservative at the file level. If a backing file
still contains newer live refs, the file is not deleted yet, which means its
expired refs are also retained until the whole file becomes eligible.

Partition-leader maintenance is bounded per node by
`coordination.maintenance_max_concurrency`, so retention and merge work remain
parallel without fanning out unbounded across owned partitions.

## Coordination

Camu uses S3 conditional writes instead of a separate consensus cluster.

- One cluster controller publishes assignments
- Partition leaders own produce and replication for their partitions
- Partition leaders now execute partition-local maintenance through a dedicated service layer
- Partition followers now handle proxying and fetch/failover transitions through a dedicated service layer
- Epoch history fences stale leaders and supports divergence checks

See [architecture/coordination.md](architecture/coordination.md) for the coordination-specific view.
