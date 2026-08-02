# Parquet Export and SQL Query Design

This document describes a design for extending Camu's partition-leader
maintenance model with Parquet export and query support.

The goal is not to replace the native log. The canonical write/read path remains
Kafka `RecordBatch` bytes. Parquet is an additional analytical projection built
from committed log data.

## Goals

- Trigger more natural classic compaction from segment size thresholds rather
  than only conservative adjacent-pair merge discovery.
- Export committed log data into Parquet files partitioned by date and hour.
- Use DuckDB as the writer for Parquet generation and the query engine for
  analytical reads.
- Add a second-stage Parquet compaction flow so small Parquet files do not
  accumulate indefinitely.
- Add `POST /v1/sql` to run SQL over exported Parquet data.

## Non-Goals

- Replacing native Camu consume/fetch paths with SQL queries.
- Using Parquet as the source of truth for produce acknowledgements.
- Full OLAP warehouse feature parity.
- Arbitrary mutation SQL.
- Cross-topic transactional guarantees.

## Core Principles

- The native log remains canonical.
- All Parquet work is asynchronous.
- Only committed data is eligible for Parquet export.
- Partition-local export and compaction should run on the partition leader.
- Query execution should be able to run on separate query-only nodes.
- Query-visible Parquet state must be manifest-driven, not raw-directory-driven.
- Export identity must be deterministic and idempotent for the same committed source range.
- Physical Parquet partitioning should use ingest/flush time, not event time.
- Every Parquet file must carry a schema version.
- Topic deletion remains coordination-leader-led.
- Parquet artifacts must be resumable and rebuildable from the canonical log.
- Deletion and compaction must fence publication so stale jobs cannot republish deleted or superseded data.

## Existing Foundation

The current codebase already has the right execution model for this direction:

- `PartitionIdentity` for leader/follower fencing
- `PartitionJob` for durable partition-leader maintenance
- partition-leader retention jobs
- partition-leader classic segment merge jobs
- coordination-leader topic deletion

This design reuses the same partition-job framework rather than introducing a
separate background system.

## Problem Statement

Today Camu stores the canonical log in native segment format. That is correct
for streaming and Kafka compatibility, but it leaves two gaps:

1. classic cold-data layout still needs a more natural merge policy driven by
   segment sizes
2. analytical access over historical data is awkward without a columnar
   projection

If Camu is going to support product and internal analytics without a separate
  ETL stack, it needs:

- a durable export from committed log data into Parquet
- a query surface on top of that Parquet data
- a file-compaction story so the Parquet layout stays efficient

## Proposed Architecture

### Layers

Keep the existing role split:

- `CoordinationLeader`
  - topic deletion orchestration
  - cluster-global cleanup
  - no partition-local Parquet execution
- `PartitionLeader`
  - native retention
  - native segment merge
  - Parquet export
  - Parquet compaction
- `PartitionFollower`
  - replication and catch-up only
- `QueryEngine`
  - read-only DuckDB query execution over Parquet data
  - no partition ownership
  - no produce, fetch, replication, retention, or compaction duties

### Query-Only Deployment Mode

Add a mode that runs only the analytical query surface on separate instances.

This mode should:

- expose `POST /v1/sql`
- mount or access the same object-storage bucket as the main Camu cluster
- read Parquet files, published Parquet manifests, and a read-only query catalog only
- not participate in leader election
- not own partitions
- not serve produce or native consume traffic
- not run partition maintenance jobs

This gives a clean deployment split:

- streaming cluster nodes handle produce/consume/replication/export
- query nodes handle analytical SQL over Parquet only

That avoids mixing DuckDB query load with hot-path streaming nodes.

Query-only nodes must not infer visibility from raw `dt/hour` directory listings.
They should only query files referenced by the current published manifest and
read-only catalog snapshot.

### Maintenance Work

Parquet export is a long-lived internal consumer, not a partition-job type. Its
per-partition cursor is stored in the pipeline checkpoint described below.
Native segment merge remains a separate maintenance job. Parquet compaction is
future work and must not be confused with export or retention.

### V1 Scope

The first shippable version should stay narrow:

- partition leaders export committed native data into Parquet
- Parquet visibility is manifest-driven
- `POST /v1/sql` reads only from published manifests
- query-only mode is a mode in the same binary, not a separate control plane
- compaction only rewrites same-schema files within one `dt/hour` bucket

Deferred from v1:

- cross-topic joins as a product goal
- schema widening across Parquet versions
- event-time physical bucketing
- multi-node query orchestration
- Parquet compaction across buckets or schema versions

## Native Segment Compaction Direction

### Current Gap

Classic merge currently uses a conservative adjacent-pair discovery rule. That
works, but it is not a natural long-term policy.

### Proposed Rule

Base classic merge eligibility on segment sizes.

For one topic-partition:

1. list retained sealed segments in offset order
2. form a contiguous same-epoch run
3. accumulate segments until:
   - total bytes reach `maintenance.segment_merge.target_bytes`, or
   - segment count reaches `maintenance.segment_merge.max_segments`
4. if the accumulated run contains at least two segments and exceeds
   `maintenance.segment_merge.min_input_bytes`, enqueue one merge job

### Why Size-Based Thresholds Are Better

- they converge cold-data layout toward fewer larger objects
- they behave more predictably than fixed adjacent-pair merging
- they reduce object-count fan-out for longer retention windows
- they make downstream Parquet export batching more natural

### Suggested Settings

- `maintenance.segment_merge.enabled`
- `maintenance.segment_merge.min_input_bytes`
- `maintenance.segment_merge.target_bytes`
- `maintenance.segment_merge.max_segments`
- `maintenance.segment_merge.min_segment_age`

## Parquet Export Model

### What Gets Exported

Only committed data should be exported:

- for replicated topics: up to the committed high watermark
- for non-replicated topics: up to the local committed tail once the segment is
  sealed

This avoids exporting unstable tail data and keeps Parquet as a committed view
of history.

### Export Granularity

The export consumer reads a deterministic committed record batch, not live
active-segment data.

That gives:

- deterministic replay
- resumability
- a clear mapping from native source range to Parquet artifact
- idempotent retry at the same final file identity

### Query-Visible Manifest

Queries should not discover Parquet files by listing bucket directories.

Instead, every query-visible bucket should publish a manifest under a stable path
such as:

```text
_meta/parquet_manifests/{topic}/dt=YYYY-MM-DD/hour=HH/part-{partition}.json
```

The manifest is the only source of truth for visible Parquet files in that
bucket.

Manifest contents should include:

- `generation`
- `topic`
- `partition`
- `dt`
- `hour`
- `schema_version`
- visible file list
- replacement generation, if any
- `updated_at`

Export and compaction publish files first, then advance the manifest, then
garbage-collect old files later.

### Target Layout

Parquet should be written into an ingest-time date/hour partitioned path
layout.

Suggested layout:

```text
parquet/dt=YYYY-MM-DD/topic={topic}/hour=HH/{file}.parquet
```

V1 file naming should be deterministic:

```text
parquet/dt=YYYY-MM-DD/topic={topic}/hour=HH/{file-id}.parquet
```

Example:

```text
parquet/dt=2026-04-11/topic=events/hour=13/8f1c2e7a.parquet
```

This gives:

- pruning by date/hour
- partition isolation
- predictable query paths
- straightforward lifecycle cleanup on topic deletion

### Time Source For Partitioning

Use ingest/flush time for physical `dt/hour` path placement.

Keep event/record timestamp as a row column for SQL filtering and analysis.

This is the v1 rule because it gives stable bucket membership for export,
compaction, retention, and query-node cache reuse.

### Exported Schema

Recommended baseline schema:

- `topic`
- `partition`
- `offset`
- `timestamp`
- `key` as `BLOB`
- `value` as `BLOB`
- `headers` as nested/JSON-encoded representation
- `producer_id`
- `producer_epoch`
- `sequence`
- `ingested_at`
- `schema_version`

Optional later:

- physical typed columns for topics configured with the V1 immutable JSON schema

For V1, topics may declare an immutable JSON schema. Their configured fields
are exported as physical typed Parquet columns; raw `key` and `value` bytes are
retained alongside the typed columns. Schema evolution and additional encodings
remain future work.

For v1, `headers` should use one explicit representation:

- JSON-encoded value

### Schema Evolution

Schema evolution must be explicit.

Every Parquet file should carry:

- `schema_version`
- the physical column layout for that version

V1 compaction rule:

- only compact files with the same `schema_version`

Later, the system can support widening across versions, but that should not be
part of the first implementation.

## DuckDB Integration

### Why DuckDB

DuckDB fits this design because it can:

- read staged native rows in-process
- write Parquet efficiently
- compact Parquet by rewriting query results
- execute SQL for the future `/v1/sql` endpoint

### Writer Model

Use DuckDB on the partition leader as an embedded export engine.

Export flow:

1. partition leader claims a `parquet_export` job
2. native committed source range is read and decoded into a row stream
3. DuckDB writes the row stream into a local temporary DuckDB table or directly
   into Parquet
4. the produced Parquet file is uploaded to a deterministic final object key
5. the bucket manifest is advanced
6. export metadata/checkpoint is persisted
7. the job is finalized

### Reader Model

DuckDB should also run in a query-only role.

Two deployment shapes should be supported:

1. embedded on normal Camu nodes for small/simple deployments
2. query-only nodes for production analytical isolation

The design should treat query execution as logically separate from partition
maintenance even if both use DuckDB.

### Caching Model

DuckDB should use local caching on the node where it runs.

There are two useful cache layers:

1. Camu-managed object cache
   - reuse the existing local object/cache directory for downloaded Parquet
     files where practical
   - avoid refetching the same Parquet files from object storage on repeated
     queries or compaction passes
2. DuckDB local temp/cache state
   - allow DuckDB to use a bounded local temp directory for intermediate query
     and rewrite work
   - keep this separate from the canonical data path

For query-only nodes, caching is especially important because these nodes do not
own partitions and will otherwise repeatedly scan object storage for hot query
ranges.

Recommended first rule:

- cache Parquet objects by object key plus ETag/version
- invalidate or bypass cached entries when object version changes
- bound cache size explicitly

The design should treat cache as an optimization only. Object storage remains
the source of truth for Parquet data.

### Why Not Write Parquet Manually First

DuckDB reduces implementation risk:

- one engine for writing and querying
- fewer custom Parquet details in Camu
- easier future schema evolution
- easier second-stage compaction

## Export Checkpointing

Each topic-partition has one durable pipeline checkpoint at
`_meta/pipelines/parquet-export/{topic}/{partition}.json`. The checkpoint is
advanced only after the immutable Parquet object and its bucket manifest have
been published. It records the next source offset, output sequence, source
epoch, and a monotonic generation.

The checkpoint is not a source of truth for the log. It is the resumable cursor
for the internal committed-record consumer; manifests remain the source of SQL
visibility.

## Parquet Compaction

### Why It Is Needed

If export writes one Parquet file per small native source range, the Parquet
side will repeat the same small-file problem that native merge is trying to
reduce.

### Proposed Two-Stage Model

Stage 1: export

- write smaller committed Parquet files quickly and incrementally

Stage 2: Parquet compaction

- combine small files into larger analytical files under the same `dt/hour`
  partition

### Compaction Job

Use `parquet_compaction` as a partition-leader job.

For one topic-partition and one `dt/hour` bucket:

1. list candidate Parquet files
2. if total size or file count crosses threshold, claim a compaction job
3. DuckDB reads those files
4. DuckDB rewrites them into fewer larger Parquet files
5. new compacted files are uploaded
6. a new manifest generation is published for that bucket
7. old Parquet files are removed later by garbage collection
8. compaction metadata is updated

This manifest swap is the visibility boundary. Query nodes must never infer
visibility by listing both old and new files directly.

### Suggested Settings

- topic-level `export_enabled`
- `maintenance.parquet_export.max_records`
- `maintenance.parquet_export.max_duration`
- `maintenance.parquet_export.temp_directory`
- `maintenance.parquet_compaction.enabled`
- `maintenance.parquet_compaction.min_file_count`
- `maintenance.parquet_compaction.target_file_size_bytes`
- `maintenance.parquet_compaction.max_input_files`

## SQL Endpoint

### API

Add:

- `POST /v1/sql`

This endpoint can be exposed by:

- normal Camu nodes in combined mode
- dedicated query-engine nodes in query-only mode

### Request Shape

Suggested first version:

```json
{
  "sql": "select dt, count(*) from parquet_scan group by 1",
  "params": [],
  "topics": ["events"],
  "time_range": {
    "from": "2026-04-10T00:00:00Z",
    "to": "2026-04-11T00:00:00Z"
  },
  "limit": 1000
}
```

### Response Shape

Suggested first version:

```json
{
  "columns": [
    {"name": "dt", "type": "DATE"},
    {"name": "count_star()", "type": "BIGINT"}
  ],
  "rows": [
    ["2026-04-10", 12345]
  ]
}
```

### Query Model

The server should not expose unrestricted DuckDB filesystem access.

Instead:

1. resolve allowed manifests from requested topics and time-range filters
2. create DuckDB views/tables over those paths
3. run the user SQL in a constrained session
4. stream or return bounded results

This should be a curated query surface, not "arbitrary DuckDB on the host".

In query-only mode, the endpoint should resolve only against:

- Parquet data paths
- published Parquet manifests
- read-only query catalog metadata

It should not depend on local partition runtime, active segments, or ownership
state.

The endpoint should also preferentially query through cached local Parquet
copies when they are valid, so repeated analytical reads over the same time
windows do not repeatedly hit object storage.

Envelope filters and SQL parameters have different roles:

- `topics` and `time_range` define allowed file scope
- `params` are only SQL bind variables inside that already-scoped query

### Safety Rules

- read-only SQL only
- bounded result size
- bounded execution time
- bounded scanned path set
- no arbitrary file reads
- no external network access
- topic allowlist enforced by API request context

## Data Flow

### Native to Parquet Export

1. the partition leader's internal consumer reads committed records
2. records are decoded and validated against the topic schema
3. DuckDB writes a bounded Parquet chunk locally
4. the chunk uploads to a deterministic object key under `parquet/dt=.../topic=.../hour=.../`
5. the bucket manifest advances
6. the pipeline checkpoint advances

Schema failures are either skipped or written in batches to the configured
dead-letter topic before the source checkpoint advances.

### Parquet Query

1. client sends `POST /v1/sql`
2. server validates query envelope and topic/time scope
3. server resolves matching manifest generations and Parquet paths
4. DuckDB runs read-only query over those paths
5. server returns bounded rowset

### Parquet Compaction

1. partition leader discovers small-file bucket
2. writes `parquet_compaction` job
3. DuckDB reads source files
4. writes larger Parquet file(s)
5. uploads replacement files
6. publishes a new manifest generation
7. old files are deleted later
8. finalizes job

## Coordination and Ownership

### Partition Leader Responsibilities

- consume eligible committed native records
- write Parquet files
- compact Parquet files
- advance the pipeline checkpoint
- resume from the checkpoint after restart or reassignment

### Query Engine Responsibilities

- resolve topic/time-scope to published manifests and Parquet paths
- run bounded read-only DuckDB SQL
- return result sets
- maintain no ownership over topic-partitions

Query-engine nodes should not:

- run partition jobs
- take part in replication
- become coordination leader
- mutate topic or group state

Query-engine visibility is eventually consistent with object-storage publication.
That is acceptable for analytical SQL, but it must be explicit.

### Coordination Leader Responsibilities

- topic deletion
- cluster-global cleanup only

Topic deletion must also remove:

- published Parquet manifests
- read-only query catalog metadata for the topic
- Parquet pipeline checkpoints
- Parquet data paths
- any Parquet compaction/export job remnants

But it should still remain coordination-leader-led, consistent with the current
deletion model.

## Failure and Resume Semantics

### Export

Parquet export should be resumable:

1. read a committed source batch
2. write a local temp Parquet artifact
3. upload the immutable Parquet file
4. publish or replace the manifest entry
5. advance the pipeline checkpoint

If a crash happens before checkpoint advance, replay is safe because Parquet is
a derived view and the source log is canonical, but only if the same committed
source range retries to the same deterministic object identity and republishes
through the manifest.

### Compaction

Parquet compaction should be resumable:

1. persist compaction job
2. write replacement Parquet file(s)
3. upload replacements
4. publish a new manifest generation
5. delete old files later
6. clear job

### Reassignment

Like retention and native merge:

- jobs carry expected owner and leader epoch
- stale owners must stop
- new owners rediscover and resume safely
- stale owners must not be able to publish a newer manifest generation after
  reassignment or deletion fencing begins

## Deletion and Retention Semantics

### Topic Deletion

Topic deletion must delete:

- native topic data
- diskless metastore data if relevant
- Parquet checkpoint metadata
- published Parquet manifests
- read-only query catalog metadata
- Parquet object paths

Ordering should stay conservative:

1. hide topic
2. fence all new and in-flight Parquet export/compaction publication
3. delete derived Parquet manifests, checkpoints, and data
4. delete native S3 topic data
5. delete diskless metastore refs if needed
6. clear deletion marker

Exact ordering between native and derived Parquet data can be implementation
specific, but the final design must avoid leaving live query metadata pointing
at deleted objects.

### Native Retention vs Parquet Retention

Native retention is gated by durable export progress: with Parquet export
enabled, classic retention deletes a sealed native segment only when the
partition pipeline checkpoint covers its end offset. A missing, behind, or
unreadable checkpoint blocks native cleanup for that partition. This may grow
native storage temporarily, but avoids permanent query gaps.

Parquet retention still needs an explicit policy:

- Parquet follows native retention horizon per topic
- if native data ages out, the equivalent Parquet data is also eligible for
  deletion

Alternative policy later:

- allow longer Parquet retention than native log retention

That would make Camu more warehouse-like, but it is a bigger product decision.

## Operational Concerns

### Metrics

Add metrics for:

- pending export jobs
- pending Parquet compaction jobs
- export lag by topic-partition
- exported bytes
- compacted Parquet bytes
- DuckDB job duration
- SQL query duration
- SQL rows returned
- SQL bytes scanned
- manifest publish count
- manifest generation lag

### Concurrency

Parquet work should reuse bounded partition maintenance concurrency rather than
introducing a second independent worker pool first.

Later, if needed, split:

- native maintenance concurrency
- Parquet maintenance concurrency
- SQL query concurrency

### Query-Only Node Configuration

Add a mode such as:

- `server.mode = "query"`

Or an equivalent explicit feature flag set.

In query-only mode:

- Kafka listener can be disabled
- produce/consume HTTP endpoints can be disabled
- coordination leader election can be disabled
- partition maintenance can be disabled
- DuckDB and `POST /v1/sql` stay enabled
- object-storage credentials remain required
- local cache directory remains required

Suggested additional settings:

- `sql.cache.directory`
- `sql.cache.max_size`
- `sql.duckdb.temp_directory`
- `sql.duckdb.memory_limit`
- `sql.max_concurrency`

This is intentionally a read-only analytical role, not a degraded full node.

### Resource Controls

DuckDB jobs need explicit bounds:

- temp disk space
- memory ceiling
- per-query timeout
- max result rows
- max scanned files
- cache size
- cache eviction policy

Query-only nodes also need fairness rules:

- explicit `sql.max_concurrency`
- admission control or queueing
- per-query timeout and memory limit
- no single query may monopolize the node indefinitely

## Open Questions

1. Should Parquet retention exactly mirror native retention, or can Parquet keep
   data longer?
2. Should `POST /v1/sql` allow joins across topics in the first version?
3. Which additional encodings (Avro/Protobuf) should be supported after the V1
   JSON schema and physically typed Parquet columns?
4. Should Parquet compaction be partition-leader-local forever, or eventually
   move to a separate analytical maintenance role?
5. Should the read-only query catalog be one global snapshot or topic-scoped manifests only?

## Suggested Delivery Order

1. Improve classic native merge policy to size-threshold-based discovery.
2. Add Parquet manifests, deterministic object identity, and export checkpoint model.
3. Integrate embedded DuckDB for Parquet writing.
4. Write Parquet under ingest-time `parquet/dt=YYYY-MM-DD/topic={topic}/hour=HH/`.
5. Add restart/failure integration coverage for the internal export consumer.
6. Add `POST /v1/sql` with read-only bounded DuckDB execution over manifests.
7. Add query-only mode on separate nodes.
8. Add optional same-schema Parquet compaction with manifest-generation publication.
9. Add topic deletion fencing and Parquet cleanup.
10. Add metrics and operational tuning.

## Bottom Line

The clean design is:

- native log stays canonical
- partition leaders project committed history into Parquet
- DuckDB is the shared engine for writing and querying Parquet
- query execution can be isolated on dedicated query-only nodes
- Parquet files are partitioned by ingest-time date/hour, published through manifests, and later compacted
- SQL is a bounded analytical API on top of derived Parquet data
- topic deletion remains coordination-leader-led

That keeps Camu's streaming path simple while adding a credible analytical
surface on the same data.
