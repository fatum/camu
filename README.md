# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

**Camu is an S3-native event log for teams that want Kafka-style durability and replication without running Kafka’s full operational stack.**

It stores native Kafka `RecordBatch` bytes, serves both HTTP and Kafka clients, coordinates cluster state through object storage, and keeps the deployable unit simple: one binary, object storage, and optional local cache.

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

## Why Camu

Camu is built for the cases where the value is the log, not the infrastructure around the log.

- **Object-storage-native durability**: segments, indexes, offsets, assignments, and control-plane state live in S3-compatible storage.
- **Single-binary deployment**: no ZooKeeper, no Raft quorum, no external metadata plane.
- **Dual protocol surface**: HTTP for product-friendly integration, Kafka wire protocol for existing Kafka clients and tooling.
- **Replicated writes with ISR semantics**: `replication_factor` and `min_insync_replicas` work the way operators expect.
- **Idempotent produce**: duplicate retry handling with producer IDs and sequence tracking.
- **Classic and diskless modes**: pick local active-segment buffering or object-storage-first ingestion depending on the workload.
- **Parquet + SQL layer**: export classic sealed segments into Parquet and query them through embedded DuckDB.
- **Resumable background maintenance**: topic deletion, retention, and classic sealed-segment merge are durable workflows rather than best-effort cleanup.
- **Jepsen-backed confidence**: the project is fault-tested, not only unit-tested.

## Feature Set

### Storage and Durability

- Native Kafka `RecordBatch` bytes are the canonical log format.
- Immutable sealed segments are uploaded to S3-compatible storage.
- Local active segments provide a fast mutable tail in `classic` mode.
- `diskless` topics keep the storage path object-store-centric while preserving the same external API shape.
- Time-based retention is asynchronous and resumable in both modes.
- Classic sealed-segment merge reduces cold object fan-out through partition-leader background jobs.

## Diskless Mode

`diskless` is the mode for teams that want Camu to lean harder into object
storage and lighter into local segment ownership.

Instead of relying on the normal `classic` active-segment lifecycle,
`diskless` topics use the diskless engine and metastore path while keeping the
same external HTTP and Kafka topic model.

### What It Gives You

- **Object-storage-first write path**: the topic’s durable shape is built around the diskless engine rather than classic local segment ownership.
- **Same client-facing contracts**: produce, consume, offsets, and Kafka metadata still look like the same product surface.
- **Good fit for stateless-ish nodes**: useful when you want nodes to carry less local log responsibility and put more of the durable path in shared storage.
- **Same topic abstraction**: `diskless` topics still behave like first-class topics rather than a separate product or API family.

### Metastore Model

`diskless` topics use a metastore that maps topic-partition offset ranges to backing objects in shared storage.

- The metastore records which file contains which partition data and the byte range for that partition slice.
- One backing object can be referenced by multiple partition slices, so object ownership is not one-file-per-partition.
- Reads resolve through the metastore first, then fetch and decode the referenced data from object storage.
- Earliest retained offset for a partition is derived from the current live metastore view, not assumed to start at `0`.

### How It Differs From Classic

| Area | `classic` | `diskless` |
|---|---|---|
| Mutable write tail | local active segment | diskless engine path |
| Read path | sealed segments + active segment merge | diskless fetch/decode path |
| Retention unit | classic segment objects | backing file references |
| Retention behavior | delete segment/index, then metadata | delete S3 data first, then metastore refs |
| Cold-data optimization | sealed-segment merge | not the same merge path |

### Operational Characteristics

- The metastore is part of the durable read path, not just background bookkeeping.
- `diskless` retention is resumable and ordered safely: delete S3 data first, then remove metastore refs.
- `diskless` retention is also conservative at the backing-file level. If a file still contains newer live refs, old refs in that file remain until the whole file becomes eligible.
- Kafka topic creation supports `diskless` directly through `camu.storage.mode=diskless`.
- Storage mode is immutable after topic creation.
- Arbitrary timestamp lookup through Kafka `ListOffsets` is currently unsupported for `diskless` topics and returns an explicit invalid-request style error.

### When To Choose It

Choose `diskless` when you care more about:

- pushing durability and state coordination toward object storage
- minimizing dependence on classic local active-segment ownership
- keeping the same public API while changing the internal storage path

Choose `classic` when you care more about:

- the most mature local-tail behavior
- classic segment/index lifecycle
- classic sealed-segment optimizations such as segment merge

### Creating Diskless Topics

Over HTTP:

```json
{
  "name": "events",
  "partitions": 4,
  "retention": "168h",
  "storage_mode": "diskless"
}
```

Over Kafka `CreateTopics`:

- `camu.storage.mode=diskless`

The detailed creation and API examples live in [docs/api.md](docs/api.md).

### Protocols and Client Compatibility

- HTTP API for topic admin, produce, consume, offsets, and SSE streaming.
- Kafka wire protocol support for the implemented API subset, advertised through `ApiVersions`.
- Existing Kafka clients can produce and consume without custom protocol adapters.
- HTTP and Kafka topic creation both support `classic` and `diskless` storage modes.

### Replication and Correctness

- Partition leadership and cluster assignments are coordinated through S3 conditional writes.
- Replicated writes wait for ISR quorum acknowledgement.
- Reads are capped by readable high watermark so consumers do not see uncommitted replicated data.
- Failover uses assignment epochs and epoch history to fence stale leaders.
- Follower fetch, proxying, and failover are now isolated behind follower-specific service logic rather than scattered checks.

### Operations

- One binary, one config file, and an S3-compatible bucket are enough to start.
- Background maintenance is bounded by `coordination.maintenance_max_concurrency`.
- Topic deletion is asynchronous, resumable, and safe for restart.
- Retention and classic merge are partition-leader-executed durable jobs.

## Parquet and SQL

Camu can project committed records from export-enabled classic topics into Parquet and expose those files through a bounded read-only SQL endpoint.

- A fenced Parquet consumer runs asynchronously for each partition leader and never blocks produce.
- Physical file layout uses ingest-time buckets:
  - `parquet/dt=YYYY-MM-DD/topic={topic}/hour=HH/{file-id}.parquet`
- Query visibility is manifest-driven, not raw directory-list-driven.
- `POST /v1/sql` only reads files referenced by published manifests.

SQL authentication: when `server.auth_token` is configured, `/v1/sql` requires
`Authorization: Bearer <token>`. TLS is expected to terminate at the deployment
proxy; Camu does not provide TLS in this release.

Each consumer batch is bounded by
`maintenance.parquet_export.max_records` and `max_duration`; DuckDB conversion uses the configured
`maintenance.parquet_export.temp_directory`. Parquet compaction is deferred and
is not an executable maintenance job in this release.
- Query execution uses embedded DuckDB with local cache and explicit bounds:
  - `sql.enabled`
  - `sql.cache_directory`
  - `sql.duckdb_temp_directory`
  - `sql.cache_max_size`
  - `sql.duckdb_memory_limit`
  - `sql.max_concurrency`
  - `sql.query_timeout`
  - `sql.max_scan_files`

Current shape:

- SQL is read-only: `SELECT` / `WITH` queries only.
- Queries are topic-scoped and must name one or more topics in the request envelope.
- Query nodes can run separately with `server.mode = "query"`.
- Parquet export is opt-in per classic topic with `export_enabled: true`.
- With export enabled, classic retention deletes a sealed native segment only
  after its durable pipeline checkpoint covers the segment's end offset. If the
  checkpoint is absent, behind, or unreadable, retention pauses for that
  partition rather than risking a permanent SQL data gap.
- Export-enabled topics require `unclean_leader_election=false`. Export
  publishes per-ingest-hour manifests and cannot atomically reconcile a
  divergent offset history after an unclean leader election. Before enabling
  export, turn off unclean leader election for that topic. A stream node
  refuses to start while an export-enabled classic topic remains
  incompatible; Camu does not rewrite or migrate topic configuration.

For a dedicated query node, use [`camu.query.yaml.example`](camu.query.yaml.example):
SQL is required, and the node does not need an internal or Kafka listener. Give
it read-only object-store access limited to `parquet/`, `_meta/parquet_manifests/`,
and `_meta/topics/`; `/v1/cluster/status` is local node status, not cluster discovery.

Example:

```bash
curl -X POST http://localhost:8080/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{
    "sql":"select count(*)::BIGINT as n from \"events\"",
    "topics":["events"]
  }'
```

## Jepsen Notes

Camu has a repository-local Jepsen harness. The goal is not to claim "everything is linearizable"; the goal is to prove the durability and failover properties that the product actually depends on.

- The harness runs a five-node Docker cluster against MinIO-backed storage.
- The currently documented matrix includes **22 passing scenarios** across `kill`, `partition`, `pause`, `leader-kill`, `leave`, `membership`, `rejoin`, `s3-partition`, `clock-skew`, and combined faults.
- The strongest claim is for replicated topics: acknowledged writes survive the tested fault matrix and committed prefixes survive leader failover.
- Jepsen is complemented by unit and integration coverage; it is not the only correctness layer.
- Every run produces artifacts you can inspect locally, including `results.edn`, `history.edn`, `jepsen.log`, and per-node `camu.log` files.

Jepsen in Camu is mainly a **durability and failover** signal, not a blanket claim about arbitrary low-latency replica-read freshness under all conditions.

For the harness, scenarios, and run commands, see [jepsen/camu/README.md](jepsen/camu/README.md). For the higher-level interpretation of what the Jepsen matrix proves, see [docs/reliability.md](docs/reliability.md).

## Where It Fits

Camu is a strong fit when you want:

- a durable append log backed by object storage
- Kafka client compatibility without operating Kafka itself
- a simpler control plane than broker + controller + metadata quorum stacks
- HTTP-first integration for internal services, edge systems, or product backends
- replicated write semantics with a smaller operational footprint

It is not trying to be a full Kafka clone. The project is intentionally focused on a well-supported subset with explicit unsupported behavior rather than maximal protocol surface.

## What An Ack Means

| Mode | Durability guarantee |
|---|---|
| `rf=1`, `minISR=1` | A successful produce is durable in the local active segment on the owning node. |
| `rf>1` | A successful produce is durable on the leader and only acknowledged after the configured ISR quorum confirms it. |

Flush to object storage is asynchronous. Cross-instance visibility for non-replica reads follows flush timing, not ack timing.

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
  maintenance_max_concurrency: 4
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

### First Requests

```bash
curl http://localhost:8080/v1/ready

curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":4,"retention":"168h"}'

curl -X POST http://localhost:8080/v1/topics/events/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"user-123","value":"clicked"}]'

curl "http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=100"
```

## Documentation

- [API Guide](docs/api.md)
- [API Support Matrix](docs/api-support-matrix.md)
- [Parquet + SQL Design](docs/parquet-sql-design.md)
- [Architecture](docs/architecture.md)
- [Coordination Architecture](docs/architecture/coordination.md)
- [Reliability Notes](docs/reliability.md)
- [Partition Maintenance Refactor](docs/partition-maintenance-refactor.md)

## Current Highlights

- `CreateTopics` supports `camu.storage.mode=diskless`
- `camu.storage.mode` is immutable after creation
- `retention.bytes` is explicitly unsupported
- `CreatePartitions` is expand-only
- retention is time-based and resumable in both `classic` and `diskless`
- follower-side leader proxying and failover handling are isolated behind a dedicated service layer

## Status

The canonical current support status lives in [docs/api-support-matrix.md](docs/api-support-matrix.md).

The project is deliberately optimized for:

- strong correctness of the supported subset
- explicit unsupported behavior
- simple deployment and operational clarity

## License

AGPL-3.0. See [LICENSE](LICENSE).
