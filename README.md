# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

**Camu is an S3-native event log with HTTP and Kafka APIs.** It keeps the
mutable tail on the partition owner, replicates it to an ISR quorum, and
persists immutable segments in S3-compatible storage.

```text
producer ──HTTP / Kafka──> partition leader ──fetch──> ISR followers
                                  │
                                  └── background sealed segments ──> S3 / MinIO
```

## Why Camu?

Use Camu when applications need an ordered, replayable event log but object
storage should be the durable shared data plane.

- Keep the familiar partition, consumer-offset, idempotent-produce, and ISR
  quorum model for operational data.
- Store immutable history in S3-compatible storage rather than a separate
  broker-disk fleet and a second archival pipeline.
- Query committed history with Parquet and DuckDB SQL without copying it into a
  separate analytics system.

Camu is a focused alternative for systems that value S3-native durability and
simple replayable streams; it is not a drop-in replacement for the full Kafka
ecosystem.

## Core model

- A topic has partitions, a replication factor, and `min_insync_replicas`.
- The partition leader appends native Kafka `RecordBatch` bytes to its local
  active segment. Followers replicate those bytes without re-encoding.
- For replicated topics, a produce succeeds only after the high watermark
  passes the written offset: the configured ISR quorum has acknowledged it.
- Segment sealing and S3 persistence happen in the background. A pending
  sealed segment remains available to follower replication until it is
  published.
- Object storage holds immutable segments and the coordination state; there is
  no separate metadata quorum.

| Produce configuration | Successful response means |
| --- | --- |
| `rf=1`, `minISR=1` | The owner appended the record to its local active segment. |
| `rf>1` | The leader and the configured ISR quorum have the record. |

Object-store persistence is asynchronous in both cases. Do not treat an
`rf=1` acknowledgement as protection against permanent loss of the only owner
before its active segment is persisted.

## What Camu provides

- HTTP, SSE, and a supported subset of the Kafka wire protocol.
- Idempotent produce with producer IDs and per-partition sequences.
- `classic` topics (local active tail plus sealed S3 segments) and `diskless`
  topics (object-store-centric storage path).
- Optional leader-owned Parquet export and read-only DuckDB SQL. Query
  visibility is manifest-driven.
- Resumable topic deletion, retention, and maintenance jobs.

It is intentionally not a complete Kafka implementation. See the
[API support matrix](docs/api-support-matrix.md) for exact support and explicit
limitations.

## SQL analytics

Set `export_enabled: true` on a classic topic to project its committed records
to Parquet. Export runs on the partition leader and does not delay produce.
`POST /v1/sql` exposes those manifest-published files through read-only DuckDB
SQL:

```bash
curl -X POST http://localhost:8080/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select count(*) as n from \"events\"","topics":["events"]}'
```

SQL can run on a separate `server.mode=query` node with read-only object-store
credentials. See [Parquet and SQL](docs/parquet-sql.md) for the consistency and
retention rules.

### SQL sizing and performance

DuckDB is a vectorized analytical engine: Parquet projection and filter pushdown
mean that a selective query can read far less than the whole topic. There is no
single records-per-second promise; scan rate depends on selected columns,
predicate selectivity, Parquet layout, CPU, local temporary-disk throughput,
and object-store latency.

The queryable dataset is not limited to RAM. DuckDB can spill larger-than-memory
workloads to its temporary directory, and reports production database files
larger than 15 TB. Camu still applies request-level bounds, by default:

| Limit | Default | Meaning |
| --- | ---: | --- |
| Parquet cache | 5 GiB | Local object cache, not a dataset-size cap. |
| Concurrent queries | 4 | Independent DuckDB connections. |
| Query timeout | 30s | Wall-clock budget per request. |
| Files per query | 4,096 | Maximum manifest-referenced files scanned. |

Size a query node for its working set, not its total history: provide fast local
disk for `sql.duckdb_temp_directory`, set `sql.duckdb_memory_limit` below the
memory available to Camu, and increase `sql.max_scan_files` only with measured
queries. DuckDB recommends roughly 1–4 GiB of memory per execution thread for
good performance, and its Parquet guidance favors files around 100 MB–10 GB
with row groups large enough to use available cores. Camu does not yet compact
Parquet automatically, so benchmark your actual exported file layout before
raising these limits.

See DuckDB’s [workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads),
[out-of-memory guidance](https://duckdb.org/docs/current/guides/performance/oom),
and [Parquet guidance](https://duckdb.org/docs/current/guides/performance/my_workload_is_slow).

## Quick start

Start MinIO and create a bucket:

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/camu-data
```

Build Camu, copy the example configuration, and set `storage.endpoint`,
`storage.bucket`, and credentials for MinIO:

```bash
go build -o camu ./cmd/camu
cp camu.yaml.example camu.yaml
./camu serve --config camu.yaml
```

Create, produce, and consume:

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":4,"retention":"168h"}'

curl -X POST http://localhost:8080/v1/topics/events/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"user-123","value":"clicked"}]'

curl 'http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=100'
```

For a replicated topic, set `replication_factor` and `min_insync_replicas` at
creation time. See the [API guide](docs/api.md) for request shapes, idempotent
produce, consumer offsets, SQL, and Kafka behavior.

## Documentation

- [Architecture](docs/architecture.md): write/read paths, storage, and maintenance.
- [Coordination](docs/architecture/coordination.md): leases, assignments, ISR, and failover.
- [API guide](docs/api.md) and [API support matrix](docs/api-support-matrix.md).
- [Parquet and SQL](docs/parquet-sql.md): export and query model.
- [Reliability](docs/reliability.md): guarantees, limits, and Jepsen evidence.
- [Jepsen harness](jepsen/camu/README.md): run commands and artifacts.

## Verification

Core distributed behavior is covered by integration tests and a five-node
Jepsen harness backed by MinIO. The harness checks acknowledged-write
durability, leader safety, ordering, high-watermark monotonicity, and replica
convergence under faults. Its exact scope and latest reproducible runs are in
[docs/reliability.md](docs/reliability.md).

## License

AGPL-3.0. See [LICENSE](LICENSE).
