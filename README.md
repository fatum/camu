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
  This applies to both the HTTP API and the Kafka wire protocol (`acks=0`
  requests are fire-and-forget and return immediately).
- Segment sealing and S3 persistence happen in the background. A pending
  sealed segment remains available to follower replication until it is
  published.
- Object storage holds immutable segments and the coordination state; there is
  no separate metadata quorum.

| Produce configuration | Successful response means |
| --- | --- |
| `rf=1`, `minISR=1` | The owner appended the record to its local active segment, and re-verified its ownership against the assignment store (amortized by `coordination.fence_interval`, default `2s`). |
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

## Iceberg analytics

Set `export_enabled: true` on a topic (classic or diskless) and
`maintenance.parquet_export.iceberg: true` to project its committed records as
a self-managed Apache Iceberg table under `maintenance.parquet_export.warehouse`.
Export runs on the partition leader and does not delay produce. Point any
Iceberg engine (DuckDB `iceberg_scan`, Trino, Spark) at the warehouse path to
query the projection; see [Iceberg](docs/iceberg.md) for the pipeline, layout,
and retention rules.

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
- [Iceberg](docs/iceberg.md): export and query model.
- [Reliability](docs/reliability.md): guarantees, limits, and Jepsen evidence.
- [Jepsen harness](jepsen/camu/README.md): run commands and artifacts.

## Verification

Core distributed behavior is covered by integration tests and a five-node
Jepsen harness backed by MinIO. The harness checks acknowledged-write
durability, leader safety, ordering, high-watermark monotonicity, and replica
convergence under faults. The Jepsen matrix covers both the HTTP and the Kafka
wire-protocol API, including leader-kill, leader-pause-then-ack, partition,
pause, clock-skew, and object-store isolation faults; its exact scope and latest
reproducible runs are in [docs/reliability.md](docs/reliability.md).

The `dynamodb` diskless metastore is exercised against a real DynamoDB in CI
(`go test -tags dynamodb ./internal/diskless/` with `DYNAMODB_ENDPOINT`
pointing at DynamoDB Local).

## License

AGPL-3.0. See [LICENSE](LICENSE).
