# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

> **Camu is the S3-native event log that turns your stream into a queryable Iceberg lake.**
>
> Kafka-compatible produce/consume, an ISR quorum for acknowledged durability, and a
> self-managed Apache Iceberg projection — all on plain object storage. No broker disk
> fleet, no ZooKeeper, no CGO, no ETL pipeline.

```text
  Kafka / HTTP producers ──▶ partition leader ──▶ ISR quorum (acknowledged durability)
                                    │
                                    └──▶ sealed segments ──▶ S3 / MinIO ──▶ Apache Iceberg tables
                                                                                │
                                                                                ▼
                                                            DuckDB · Trino · Spark query directly
```

## Why Camu?

Applications need an ordered, replayable event log. Camu is the alternative that makes
object storage the shared durable data plane — so the log **is** the archive, and the
archive **is** the analytics store.

- **One storage tier, not three.** No broker-disk fleet, no separate archival pipeline,
  no separate data warehouse. Immutable history lives once in S3-compatible storage, and
  everything reads from it.
- **Kafka-compatible, zero-copy.** Native `RecordBatch` bytes replicate unchanged and are
  served over the Kafka wire protocol, so existing clients, CLIs, and tooling just work —
  alongside a first-class HTTP + SSE API.
- **Acknowledged durability you can trust.** Writes to replicated topics are confirmed
  only after the configured ISR quorum holds them, and the whole model is exercised under
  faults by a five-node Jepsen harness.
- **Analytics with no ETL.** Turn a topic into a self-managed Apache Iceberg table with a
  single flag, then query it with DuckDB, Trino, or Spark directly — no copy jobs, no
  warehouse.
- **Typed topics with a schema registry.** Define JSON, Avro, or Protobuf schemas, evolve
  them backward-compatibly, and route decode failures to a dead-letter topic.
- **Runs anywhere.** A single Go binary — [download a release](https://github.com/fatum/camu/releases)
  for macOS or Linux, or build from source. `classic` mode for local fast tails,
  `diskless` mode that needs no local disks at all; both speak the same APIs.

Camu is a focused system, not a drop-in for every Kafka feature. It is the right choice
when S3-native durability, simple replayable streams, and a queryable projection matter
more than the full Kafka ecosystem. See the
[API support matrix](docs/api-support-matrix.md) for exactly what is supported and what is
intentionally out of scope.

## Features

### An event log with real durability semantics

- Topics with partitions, replication factor, and `min_insync_replicas`.
- The partition leader appends native Kafka `RecordBatch` bytes; followers replicate them
  byte-for-byte, no re-encoding.
- A produce is acknowledged only after the high watermark passes the written offset — the
  ISR quorum has it. `acks=0` stays fire-and-forget, matching Kafka.
- Idempotent produce with producer IDs and per-partition sequences, so safe retries never
  duplicate records.
- Object storage holds immutable segments **and** the coordination state — no separate
  metadata quorum.

| Produce configuration | Successful response means |
| --- | --- |
| `rf=1`, `minISR=1` | The owner appended the record to its local active segment, and re-verified ownership against the assignment store. |
| `rf>1` | The leader and the configured ISR quorum have the record. |

### Iceberg analytics built in

Set `export_enabled: true` on any topic and Camu projects its committed history as a
self-managed Apache Iceberg table (v2 metadata, Avro manifests, `dt`/`hour` partitioning).
Point DuckDB, Trino, or Spark at the warehouse path and query it like any Iceberg table —
no in-process SQL engine, no export job to operate, and export never delays produce.

```sql
INSTALL iceberg; LOAD iceberg;
SELECT count(*), dt FROM iceberg_scan('s3://bucket/warehouse/events') GROUP BY dt;
```

See [Iceberg](docs/iceberg.md) for the pipeline, layout, and retention rules.

### Typed topics, evolved safely

Give a topic a schema — JSON, Avro, or Protobuf — and Camu validates produces, writes typed
Parquet columns, and carries the schema through an embedded registry with Confluent-style
ids so values decode against their own writer schema across versions. Add fields over time
with backward-compatibility checks; failed decodes go to a configured dead-letter topic.

### Two storage modes, one API

- **`classic`** — a local active tail on the partition owner plus immutable sealed segments
  in object storage.
- **`diskless`** — the object-store-centric path: no local disks, atomic offset allocation
  backed by an S3 head object or DynamoDB, background compaction that bounds the hot
  window regardless of history.

Both modes expose identical HTTP, SSE, and Kafka APIs. See
[Storage Modes](docs/storage-modes.md) for throughput characteristics,
configuration, and guidance on choosing between them.

## Quick start

### With Docker Compose

```bash
docker compose up
```

This starts MinIO, creates the bucket, and runs Camu — all configured.
Camu is available at `http://localhost:8080`.

### Manual

Start MinIO and create a bucket:

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/camu-data
```

Build Camu and configure the endpoint and credentials for MinIO.
Or download a [prebuilt binary](https://github.com/fatum/camu/releases) for your platform.

```bash
go build -o camu ./cmd/camu
cp camu.yaml.example camu.yaml
# Edit camu.yaml: set storage.endpoint to http://localhost:9000,
# storage.credentials.access_key to minioadmin,
# and storage.credentials.secret_key to minioadmin.
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

Create a typed, Iceberg-exported topic:

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"orders","partitions":4,"export_enabled":true,
    "schema":{"encoding":"json","fields":[{"name":"id","type":"int64","path":"$.id"},{"name":"sku","type":"string","path":"$.sku"}]}
  }'
```

For a replicated topic, set `replication_factor` and `min_insync_replicas` at creation time.
See the [API guide](docs/api.md) for request shapes, idempotent produce, typed schemas,
Iceberg export, and Kafka behavior.

## Documentation

- [Architecture](docs/architecture.md) — write/read paths, storage, and maintenance.
- [Coordination](docs/architecture/coordination.md) — leases, assignments, ISR, and failover.
- [Storage Modes](docs/storage-modes.md) — classic vs diskless: pros, cons, throughput, and configuration.
- [API guide](docs/api.md) and [API support matrix](docs/api-support-matrix.md).
- [Iceberg](docs/iceberg.md) — the export and query model.
- [Reliability](docs/reliability.md) — guarantees, limits, and Jepsen evidence.
- [Jepsen harness](jepsen/camu/README.md) — run commands and artifacts.

## Verification

Core distributed behavior is covered by integration tests and a five-node Jepsen harness
backed by MinIO. The harness checks acknowledged-write durability, leader safety, ordering,
high-watermark monotonicity, and replica convergence under faults — covering leader kills,
pauses, partitions, clock skew, and object-store isolation across both the HTTP and Kafka
APIs. The exact scope and latest reproducible runs are in
[docs/reliability.md](docs/reliability.md).

The `dynamodb` diskless metastore is exercised against a real DynamoDB in CI
(`go test -tags dynamodb ./internal/diskless/` with `DYNAMODB_ENDPOINT` pointing at
DynamoDB Local).

## License

AGPL-3.0. See [LICENSE](LICENSE).
