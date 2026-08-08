# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

> **The S3-native event log.** Your Kafka-compatible stream, an ISR-quorum durability
> story verified by a five-node Jepsen harness, and a self-managed Apache Iceberg lake —
> all on plain object storage. One binary. No broker disk fleet. No ZooKeeper. No ETL.

```text
 Kafka / HTTP producers
        │
        ├── classic ──▶ partition leader ──▶ ISR quorum ─────────▶ ack   (sub-ms)
        │                    └──▶ seal + publish segments (off hot path)
        │
        └── diskless ──▶ buffer ──▶ upload to S3 ──▶ metastore commit ──▶ ack
                                    │                 (memory · s3 · dynamodb)
                                    ▼
                             S3 / MinIO / R2 / B2   ◀── one durable data plane
                                │                  │
                                ▼                  ▼
                        Apache Iceberg        coordination state
                        (DuckDB · Trino)      (leases · ISR · groups)
```

Camu collapses the classic streaming stack — broker, archive, warehouse, and coordination
cluster — into **one durable data plane**: object storage. The log *is* the archive, the
archive *is* the analytics store, and even the coordination layer (leadership, ISR, consumer
groups) runs on object-store conditional writes instead of a consensus cluster.

- **One storage tier, not three.** No broker-disk fleet, no separate archival pipeline, no
  separate data warehouse. Immutable history lives once in S3-compatible storage, and
  everything reads from it.
- **Kafka-compatible, zero-copy.** Native `RecordBatch` bytes replicate unchanged and are
  served over the Kafka wire protocol — existing clients, CLIs, and tooling just work,
  alongside a first-class HTTP + SSE API.
- **Acknowledged durability you can trust.** Writes to replicated topics confirm only after
  the ISR quorum holds them, and the whole model is exercised under faults by a five-node
  Jepsen harness that runs **every day in CI**.
- **Analytics with no ETL.** Turn any topic into a self-managed Apache Iceberg table with a
  single flag, then query it with DuckDB, Trino, or Spark directly — no copy jobs, no
  warehouse to run.
- **Typed topics with a schema registry.** Define JSON, Avro, or Protobuf schemas, evolve
  them backward-compatibly, and route decode failures to a dead-letter topic.
- **Run it anywhere.** A single Go binary, no CGO, no JVM — [download a release](https://github.com/fatum/camu/releases)
  for macOS or Linux, or build from source. Pick `classic` mode for sub-millisecond local
  tails or `diskless` mode that needs **no local disks at all**; both speak the same APIs.

Camu is a focused system, not a drop-in for every Kafka feature. It is the right choice when
S3-native durability, simple replayable streams, and a queryable projection matter more than
the full Kafka ecosystem. The [API support matrix](docs/api-support-matrix.md) states exactly
what is supported, what is verified, and what is intentionally out of scope.

## Feature highlights

### A Kafka-compatible log with real durability semantics

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

### A coordination layer with no consensus cluster

Leadership, failover, and consumer groups run on **S3 conditional writes and leases**, not
ZooKeeper/etcd:

- A single controller holds a lease with monotonic epochs; renewals are CAS bumps, so a
  clock-skewed stale controller is fenced by epoch comparison, not wall-clock hope.
- Partition leaders publish assignments; caught-up ISR followers **self-promote** with a CAS
  bump on leader death — no controller round trip — and the controller is the backstop.
- Epoch history fences stale leaders and detects divergence; ISR mutations are guarded by
  the leader epoch so a fenced leader cannot acknowledge uncommitted writes.
- Kafka consumer-group state (members, generation, offsets) is S3-backed JSON with
  ETag-CAS versioning and controller-epoch stamping.
- Background work — retention, segment merge, Iceberg export, topic deletion — runs as
  **durable, resumable jobs** that survive restart and reassignment. Every job is idempotent:
  re-execution converges, never double-publishes, never corrupts state.

The design eliminates a whole class of infrastructure: no separate consensus cluster, no
meta-store to patch, no leader-election wiring to get wrong. One less system to fail.

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

## Two storage modes, one API

Both modes expose identical HTTP, SSE, and Kafka APIs — your producers and consumers don't
know (or care) which mode a topic uses. The difference is the data path and the
infrastructure each mode demands.

### `diskless` — no disks, no data-plane ops

**The headline mode.** Diskless topics write straight to object storage: records buffer in
memory (linger + batch bytes), flush to S3 as one object, then commit offsets and segment
refs in a metastore **after** the upload succeeds.

```
Produce ──▶ buffer ──▶ upload to S3 ──▶ metastore atomic commit (offsets + refs) ──▶ ack
                                                    │
                                    background compaction merges small segments
```

This **upload-then-allocate** order is the point: a failed upload leaves an orphan S3
object, never a visible offset hole. Background compaction merges small flushes into
64 MiB segments so reads stay efficient as history grows, and the S3 metastore archives old
compaction-sized refs so commits cost O(window), not O(all history).

**Pros.**

- **No local disks.** Nodes are stateless — run them on anything, with zero storage
  provisioning. Scale by adding instances, not disks.
- **Any node can serve any partition.** Durable state is in S3 + the metastore, so ownership
  is an assignment, not a pile of local data.
- **Upload-then-allocate** makes offset holes from failed uploads impossible.
- **Three metastore backends** to match your infrastructure (below), from zero extra services
  to fully managed.
- **Works behind any S3-compatible store** — MinIO, Cloudflare R2, Backblaze B2, AWS S3.

**Cons.**

- **Produce latency includes S3.** Each batch pays at least one S3 round trip (upload +
  commit), plus `linger_ms` of batching (default 250 ms).
- **Not replicated.** Diskless topics ignore `replication_factor` / `min_insync_replicas`;
  durability rests on S3 + the metastore commit, not ISR quorums.
- **Reads always hit S3.** No in-memory or local-disk read cache.

**Metastore backends** control commit latency and infrastructure:

| Backend | Commit latency | Infrastructure | Best for |
|---------|----------------|----------------|----------|
| `memory` | None (in-process counter) | None | Single-node development |
| `s3` | S3 conditional PUT (~1 round trip) | Same S3 bucket | Multi-node with zero extra services |
| `dynamodb` | Atomic DynamoDB transaction (~single-digit ms) | DynamoDB tables | Predictable latency at production throughput |

### `classic` — lowest latency, full replication

**The workhorse.** The partition leader appends to a local active segment — a sub-millisecond
ack — and a background flusher seals segments and publishes them to S3 off the hot path.
Replication (ISR quorum) is fully supported for `rf > 1`.

```
Produce ──▶ local active segment (sub-ms append) ──▶ replicate (rf>1) ──▶ ack
                                    │
                     background seal + publish to S3 (off hot path)
```

**Pros.**

- **Lowest produce latency** — local disk write, no S3 round trip on the hot path.
- **ISR-quorum replication** with the full Jepsen-verified durability matrix.
- **Hot reads served from memory / local disk**, not S3.
- **Flush is decoupled from produce** — object-store latency gates backpressure, not every ack.

**Cons.**

- **Requires local disk** — one active segment per owned partition; provisioning is tied to
  partition count and segment size.
- **Node failure loses the unflushed tail** (acks bound this window).
- **Disks must be managed per node**, in addition to object storage.

### Choosing a mode

| Concern | `classic` | `diskless` |
|---------|-----------|-----------|
| Produce latency | Sub-millisecond (local disk) | S3 round trip per batch (~tens of ms) |
| Local disk | Required | **Not required** |
| Replication (ISR) | Supported | Not supported |
| Read latency (hot data) | In-memory / local disk | S3 |
| Nodes | Disk-provisioned | Stateless |
| Object-store dependency | Async flush only | Synchronous produce path |
| Jepsen evidence | Full fault matrix | S3-metastore correctness via integration tests |

**Use `classic`** when you need the lowest latency, ISR-quorum durability, and sustained
high throughput — and are willing to provision disks.

**Use `diskless`** when you want stateless nodes, no disk fleet, and simpler operations —
and can accept S3-latency produce acks with linger batching.

See [Storage Modes](docs/storage-modes.md) for throughput characteristics, configuration,
and metastore tuning.

## Proven under faults — Jepsen

Camu's distributed behavior isn't asserted, it's **exercised**. A five-node Jepsen harness
(backed by MinIO) runs the full fault, read-mode, and recovery matrices **automatically
every day in CI**, and writes every run's `results.edn`, history, and node logs under
`jepsen/camu/store/`.

Faults injected against both the HTTP and Kafka APIs:

`kill` · `leader-kill` · `leader-pause-then-ack` · `partition` · `partition-ring` ·
`pause` · `membership` · `rejoin` · `s3-partition` · `clock-skew` · combined faults

Checkers run on every replicated run:

`committed-durability` · `truncation-safety` · `single-leader` / `no-split-brain` ·
`total-order` / `offset-monotonicity` · `hw-monotonicity` · `no-ghost-reads` ·
`replica-convergence`

What the evidence establishes:

- **Acknowledged writes survive** the fault matrix — every acked produce appears in the
  final partition drain after recovery.
- **No ghost reads** — consumers never observe data that was never acknowledged.
- **No split brain** — conflicting leadership is never observed; epoch fencing holds even
  when a paused leader resumes after lease expiry (`leader-pause-then-ack`).
- **Ordered, contiguous history** — offsets and the high watermark never duplicate, regress,
  or gap.

Two concrete, reproducible Kafka-API results: `leader-pause-then-ack` — **390 acked, 0
lost**; `leader-kill` — **337 acked, 0 lost, 0 missing** (the base branch loses 81 records
under the identical run). Availability stays in the **0.92–1.0** range even while nodes are
being killed, partitioned, paused, and clock-skewed.

This is verification in the open: the harness is [in-repo](jepsen/camu/README.md), the
artifacts are persisted, and the [reliability doc](docs/reliability.md) tells you exactly
what each checker establishes — and what it deliberately does not.

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

Create a diskless, typed, Iceberg-exported topic:

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"orders","partitions":4,"export_enabled":true,"storage_mode":"diskless",
    "schema":{"encoding":"json","fields":[{"name":"id","type":"int64","path":"$.id"},{"name":"sku","type":"string","path":"$.sku"}]}
  }'
```

For a replicated classic topic, set `replication_factor` and `min_insync_replicas` at
creation time. See the [API guide](docs/api.md) for request shapes, idempotent produce,
typed schemas, Iceberg export, and Kafka behavior.

## Documentation

- [Architecture](docs/architecture.md) — write/read paths, storage, and maintenance.
- [Coordination](docs/architecture/coordination.md) — leases, assignments, ISR, and failover.
- [Storage Modes](docs/storage-modes.md) — classic vs diskless: pros, cons, throughput, and configuration.
- [API guide](docs/api.md) and [API support matrix](docs/api-support-matrix.md).
- [Iceberg](docs/iceberg.md) — the export and query model.
- [Reliability](docs/reliability.md) — guarantees, limits, and Jepsen evidence.
- [Jepsen harness](jepsen/camu/README.md) — run commands and artifacts.

## License

AGPL-3.0. See [LICENSE](LICENSE).
