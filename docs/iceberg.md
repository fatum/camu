# Iceberg Analytics

Camu's native log is canonical. The export pipeline is a background analytical projection
that writes committed records as a **self-managed Apache Iceberg table** — queryable by
DuckDB, Trino, Spark, or any other Iceberg engine — under the configured warehouse prefix.
Export is never part of a produce acknowledgement, and a query engine reads the table
directly through the object store.

## Enable

Create a topic with `export_enabled: true` (and `unclean_leader_election: false`; after the
fact, unclean leader election is refused on export-enabled topics). The export pipeline is
the only projection sink and is always Iceberg; `maintenance.parquet_export` only tunes it.

Each export-enabled topic becomes one Iceberg table at
`{maintenance.parquet_export.warehouse}/{topic}/`:

- `metadata/version-hint.text` and `metadata/{version}-{uuid}.metadata.json` hold the table
  metadata (schema, snapshots). The schema is derived from the topic's typed schema:
  `record_offset`, `record_timestamp`, `key`, `value`, `headers`, `dt`, and `hour`, plus one
  column per typed topic field. The table is partitioned by `identity(dt), identity(hour)`
  on the ingest time.
- `metadata/snap-*.avro` are manifest lists and `metadata/*-m*.avro` are manifests; together
  they reference the exported data files.
- `data/*.parquet` are immutable, content-addressed data files.

## Pipeline

The partition leader runs the exporter in the background:

1. Read a bounded range at or below the partition's committed high watermark.
2. Convert the range to a deterministic Parquet object and upload it.
3. Buffer ranges until `target_bytes` (default 64 MiB) or `max_interval` (default 30s) is
   reached, then commit one snapshot.
4. Publish the export checkpoint last.

If the process stops before the checkpoint is advanced, the same range is retried. Data
objects and snapshot commits are deterministic and idempotent for a source range, so retries
do not change the logical result. Manifest lists carry the parent snapshot's manifests
forward and are merged (minor compaction) when they would exceed the snapshot bound.

## Typed schemas and schema evolution

A topic's typed schema (JSON, Avro, or Protobuf encoding) defines both what the exporter
validates and the shape of the projected table. New schema versions are registered in the
embedded schema registry, checked for backward compatibility (fields are only added, never
removed or retyped; required fields may only relax to nullable), and the Iceberg table schema
evolves to match while **preserving stable column ids** — a reader that already knows a
column never sees it renumbered.

Avro and Protobuf values carry a Confluent-style schema-id envelope (`0x00` + 4-byte big
endian id), so the export decodes each value against its **own writer schema** rather than
the current projection — values written under older versions read correctly after evolution.
HTTP produce/consume uses base64 for the binary encodings; Kafka carries raw bytes.

See [api.md](api.md) for the schema API and encodings.

## Querying

Point any Iceberg engine at the warehouse path. For example with DuckDB:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('s3://bucket/warehouse/events');
```

Camu does not serve in-process SQL; query engines read the table directly through the
object store.

## Retention and safety

Retention does not remove native source data until the export checkpoint covers it (for
classic segments, `record_offset` coverage; for diskless, every referencing partition's
checkpoint). This avoids a permanent gap between the native log and the projection.

## Configuration

See [api.md](api.md) and `camu.yaml.example`. Relevant keys:

```yaml
maintenance:
  parquet_export:
    temp_directory: "/var/lib/camu/export-tmp"
    warehouse: "warehouse/"
    target_bytes: 67108864
    max_interval: "30s"
    max_records: 16384
    max_duration: "30s"
```
