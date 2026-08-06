# Iceberg Export

Camu's native log is canonical. The export pipeline is an optional analytical
projection that writes committed records as a self-managed Apache Iceberg table
(readable by DuckDB, Trino, Spark, and any other Iceberg engine) into the
configured warehouse prefix. Export is never part of a produce
acknowledgement.

## Enable

Create a topic with `export_enabled: true` and `unclean_leader_election: false`
(after the fact, unclean leader election is refused on export-enabled topics).
Set `maintenance.parquet_export.iceberg: true` to write Iceberg tables.

Each export-enabled topic becomes one Iceberg table at
`{maintenance.parquet_export.warehouse}/{topic}/`:

- `metadata/version-hint.text` and `metadata/{version}-{uuid}.metadata.json`
  hold the table metadata (schema, snapshots). The schema is derived from the
  topic's immutable typed schema: `record_offset`, `record_timestamp`, `key`,
  `value`, `headers`, plus one column per typed topic field.
- `metadata/snap-*.avro` are manifest lists and `metadata/*-m*.avro` are
  manifests; together they reference the exported data files.
- `data/*.parquet` are the immutable, content-addressed data files.

## Pipeline

The partition leader runs the exporter in the background:

1. Read a bounded range at or below the partition's committed high watermark.
2. Convert the range to a deterministic Parquet object and upload it.
3. Buffer ranges until `target_bytes` (default 64 MiB) or `max_interval`
   (default 30s) is reached, then commit one snapshot.
4. Publish the export checkpoint last.

If the process stops before the checkpoint is advanced, the same range is
retried. Data objects and snapshot commits are deterministic and idempotent for
a source range, so retries do not create a different logical result. Manifest
lists carry the parent snapshot's manifests forward and are merged (minor
compaction) when they would exceed `maxManifestsPerSnapshot`.

## Querying

Point any Iceberg engine at the warehouse path. For example with DuckDB:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('s3://bucket/warehouse/events');
```

Camu no longer serves in-process SQL; query engines read the table directly
through the object store.

## Retention and safety

Retention does not remove native source data until the export checkpoint
covers it (for classic segments, `record_offset` coverage; for diskless, every
referencing partition's checkpoint). This avoids a permanent gap between the
native log and the projection.

## Configuration

See [api.md](api.md) and `camu.yaml.example`. Relevant keys:

```yaml
maintenance:
  parquet_export:
    enabled: false
    temp_directory: "/var/lib/camu/export-tmp"
    iceberg: true
    warehouse: "warehouse/"
    target_bytes: 67108864
    max_interval: "30s"
```
