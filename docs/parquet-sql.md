# Parquet Export and SQL

Camu's native log is canonical. Parquet is an optional analytical projection
for committed records from `classic` topics; it is never part of a produce
acknowledgement.

## Export

Enable export when creating a classic topic with `export_enabled: true`.
The partition leader runs the exporter in the background:

1. Read a bounded range at or below the partition's committed high watermark.
2. Convert that range to a deterministic Parquet object.
3. Publish the corresponding manifest entry.
4. Publish the export checkpoint last.

If the process stops before the checkpoint is advanced, the same range is
retried. Parquet objects and manifest publication are idempotent for a source
range, so retries do not create a different logical result.

Schema validation failures can be written to the configured DLQ topic using
normal produce and replication semantics.

## Query

`POST /v1/sql` runs read-only `SELECT` or `WITH` queries through DuckDB. The
request must name the topics it is allowed to scan. Camu reads only Parquet
files referenced by published manifests; directory listing is not query
visibility.

Query execution is bounded by the SQL cache, memory, concurrency, timeout, and
maximum-scan-file settings. A `server.mode=query` node exposes only SQL,
readiness, and local status endpoints and can use read-only object-store
credentials.

## Retention and safety

For an export-enabled topic, classic retention does not remove a native sealed
segment until the export checkpoint covers that segment's end offset. This
avoids a permanent gap between the native log and the queryable projection.

Export-enabled topics require `unclean_leader_election=false`; the exporter
cannot safely reconcile divergent offset histories created by an unclean
election.

## API and configuration

See [api.md](api.md) for topic creation, SQL request examples, authentication,
and the query role configuration. See [architecture.md](architecture.md) for
the native segment and partition-leader model.
