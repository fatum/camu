# Internal sink pipeline contract

The `internal/pipeline` package is the common foundation for durable record
processing. A pipeline reads only records at or below the source partition's
readable high watermark, writes a bounded batch to a sink, waits for sink
durability, and publishes the source checkpoint last.

This ordering provides at-least-once processing. If a process stops after the
sink write but before the checkpoint, the batch is retried. Sink writes must
therefore be deterministic and idempotent. Checkpoints are generation fenced
and owner epoch fenced, so a former partition leader cannot advance progress.

The current implementation uses these contracts for two paths:

- schema-failure DLQ delivery, which preserves raw bytes and uses normal topic
  produce/replication semantics; and
- Parquet export, which reads committed ranges, writes deterministic Parquet
  objects and manifests, then publishes the source checkpoint.

Future materialized-topic or Iceberg sinks must keep the same
output-before-checkpoint rule and provide their own durable commit or
replication acknowledgement.
