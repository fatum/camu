# API Guide

This document is the practical API overview for Camu.

Use it for:

- the main HTTP endpoints
- Kafka protocol notes
- storage-mode-specific creation behavior
- idempotent produce usage
- where to look next for exact support status

For the exact verified support surface, see [api-support-matrix.md](./api-support-matrix.md).

## Modes

Camu supports two storage modes:

- `classic`
- `diskless`

`classic` uses local active segments with immutable sealed segments in object storage.

`diskless` keeps the external API shape but makes the storage path object-store-centric.

Create mode over HTTP:

```json
{
  "name": "orders",
  "partitions": 4,
  "retention": "168h",
  "storage_mode": "diskless"
}
```

Create mode over Kafka:

- `camu.storage.mode=diskless`

## Query Mode

Camu also supports a separate runtime role:

- `server.mode=stream`
- `server.mode=query`

`stream` is the normal log-serving node.

`query` is a read-only SQL node:

- no topic creation or produce/consume surface
- no Kafka listener
- no registry/assignment participation
- exposes `POST /v1/sql`, `GET /v1/ready`, and `GET /v1/cluster/status`

When `server.auth_token` is configured, SQL requests require
`Authorization: Bearer <token>`; missing or invalid credentials return 401.
TLS is terminated by the deployment proxy and is not provided by Camu.

Query nodes require `sql.enabled: true`. They do not bind `internal_address`
or `kafka_port`; those settings can be omitted from a query-role configuration.
Use separate object-store credentials restricted to read-only access for
`parquet/`, `_meta/parquet_manifests/`, and `_meta/topics/`. The status endpoint
reports only that query node's local status; it is not cluster discovery.

Start from [`camu.query.yaml.example`](../camu.query.yaml.example) when
deploying the role.

## HTTP API

### Topic Admin

Create a topic:

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"orders",
    "partitions":4,
    "retention":"168h",
    "replication_factor":3,
    "min_insync_replicas":2
  }'
```

Read topics:

```bash
curl http://localhost:8080/v1/topics
curl http://localhost:8080/v1/topics/orders
```

Delete a topic:

```bash
curl -X DELETE http://localhost:8080/v1/topics/orders
```

Delete is asynchronous and resumable:

1. topic metadata becomes invisible immediately
2. background cleanup removes S3 data
3. for `diskless`, metastore refs are removed after the data path is gone
4. deletion markers are cleared last

### Produce

High-level produce:

```bash
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '[{"key":"user-123","value":"order placed","headers":{"trace-id":"abc"}}]'
```

Partition-specific produce:

```bash
curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '[{"value":"direct-partition-write"}]'
```

Notes:

- both endpoints are batch-shaped
- regular produce uses a JSON array even for one message
- the high-level endpoint routes by key
- requests on a non-owner are either proxied internally or rejected with routing information

Routing view:

```bash
curl http://localhost:8080/v1/topics/orders/routing
```

### Consume

Polling:

```bash
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=100"
```

SSE:

```bash
curl -N \
  -H 'Accept: text/event-stream' \
  "http://localhost:8080/v1/topics/orders/partitions/0/stream?offset=0"
```

Notes:

- polling returns JSON with `messages` and `next_offset`
- the endpoint accepts up to 20,000 messages, but returns at most 1,000 per
  response so the complete JSON page is read before the response begins
- SSE uses `id: {offset}`
- resume uses `Last-Event-ID + 1`

### Offsets

Consumer offsets:

```bash
curl -X POST http://localhost:8080/v1/topics/orders/offsets/consumer-a \
  -H 'Content-Type: application/json' \
  -d '{"offsets":{"0":42}}'

curl http://localhost:8080/v1/topics/orders/offsets/consumer-a
```

Group offsets:

```bash
curl -X POST http://localhost:8080/v1/groups/group-a/commit \
  -H 'Content-Type: application/json' \
  -d '{"offsets":{"orders":{"0":42}}}'

curl http://localhost:8080/v1/groups/group-a/offsets
```

### Producer Init

Allocate a producer ID:

```bash
curl -X POST http://localhost:8080/v1/producers/init
```

### SQL Query

Enable SQL first:

- `sql.enabled: true`

Optional operational bounds:

- `sql.cache_directory`
- `sql.duckdb_temp_directory`
- `sql.cache_max_size`
- `sql.duckdb_memory_limit`
- `sql.max_concurrency`
- `sql.query_timeout`
- `sql.max_scan_files`

Run a query:

```bash
curl -X POST http://localhost:8080/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{
    "sql":"select key, value from \"orders\" order by record_offset",
    "topics":["orders"],
    "limit":1000
  }'
```

Envelope fields:

- `sql`: required SQL text (single `SELECT` or `WITH` statement; no `;` inside)
- `topics`: required list of topic names that may be referenced
- `params`: optional bound parameters, positional. Numbers come through JSON as floats — cast to the target type inside the SQL (e.g. `WHERE record_offset = CAST(? AS BIGINT)`) rather than relying on implicit DuckDB coercion
- `time_range`: optional manifest bucket filter (`from`, `to` RFC 3339; end is inclusive at the bucket boundary, so `to=2026-04-11T23:00:00Z` includes the `hour=23` bucket through midnight)
- `limit`: optional server-side row cap. Default `1000`, max `10000`; values above max are rejected

Response shape:

```json
{
  "columns": [
    {"name": "key",   "type": "BLOB"},
    {"name": "value", "type": "BLOB"}
  ],
  "rows": [
    ["base64-of-key-bytes", "base64-of-value-bytes"]
  ]
}
```

- `columns[].type` is the DuckDB column type name.
- `rows` is a list of lists; column order matches `columns`.
- `BLOB` columns (including `key`/`value` on the default export schema) are base64-encoded JSON strings; decode client-side. JSON cannot represent raw bytes, so this is unavoidable on the wire.
- `NULL` is sent as JSON `null`.

Error codes:

- `400 Bad Request` — catch-all for validation and execution errors. Covers: missing required field, mutating statement, multi-statement SQL, invalid topic name, invalid `time_range`, `limit` above max, no published manifests for a requested topic in the time window (`no parquet data available for topic ...`), scan budget exceeded (`sql scan budget exceeded`), query timeout (`sql.query_timeout` expired), concurrency admission cancellation (`sql.max_concurrency` saturated and client disconnects while queued), and any DuckDB-level execution error. Inspect the response body's error string to disambiguate.
- `404 Not Found` — a requested topic does not exist in the topic store at all.

Note: timeout and concurrency saturation are not surfaced as distinct HTTP codes today; they collapse into 400 with a descriptive message. Future work may split these out.

Current behavior:

- read-only `SELECT` / `WITH` only; `copy`, `attach`, `install`, `load`, `create`, `alter`, `drop`, `insert`, `update`, `delete`, `export`, `call`, `pragma` are rejected (the validator strips quoted identifiers and string literals before scanning, so an identifier like `"events-export"` does not trip the filter)
- queries read only Parquet files referenced by published manifests — **eventually consistent with the log**, lagging produce by the export cadence. Freshly-produced records are not visible until the partition leader's next export pass seals a segment and publishes a new manifest
- SQL is topic-scoped; topic names must be declared in the request
- if no Parquet files are published for the requested topic/time window, the query is rejected
- BLOB columns are base64-encoded JSON strings (see above)
- defaults: `sql.query_timeout=30s`, `sql.max_scan_files=4096`, `sql.max_concurrency=4`, `sql.cache_max_size=5GiB`

Example with time window:

```bash
curl -X POST http://localhost:8080/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{
    "sql":"select count(*)::BIGINT as n from \"orders\"",
    "topics":["orders"],
    "time_range":{
      "from":"2026-04-11T00:00:00Z",
      "to":"2026-04-11T23:59:59Z"
    }
  }'
```

Parquet export notes:

- export is asynchronous and partition-leader-driven
- currently applies to `classic` topics
- opt-in per classic topic with `export_enabled: true`
- while enabled, classic retention waits until the durable Parquet pipeline checkpoint
  covers a sealed segment before deleting its native data; a missing, lagging,
  or unreadable checkpoint blocks cleanup rather than losing queryable data
- requires `unclean_leader_election=false` for every export-enabled classic topic.
  Disable unclean leader election before enabling export; startup fails for
  incompatible persisted topics.
- export object layout uses ingest-time buckets:
  - `parquet/dt=YYYY-MM-DD/topic={topic}/hour=HH/{file-id}.parquet`
- query visibility is manifest-driven through:
  - `_meta/parquet_manifests/...`

## Idempotent Produce

Idempotent produce is supported on the partition-specific endpoint:

```text
POST /v1/topics/{topic}/partitions/{id}/messages
```

Example:

```bash
curl -X POST http://localhost:8080/v1/producers/init
# {"producer_id":42}

curl -X POST http://localhost:8080/v1/topics/orders/partitions/0/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "producer_id":42,
    "sequence":0,
    "messages":[
      {"key":"u1","value":"hello"},
      {"key":"u2","value":"world"}
    ]
  }'
```

Current behavior:

- duplicate sequence: accepted as duplicate, not appended again
- sequence gap: rejected
- unknown producer with non-zero sequence: rejected
- high-level routed produce does not accept idempotent batch bodies

Operational rules:

- keep one sequence stream per `(producer_id, topic, partition)`
- retry the exact same batch body on resend
- use the partition-specific endpoint for idempotent flows

## Kafka Wire Protocol

## Typed topic schemas

Topics may be created with an immutable JSON schema:

```json
{"name":"orders","partitions":1,"schema":{"encoding":"json","fields":[{"name":"id","type":"int64","path":"$.id"}],"dead_letter_topic":"orders_dlq"}}
```

Supported field types are `string`, `int64`, `float64`, `bool`, and
`timestamp`; paths are simple `$.field`/`$.nested.field` selectors. HTTP
produces are validated before append. Kafka values remain opaque until export.
The per-partition Parquet consumer writes physical typed Parquet columns alongside offset, timestamp,
key, value, and headers. Decode failures are published to the configured raw
`dead_letter_topic` before the pipeline checkpoint advances; without one they
are explicitly skipped and logged. Schema fields and DLQ configuration cannot
be changed after topic creation. Kafka exposes the schema through the
`camu.schema` topic config.

Camu supports a Kafka protocol subset rather than full Kafka parity.

Main supported APIs include:

- `Produce`
- `Fetch`
- `Metadata`
- `ListOffsets`
- `OffsetCommit`
- `OffsetFetch`
- `FindCoordinator`
- `JoinGroup`
- `SyncGroup`
- `Heartbeat`
- `LeaveGroup`
- `DescribeGroups`
- `ListGroups`
- `DeleteGroups`
- `CreateTopics`
- `DeleteTopics`
- `CreatePartitions`
- `DescribeConfigs`
- `AlterConfigs`
- `IncrementalAlterConfigs`
- `DescribeCluster`
- `ApiVersions`
- `InitProducerID`
- ACL operations

Fetch returns native Kafka `RecordBatch` bytes. Camu clamps each partition's
`PartitionMaxBytes` to 16 MiB. Consumers should also set a total fetch budget;
the bundled benchmark uses 16 MiB per partition and 64 MiB per Fetch response.

Example with `kcat`:

```bash
echo "hello" | kcat -b localhost:9092 -t orders -P
kcat -b localhost:9092 -t orders -C -e
```

Kafka admin notes:

- `CreateTopics` supports `camu.storage.mode=diskless`
- `camu.storage.mode` is immutable after creation
- retention is time-based via `retention.ms`
- `retention.bytes` is unsupported
- `CreatePartitions` is expand-only

## Durability and Visibility Notes

Ack semantics:

- `rf=1`, `minISR=1`: durable in the local active segment on the owner, and the
  ack re-verifies ownership against the assignment store on an amortized
  `coordination.fence_interval` cadence (default `2s`)
- `rf>1`: durable on the leader and acknowledged only after ISR quorum confirms
  (HTTP and Kafka alike)
- Kafka `acks=0` requests are fire-and-forget and return without waiting for a
  quorum

Important behavior:

- sealing and object-store persistence are asynchronous and do not block produce
- a sealed segment remains available to follower replication while it is pending publication
- `rf=1` does not survive permanent loss of its only owner before object-store persistence
- reads are capped by readable high watermark for replicated topics
- for `diskless` topics, offsets are allocated before the object-store write (the
  RecordBatch base offset is patched before persistence); a transient PUT or
  segment-registration failure is retried idempotently so it does not strand
  allocated offsets as a permanent gap. Only a persistent failure that outlives
  the flush retry window surfaces an error to the producer

## Where To Check Exact Support

Use:

- [api-support-matrix.md](./api-support-matrix.md) for exact current support and verification status
- [architecture.md](./architecture.md) for system layout
- [architecture/coordination.md](./architecture/coordination.md) for leadership and failover model
