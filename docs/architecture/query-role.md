# ADR: Separate Stream, Query, and Export Roles

**Status:** Accepted design; implementation is staged.  
**Date:** 2026-07-30

## Decision

Camu continues to ship as one binary, with `all-in-one` as the backwards-compatible
default. The binary will also support separately deployed `stream` and `query`
roles. Parquet export remains a partition-leader responsibility in this stage, but
is expressed as a fenced job boundary so it can later move to an `export` worker
role without changing its durable contract.

## Roles and responsibilities

| Role | Owns | Must not run |
|---|---|---|
| Stream | HTTP/Kafka ingest and consume, partition leadership, replication, controller participation, and partition-local maintenance including Parquet export | DuckDB SQL serving when deployed as a dedicated stream node |
| Query | Read-only SQL over published Parquet manifests and files; local DuckDB and Parquet cache | Kafka, produce/consume/admin HTTP APIs, controller election, instance registration, partition ownership, replication, native-log maintenance, and Parquet export |
| Parquet consumer | Fenced per-partition consumer, Parquet/DLQ writes, and manifest publication | Client APIs, controller election, and direct mutation of native log state |
| All-in-one | The current combined behavior | Nothing beyond normal feature configuration |

`query` is a serving role, not a control-plane member. A stream partition leader
only exports committed, sealed data. A query node only exposes data made visible by
published manifests; it never discovers raw Parquet paths by listing a bucket.

## Storage permissions

Permissions are prefix-scoped and should use distinct credentials for each role.

| Role | Required object-store access |
|---|---|
| Stream | Read/write the current Camu data, metadata, and coordination prefixes required for assigned partitions and controller duties, including `segments/`, `_meta/`, `_coordination/`, and Parquet export objects/manifests when export is enabled |
| Query | Read-only `parquet/`, `_meta/parquet_manifests/`, and `_meta/topics/` (for query-scope validation); no access to native segments, partition state, checkpoints, assignments, ISR, epochs, instance records, or jobs |
| Parquet consumer | Read committed partition records; write `parquet/`, `_meta/parquet_manifests/`, and pipeline checkpoints; no assignment or ISR writes |

The exact policy must also constrain bucket, endpoint, and KMS permissions. A
query credential must not be reusable as a stream credential.

## Network and trust boundaries

| Role | Externally reachable listeners | Cluster-private listeners |
|---|---|---|
| Stream | HTTP and Kafka, subject to deployment policy | Replication/proxy/control traffic only |
| Query | SQL, health, and local status only | None required |
| Export (future) | Health and local status only | Job/control traffic only if introduced |

Internal replication/control traffic must be reachable only from trusted Camu
nodes. SQL access is a separate trust boundary: it should be independently
authenticated, authorized, rate-limited, and resource-bounded. TLS termination
and identity enforcement are deployment requirements; role separation does not
provide transport or request authentication by itself.

## Compatibility and migration

Existing deployments remain all-in-one unless they explicitly select a role.
The existing SQL and Parquet configuration remains valid in all-in-one mode.

To split a deployment, first retain export on stream nodes, deploy query nodes
with read-only credentials and `server.mode = "query"`, route SQL traffic to
them, and verify manifest-visible results match the existing endpoint. Only then
disable SQL serving on stream nodes. Rolling back consists of routing SQL traffic
back to all-in-one nodes; manifests and Parquet files are shared, immutable data.

## Consequences

- Query CPU, memory, cache pressure, and slow scans no longer compete with the
  write/replication path on dedicated deployments.
- Least-privilege object-store access becomes practical for query serving.
- Query-node cache correctness remains required: an active query must pin each
  local file until DuckDB has finished using it.
- The per-partition Parquet consumer preserves leader-epoch fencing, idempotent
  object identity, manifest CAS publication, and pipeline checkpoint recovery.

## Open questions

1. Should query nodes expose a cluster-derived status endpoint, or only local
   readiness to avoid granting metadata access?
2. What identity and TLS mechanism is mandatory for public SQL and internal
   stream traffic?
3. Should Parquet retention be coupled exactly to native retention before export
   work can be moved off partition leaders?
