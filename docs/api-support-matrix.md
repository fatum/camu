# API Support Matrix

This document is the canonical matrix for what the project currently provides,
what is verified, what is only partially verified, and what is still missing or
explicitly unsupported.

It is a status document, not a design document.

Use it for:

- deciding what can be documented as supported today
- identifying what still needs correctness work
- separating implemented behavior from well-verified behavior
- tracking classic vs `diskless` mode differences

## Status Legend

| Status | Meaning |
|---|---|
| `Verified` | Implemented and backed by direct unit and/or integration evidence for the current contract. |
| `Partial` | Implemented, but evidence is incomplete, narrow, or missing important negative/version/mode cases. |
| `Gap` | Behavior exists but is not yet proven well enough, or known edge cases are still missing. |
| `Unsupported` | Intentionally unsupported. This should return an explicit error or stay unadvertised. |

## Evidence Conventions

Evidence references below point to the main current proof points:

- integration tests under `test/integration`
- handler/server tests under `internal/server/*test.go`
- transport/dispatcher tests under `internal/server/kafka_*test.go`

## HTTP API Matrix

| Surface | Endpoint | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|---|
| Topic Admin | `POST /v1/topics` | Supported | Supported | `Partial` | `test/integration/topic_test.go`, `test/integration/diskless_test.go`, `internal/server/handlers_topic.go` | Invalid cross-mode config combinations are not fully matrixed. |
| Topic Admin | `GET /v1/topics` | Supported | Supported | `Verified` | `test/integration/topic_test.go` | Response shape now includes `storage_mode`. |
| Topic Admin | `GET /v1/topics/{topic}` | Supported | Supported | `Verified` | `test/integration/topic_test.go` | Response shape now includes `storage_mode`. |
| Topic Admin | `DELETE /v1/topics/{topic}` | Supported | Supported | `Verified` | `test/integration/topic_test.go`, `internal/server/topic_deletion_test.go`, `internal/server/server_test.go` | Delete now hides the topic immediately, persists a resumable deletion marker, deletes S3 data asynchronously, and only then clears diskless metastore state. |
| Health | `GET /v1/ready` | Supported | Supported | `Gap` | Existing handler path only | No explicit dedicated matrix row test. |
| Cluster | `GET /v1/cluster/status` | Supported | Supported | `Gap` | Existing code path only | No dedicated correctness matrix yet. |
| Routing | `GET /v1/topics/{topic}/routing` | Supported | Supported | `Gap` | Existing code path only | Needs explicit multi-node and diskless publication checks. |
| Produce | `POST /v1/topics/{topic}/messages` | Supported | Supported | `Verified` | `test/integration/diskless_test.go`, `test/integration/consume_parity_test.go` | High-level parity is in good shape. |
| Produce | `POST /v1/topics/{topic}/partitions/{id}/messages` | Supported | Supported | `Verified` | `test/integration/diskless_test.go`, `internal/diskless/idempotency.go`, `internal/diskless/metastore_memory_test.go`, `internal/diskless/metastore_s3_test.go`, `internal/diskless/metastore_dynamo_test.go`, `internal/server/handlers_produce.go` | Duplicate, gap, and out-of-order rejection is verified per metastore; the DynamoDB path runs against a real DynamoDB in CI. Remaining gap is full end-to-end Kafka idempotent parity. |
| Producer Init | `POST /v1/producers/init` | Supported | Supported API surface | `Gap` | server tests only | End-to-end usefulness for diskless idempotent flows still needs explicit proof. |
| Consume | `GET /v1/topics/{topic}/partitions/{id}/messages` | Supported | Supported | `Verified` | `test/integration/consume_parity_test.go`, `test/integration/diskless_test.go`, `internal/server/server_test.go` | Bounded read, empty-topic, and beyond-end parity are covered. |
| Stream | `GET /v1/topics/{topic}/partitions/{id}/stream` | Supported | Supported | `Verified` | `test/integration/consume_parity_test.go`, `test/integration/diskless_test.go` | Idle flush and resume semantics are now covered. |
| Consumer Offsets | `POST /v1/topics/{topic}/offsets/{consumer_id}` | Supported | Supported | `Verified` | `test/integration/offsets_parity_test.go`, `internal/server/server_test.go` | Invalid-body handler checks exist. |
| Consumer Offsets | `GET /v1/topics/{topic}/offsets/{consumer_id}` | Supported | Supported | `Verified` | `test/integration/offsets_parity_test.go` | Good parity coverage. |
| Group Offsets | `POST /v1/groups/{group_id}/commit` | Supported | Supported | `Verified` | `test/integration/offsets_parity_test.go`, `internal/server/server_test.go` | Invalid-body handler checks exist. |
| Group Offsets | `GET /v1/groups/{group_id}/offsets` | Supported | Supported | `Verified` | `test/integration/offsets_parity_test.go` | Good parity coverage. |

## Kafka API Matrix

### Transport and Negotiation

| API / Area | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `ApiVersions` key advertisement | Supported | Supported | `Verified` | `internal/server/kafka_api_test.go` | API presence and advertised min/max version ranges are tested. |
| Request framing / decode / encode | Supported | Supported | `Partial` | `internal/server/kafka_transport_test.go` | Unsupported API keys, invalid and oversized frame length, truncated flexible requests, multiple valid requests on one connection, malformed second-request close behavior, negative and future `Metadata` versions, high known request versions, response-version clamping, and flexible vs non-flexible response-header tagging are covered. Remaining gap is mainly broader per-API version-surface exhaustiveness rather than basic framing correctness. |
| Connection close on decode/handle failure | Supported | Supported | `Verified` | `internal/server/kafka_transport_test.go` | Basic failure handling is covered. |

### Data APIs

| API | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `Produce` | Supported | Supported | `Partial` | `test/integration/kafka_basic_test.go`, `test/integration/diskless_kafka_test.go`, `internal/server/kafka_transport_test.go` | `acks=0` is fire-and-forget; `acks=1`/`all` wait for the ISR quorum before acknowledging, matching the HTTP path. Diskless compressed produce is covered; diskless idempotent produce runs the same atomic sequence validation as the HTTP path, but end-to-end Kafka idempotent semantics are still open. |
| `Fetch` | Supported | Supported | `Verified` | `test/integration/kafka_basic_test.go`, `test/integration/diskless_kafka_test.go`, `internal/server/kafka_roundtrip_test.go`, transport tests | Empty fetch watermark behavior is covered in both paths. |
| `InitProducerID` | Supported | Supported API surface | `Partial` | `internal/server/kafka_api_test.go`, `test/integration/kafka_basic_test.go` | Direct integration now covers non-transactional allocation and transactional rejection; remaining gap is broader diskless/idempotent end-to-end semantics. |

### Metadata and Discovery

| API | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `Metadata` | Supported | Supported | `Verified` | `test/integration/kafka_leadership_test.go`, `test/integration/diskless_kafka_test.go`, `internal/server/kafka_transport_test.go`, `internal/server/server_test.go` | Missing-topic behavior is explicitly covered. |
| `ListOffsets` earliest/latest | Supported | Supported | `Verified` | `test/integration/kafka_basic_test.go`, `test/integration/diskless_kafka_test.go`, `internal/server/server_test.go` | Good current coverage. |
| `ListOffsets` timestamp lookup | Supported | `Unsupported` except explicit invalid request | `Partial` | `test/integration/kafka_basic_test.go`, `test/integration/diskless_kafka_test.go`, `internal/server/server_test.go` | Classic timestamp lookup is covered; diskless unsupported path is now covered across legacy and newer request versions, but broader per-version exhaustiveness is still thin. |
| `FindCoordinator` | Supported | Supported | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_api_test.go` | Current contract is controller-based routing. |

### Group Coordinator APIs

| API | Classic-backed topics | Diskless topics | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `JoinGroup` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_groups_test.go` | Unknown-member rejection is covered. |
| `SyncGroup` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_groups_test.go` | Leader-only assignment and assignment completeness are covered. |
| `Heartbeat` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_groups_test.go` | Failover heartbeat survival is covered. |
| `LeaveGroup` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_groups_test.go` | Rebalance-on-removal is covered. |
| `DescribeGroups` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go` | Basic introspection, single-member transitions, and a two-member rebalance from `PreparingRebalance` to `Stable` are covered. |
| `ListGroups` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go` | Basic listing plus positive and negative state/type filter behavior are covered. |
| `DeleteGroups` | Supported | Expected same | `Verified` | `test/integration/kafka_group_test.go` | Empty, non-empty, missing, and non-coordinator cases are covered. |

### Offset APIs

| API | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `OffsetCommit` | Supported | Supported | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_api_test.go` | Good coordinator-path coverage. |
| `OffsetFetch` | Supported | Supported | `Verified` | `test/integration/kafka_group_test.go`, `internal/server/kafka_api_test.go` | Includes failover coverage. |
| `OffsetDelete` | Supported | Supported | `Verified` | `test/integration/kafka_group_test.go` | Removal semantics are covered. |

### Admin APIs

| API | Classic | Diskless | Status | Main Evidence | Current Gaps / Notes |
|---|---|---|---|---|---|
| `CreateTopics` | Supported | Supported via `camu.storage.mode=diskless` | `Verified` | `test/integration/kafka_basic_test.go`, `internal/server/server_test.go` | Validate-only and controller-only behavior are covered. |
| `DeleteTopics` | Supported | Supported | `Verified` | `test/integration/kafka_basic_test.go`, follower-controller tests, `internal/server/topic_deletion_test.go`, `internal/server/server_test.go` | Delete is now async and resumable: topic metadata is removed immediately, cleanup resumes from a marker, S3 topic data is deleted first, and diskless metastore cleanup happens only after the data path is gone. |
| `CreatePartitions` | Supported expand-only | Supported expand-only | `Verified` | `test/integration/kafka_basic_test.go`, `internal/server/kafka_api_test.go` | Increase, no-shrink, readiness, validate-only, and manual replica assignment rejection are covered. |
| `DescribeConfigs` | Supported topic configs | Supported topic configs | `Partial` | `test/integration/kafka_basic_test.go`, `internal/server/kafka_api_test.go` | Supported configs and unknown requested config names are covered; remaining gap is broader exhaustiveness rather than missing negative-path proof. |
| `AlterConfigs` | Supported topic configs | Supported topic configs with diskless immutability constraints | `Partial` | `test/integration/kafka_basic_test.go`, `internal/server/kafka_api_test.go` | Resource-type rejection, validate-only, retention/storage-mode constraints, and unsupported config-name no-mutation behavior are covered; remaining gap is broader duplicate/exhaustiveness coverage. |
| `IncrementalAlterConfigs` | Supported topic configs | Supported topic configs with diskless immutability constraints | `Partial` | `test/integration/kafka_basic_test.go`, `internal/server/kafka_api_test.go` | Same remaining gap as `AlterConfigs`; unsupported config-name rejection without partial mutation is now covered. |
| `DescribeCluster` | Supported | Supported | `Verified` | `test/integration/kafka_basic_test.go`, `internal/server/kafka_api_test.go` | Good current coverage. |
| `CreateACLs` | Supported subset | Supported subset | `Partial` | `test/integration/kafka_basic_test.go`, `test/integration/kafka_leadership_test.go`, `internal/server/kafka_api_test.go` | Topic, group, and cluster resource creation are covered for the current subset; remaining gap is a broader resource/operation matrix. |
| `DescribeACLs` | Supported subset | Supported subset | `Partial` | same as above | Basic behavior, invalid input, `MATCH` filter behavior, and resource/operation filtering for topic, group, and cluster ACLs are covered; broader matrix coverage is still thin. |
| `DeleteACLs` | Supported subset | Supported subset | `Partial` | same as above | Basic behavior, follower `NOT_CONTROLLER`, `MATCH` filter deletion, and exact group-resource deletion are covered; broader matrix coverage is still thin. |

## Explicitly Unsupported or Constrained Kafka Behavior

These behaviors should be treated as unsupported until they are explicitly
implemented and tested:

| Behavior | Current Status | Evidence / Notes |
|---|---|---|
| Diskless Kafka timestamp lookup by arbitrary timestamp | `Unsupported` | Covered as `INVALID_REQUEST` in `test/integration/diskless_kafka_test.go` and `internal/server/server_test.go`. |
| `retention.bytes` topic config | `Unsupported` | Explicitly rejected; covered in server and integration tests. Time-based retention cleanup is async/resumable in both modes and now runs through partition-leader durable jobs, including resume after restart and after reassignment. Diskless cleanup remains conservative at the backing-file level. |
| Storage mode mutation after topic creation | `Unsupported` | Explicitly rejected for `AlterConfigs` and `IncrementalAlterConfigs`. |
| Transactional `InitProducerID` semantics | `Unsupported` / not proven | Non-transactional path exists; transactional coverage is not part of the current verified surface. |
| Unsupported Kafka API keys / versions negative-path matrix | Partially proven | Direct transport-level tests now cover unsupported API keys, invalid and oversized frame lengths, truncated flexible requests, reused-connection corruption, and several version-edge behaviors, but not a broad per-API version matrix. |

## Highest-Value Remaining Gaps

If the goal is to close the biggest remaining documentation and correctness
holes, do them in this order:

1. Broader per-API Kafka version-surface coverage
2. Broader config-admin duplicate/exhaustiveness matrix
3. Broader ACL resource/operation/filter matrix outside the currently proven subset
4. Additional `ListOffsets` per-version/client exhaustiveness
5. Broader Kafka admin edge-case matrix for mixed valid/invalid inputs
