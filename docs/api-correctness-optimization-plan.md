# API Correctness and Optimization Plan Across Classic and Diskless Modes

> Working document for execution. Update this file as APIs are verified, optimized, or cleaned up. Do not treat it as aspirational design only.

## Goal

Establish one source of truth for:

1. Expected behavior for every public API in both `classic` and `diskless` modes.
2. Confirmed behavior backed by tests and direct verification.
3. Optimization work that is safe to apply only after correctness is pinned down.
4. Mandatory cleanup of dead or superseded code from previous iterations.

## Rules

### Rule 1: Correctness before optimization

No API optimization is complete until the expected behavior is written down and verified in both modes, or explicitly marked unsupported in one mode.

### Rule 2: Expected vs confirmed must stay separate

Every API row must have:

- `Expected`: what the system should do
- `Confirmed`: what is currently proven by tests or direct validation

If they differ, treat it as a bug or a missing test, not as an acceptable ambiguity.

### Rule 3: Remove obsolete code during each iteration

Every API change must include a cleanup pass for:

- unused handlers
- unused helper functions
- superseded routing branches
- old fallback paths from earlier iterations
- tests that only validate removed behavior
- dead config flags, comments, and TODOs that no longer describe reality

If old code is intentionally retained, the PR/commit must state why it is still needed.

### Rule 4: Unsupported is acceptable only when explicit

If an API or mode combination is intentionally unsupported, it must be:

- documented here
- encoded as an explicit error
- covered by a test

Silent partial behavior is not acceptable.

### Rule 5: Actualize documentation after every run

After each execution run, this document must be updated to reflect reality.

Minimum required updates after every run:

- change `Confirmed`, `Gap`, or `Unsupported` status where evidence changed
- add the tests or verification evidence that ran
- record newly discovered mismatches
- remove claims that are no longer true
- update cleanup status when old code was removed

Do not defer doc updates to the end of a larger initiative. This file must stay current run by run.

## Scope

This plan covers both public API layers:

- HTTP API under `/v1/...`
- Kafka protocol API served by the broker port

This plan covers both storage modes:

- `classic`
- `diskless`

## Execution Order

Run the work in this order:

1. Lock down API inventory and expected behavior.
2. Add or repair missing correctness tests.
3. Fix mismatches between expected and actual behavior.
4. Remove obsolete code that earlier iterations left behind.
5. Optimize hot paths only after the behavior matrix is green.
6. Re-run focused correctness checks after each optimization.
7. Actualize this document after every run before considering the iteration complete.

## Status Definitions

- `Expected`: behavior defined and agreed
- `Confirmed`: verified by unit/integration tests or targeted manual validation
- `Gap`: known mismatch, ambiguity, or missing proof
- `Unsupported`: intentionally not available; must return explicit error

## Verification Standards

### Unit coverage

Use unit tests for:

- request validation
- mode dispatch
- response shapes
- error mapping
- coordinator and routing invariants

### Integration coverage

Use integration tests for:

- end-to-end HTTP flows
- end-to-end Kafka flows
- cross-node routing and leadership behavior
- diskless flush/read visibility
- metadata and offset semantics as observed by real clients

### Required evidence to mark `Confirmed`

At least one of:

- direct unit test for the handler/helper
- integration test covering client-visible behavior
- both, for coordinator logic, routing, or mode-specific behavior

## API Matrix

Update `Confirmed` and `Evidence` as work progresses.

Canonical status matrix:

- `docs/api-support-matrix.md` is the primary document for current provided, partial, missing, and unsupported API behavior.
- This plan remains the execution document and run ledger.

| API Surface | Endpoint / API | Classic Expected | Diskless Expected | Confirmed | Evidence | Notes / Gaps |
|---|---|---|---|---|---|---|
| HTTP Topic Admin | `POST /v1/topics` | Create classic topic with replication rules | Create diskless topic with diskless constraints | Partial | Existing topic handler tests, diskless integration create helpers | Need explicit matrix row coverage for invalid cross-mode config combinations |
| HTTP Topic Admin | `GET /v1/topics`, `GET /v1/topics/{topic}` | Return classic topic config | Return diskless topic config including `storage_mode` | Partial | Existing handler tests, Kafka admin + HTTP verification for diskless topic reads, direct integration coverage for list/get response shape | List/get response shape is now directly verified; handler-level response tests could still be added later |
| HTTP Topic Admin | `DELETE /v1/topics/{topic}` | Delete topic, local and S3 metadata cleanup | Delete topic plus MetaStore cleanup | Partial | Existing handler tests | Need explicit diskless deletion verification and dead-cleanup audit |
| HTTP Produce | `POST /v1/topics/{topic}/messages` | High-level produce, routed by ownership | High-level produce, diskless path with no local ownership dependency | Partial | Existing server tests, `test/integration/diskless_test.go` | Need exact parity table for validation, offsets, and errors |
| HTTP Produce | `POST /v1/topics/{topic}/partitions/{id}/messages` | Partition-specific produce, idempotency supported | Partition-specific produce via diskless raw batch path where applicable | Partial | Existing server tests, diskless integration | Need explicit unsupported/allowed behavior rules for idempotent + diskless combinations |
| HTTP Consume | `GET /v1/topics/{topic}/partitions/{id}/messages` | Read visible data using classic HW semantics | Read visible data using MetaStore-backed diskless visibility semantics | Partial | Existing server tests, `test/integration/diskless_test.go` | Need parity checks for empty reads, offsets beyond end, and error mapping |
| HTTP Stream | `GET /v1/topics/{topic}/partitions/{id}/stream` | Stream visible classic messages | Stream visible diskless messages with same client contract or explicit limitation | Gap | Existing SSE tests are classic-oriented | Need explicit diskless support decision and tests |
| HTTP Consumer Offsets | `POST/GET /v1/topics/{topic}/offsets/{consumer_id}` | Commit/fetch standalone consumer offsets | Same semantics | Gap | Existing offsets code, limited coverage | Need direct mode-independent verification |
| HTTP Group Offsets | `POST /v1/groups/{group_id}/commit`, `GET /v1/groups/{group_id}/offsets` | Commit/fetch group offsets | Same semantics | Gap | Existing offsets code, some Kafka-side coverage | Need HTTP-side parity verification |
| HTTP Producer Init | `POST /v1/producers/init` | Allocate producer ID | Same API contract unless explicitly unsupported for diskless-specific paths | Gap | Existing server tests | Need document saying whether diskless idempotent HTTP path is supported end-to-end |
| Kafka Data | Produce | Accept Kafka RecordBatch writes, correct offset/error semantics | Accept writes with diskless dispatch where supported | Partial | `kafka_basic_test.go`, `diskless_kafka_test.go`, transport tests | Need matrix for compression, idempotency, unsupported combinations |
| Kafka Data | Fetch | Correct visible records, HW/LSO/error semantics | Correct visible records from diskless storage | Partial | Kafka integration tests, transport tests | Need explicit parity verification for empty fetch and offset boundaries |
| Kafka Admin | Metadata | Return brokers, partitions, leaders, explicit unknown topic errors | Same, reflecting diskless topics too | Partial | Kafka leadership tests, unit tests | Need final parity checklist for missing topics and mode-specific partition metadata |
| Kafka Admin | ListOffsets | Earliest/latest/timestamp lookup with explicit errors | Earliest/latest supported; timestamp lookup only if explicitly supported, otherwise explicit error | Partial | Server tests, transport tests | Diskless timestamp path now explicit error; still need broader client-facing verification |
| Kafka Groups | FindCoordinator | Stable coordinator routing | Same | Partial | Kafka group integration tests | Need version-matrix verification |
| Kafka Groups | JoinGroup / SyncGroup / Heartbeat / LeaveGroup | Correct coordinator semantics | Same | Partial | Unit regressions plus integration tests | Need full rebalance-state matrix and failover confirmation audit |
| Kafka Offsets | OffsetCommit / OffsetFetch / OffsetDelete | Correct coordinator and partition error semantics | Same | Partial | Integration tests, recent coordinator fixes | Need explicit retention/unsupported behavior notes if any |
| Kafka Admin | Create/DeleteTopics, CreatePartitions, Describe/AlterConfigs, ACLs, DescribeCluster | Correct admin semantics for supported subset | Same, with explicit `camu.storage.mode` create-time config and immutable diskless mode | Partial | Existing API tests, integration coverage for controller-only mutations, diskless create/describe, ACL invalid/follower paths, and config resource/immutability errors | Remaining unsupported-path spot checks are now narrow |

## HTTP API Inventory

This section is the first-pass concrete inventory derived from `internal/server/routes.go` and the current handlers.

### Topic Admin

| Endpoint | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `POST /v1/topics` | Creates a topic and returns `201` with topic config. Rejects invalid JSON with `400`. Rejects invalid `name`, `partitions`, `retention`, replication settings, and `storage_mode` with `400`. Returns `409` if topic already exists. | `classic` is default. `diskless` is explicit via `storage_mode`. Diskless topic creation still shares topic-store admin flow. | Expected |
| `GET /v1/topics` | Returns `200` with all topic configs. | Should list both classic and diskless topics uniformly. | Expected |
| `GET /v1/topics/{topic}` | Returns `200` with topic config. Returns `404` if missing. | No mode-specific response difference other than stored config. | Expected |
| `DELETE /v1/topics/{topic}` | Deletes topic and returns success or `404` if missing. | Diskless delete must also clear diskless MetaStore state. | Gap |

### Health and Routing

| Endpoint | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `GET /v1/ready` | Readiness probe. | Mode-independent. | Gap |
| `GET /v1/cluster/status` | Returns cluster/controller state. | Mode-independent. | Gap |
| `GET /v1/topics/{topic}/routing` | Returns routing or ownership view for topic partitions. | Must stay meaningful for classic leadership and diskless topic assignment publication. | Gap |

### HTTP Produce

| Endpoint | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `POST /v1/topics/{topic}/messages` | High-level produce. Accepts JSON array only. Returns `400` on invalid body or empty array. Returns `404` if topic missing. Returns `503` during shutdown or backpressure. Returns ordered offsets in request order. | In `classic`, routes by key and ownership. In `diskless`, uses ephemeral router and diskless append path. Idempotent batch body is explicitly rejected here. | Expected |
| `POST /v1/topics/{topic}/partitions/{id}/messages` | Partition-specific produce. Accepts JSON array or idempotent batch object. Returns `400` for invalid partition/body/empty batch, `404` for missing topic, `422` for idempotency sequence errors, `503` for shutdown/backpressure/not-ready replicated writes. Returns offsets for successful append. | In `classic`, uses partition ownership and optional replication wait. In `diskless`, still accepts partition-specific writes, but exact idempotent support must be explicitly verified row-by-row. | Gap |
| `POST /v1/producers/init` | Allocates producer ID and returns `201`. Returns `503` during shutdown. | API is mode-independent, but end-to-end usefulness differs if some diskless idempotent flows remain unsupported. | Gap |

### HTTP Consume and Stream

| Endpoint | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `GET /v1/topics/{topic}/partitions/{id}/messages` | Low-level consume. Validates partition, offset, and limit. Returns `400` on invalid params. Returns messages plus `next_offset`. Classic path caps reads at readable HW. | In `diskless`, fetches raw batches from diskless engine, decodes them, and returns JSON messages. Need explicit parity verification for empty result, beyond-end result, and not-found behavior. | Gap |
| `GET /v1/topics/{topic}/partitions/{id}/stream` | SSE stream of messages from offset or `Last-Event-ID`. Returns `400` on invalid params and `500` if flusher unsupported. | Diskless has its own polling loop. Need explicit statement whether stream semantics match classic or only approximate them. | Gap |

### HTTP Offsets

| Endpoint | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `POST /v1/topics/{topic}/offsets/{consumer_id}` | Commits standalone consumer offsets. Returns `400` on invalid body and `200` on success. | Should be mode-independent because offsets are not storage-mode-specific. | Gap |
| `GET /v1/topics/{topic}/offsets/{consumer_id}` | Returns standalone consumer offsets. | Should be mode-independent. | Gap |
| `POST /v1/groups/{group_id}/commit` | Commits group offsets via HTTP JSON map. Returns `400` on invalid body and `200` on success. | Should be mode-independent. | Gap |
| `GET /v1/groups/{group_id}/offsets` | Returns committed group offsets. | Should be mode-independent. | Gap |

## HTTP Expected Behavior Notes

### High-level produce

- Requires JSON array input only.
- Rejects idempotent batch body on purpose.
- Preserves request order in returned offsets even when the server internally groups by partition.

### Partition-specific produce

- Is the only HTTP endpoint that accepts idempotent batch bodies.
- Must explicitly document and test whether idempotent semantics are identical in classic and diskless modes.

### Consume

- `limit` is a message-count parameter at the HTTP layer.
- Current diskless path converts `limit` into a byte fetch budget (`limit * 1024`) before decoding. This needs explicit confirmation or refinement because it is not obviously equivalent to classic behavior.

### Offsets

- The current HTTP offset handlers are storage-mode-agnostic.
- This is a place where we should prefer shared tests that run once for classic and once for diskless topic setups, to prove invariance.

## Kafka API Inventory

This section is the first-pass concrete inventory derived from `internal/server/kafka_wire.go`, `kafka_handlers_admin.go`, `kafka_handlers_data.go`, and the server-backed Kafka handler functions.

### Kafka Transport and Negotiation

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `ApiVersions` | Advertises the supported Kafka API subset and version ranges. Unsupported APIs are not advertised. | Mode-independent. | Partial |
| Request framing / decode / encode | Accept Kafka request frames, decode supported APIs, and return correlation-matched responses. Invalid frame sizes or undecodable requests terminate the connection. | Mode-independent. | Gap |

### Kafka Data APIs

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `Produce` | Accepts Kafka `RecordBatch` writes and returns per-partition error code plus base offset. Should preserve partition error semantics (`UNKNOWN_TOPIC_OR_PARTITION`, `NOT_LEADER`, sequence errors, invalid requests, etc.). | In `classic`, write path depends on owned partition leadership. In `diskless`, raw batch path is used and should not depend on classic local ownership. | Partial |
| `Fetch` | Returns record batches, HW, and LSO for readable data. Empty fetches must still return a non-null records field where required by flexible versions. | In `classic`, visibility depends on readable HW / local storage. In `diskless`, visibility comes from diskless engine fetch path. | Partial |
| `InitProducerID` | Allocates producer IDs for non-transactional idempotent produce. Transactional IDs are rejected with invalid request. | Mode-independent API surface; end-to-end produce parity still needs explicit confirmation for diskless paths. | Partial |

### Kafka Metadata and Discovery

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `Metadata` | Returns brokers, controller, topic partitions, replicas, ISR, and explicit unknown-topic responses for requested missing topics. | Must represent both classic and diskless topics as normal Kafka topics at the metadata layer. | Partial |
| `ListOffsets` | Supports earliest / latest / max-timestamp and timestamp lookup where implemented. Returns explicit errors for invalid topic/partition or unsupported diskless timestamp lookup. | In `classic`, timestamp lookup is implemented. In `diskless`, earliest/latest are supported and timestamp lookup is explicitly invalid request. | Partial |
| `FindCoordinator` | Returns the active coordinator broker for group/offset APIs. | Current implementation is controller-based, not hash-per-group. This must be documented as intended behavior until changed. | Partial |

### Kafka Group Coordinator APIs

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `JoinGroup` | Only the coordinator accepts joins. Validates protocol compatibility, allocates member IDs when needed, rejects stale unknown member IDs, and advances generation on membership changes. | Group coordinator semantics should be independent of topic storage mode. | Partial |
| `SyncGroup` | Only the coordinator accepts sync. Only the leader may install assignments, assignments must match current membership, and members receive their assignment or rebalance error. | Storage-mode-independent coordinator logic. | Partial |
| `Heartbeat` | Only the coordinator accepts heartbeats. Validates generation/member and keeps membership alive. | Storage-mode-independent. | Partial |
| `LeaveGroup` | Only the coordinator accepts leave. Any actual membership removal triggers rebalance state reset. | Storage-mode-independent. | Partial |
| `DescribeGroups` | Returns group state, members, protocol, and assignments or `NOT_COORDINATOR` when queried on non-coordinator nodes. | Storage-mode-independent. | Partial |
| `ListGroups` | Returns known groups with state and type filters. | Storage-mode-independent. | Gap |
| `DeleteGroups` | Deletes empty groups and their committed offsets; rejects non-empty groups and non-coordinator calls. | Storage-mode-independent. | Partial |

### Kafka Offset APIs

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `OffsetCommit` | Only the coordinator accepts commits. Validates topic/partition existence and stores offsets by group. | Storage-mode-independent. | Partial |
| `OffsetFetch` | Only the coordinator serves committed offsets. When no topics are supplied, returns all known committed offsets for the group. | Storage-mode-independent. | Partial |
| `OffsetDelete` | Only the coordinator accepts deletes. Returns per-topic/partition errors and deletes stored offsets where valid. | Storage-mode-independent. | Partial |

### Kafka Admin APIs

| API | Current Expected Behavior | Mode Notes | Initial Status |
|---|---|---|---|
| `CreateTopics` | Creates topics with validation for partitions, replication, and configs. Duplicate topics in request are invalid. | Supports diskless topic creation via `camu.storage.mode=diskless`. | Partial |
| `DeleteTopics` | Deletes topics; unknown topics return unknown-topic error. | Diskless deletion must also clear diskless metadata state through the shared server path. | Gap |
| `CreatePartitions` | Only controller may expand partitions. Manual replica assignment is rejected. | Mode difference should be explicit if diskless topics need different rules. | Gap |
| `DescribeConfigs` | Returns supported topic configs and rejects unsupported resource types. | Mode-independent at response shape level; mode-specific config semantics must be documented. | Gap |
| `AlterConfigs` | Only controller may mutate supported topic configs. | Need explicit diskless config compatibility rules. | Gap |
| `IncrementalAlterConfigs` | Only controller may mutate supported topic configs incrementally. | Need explicit diskless config compatibility rules. | Gap |
| `DescribeCluster` | Returns cluster ID, controller, and broker list. | Mode-independent. | Partial |
| `CreateACLs` / `DescribeACLs` / `DeleteACLs` | Supported ACL subset through shared ACL store. | Mode-independent. | Gap |

## Kafka Expected Behavior Notes

### Broker-wide notes

- The supported Kafka API subset is explicit and versioned through `ApiVersions`.
- Unsupported APIs are a hard protocol limitation, not an undocumented omission.
- Broker metadata and topic metadata must not expose storage mode differences unless the behavior truly differs at the protocol layer.

### Produce and fetch

- The client-facing contract should be mode-agnostic where possible: same error family, same offset semantics, same fetch-empty behavior.
- Mode-specific behavior is acceptable only when encoded as explicit errors or explicitly documented differences.

### Coordinator behavior

- Current coordinator routing is controller-based rather than per-group distributed.
- Until that changes, this document should treat it as intentional and require tests to match that contract.

### Offsets

- Kafka offset APIs should remain storage-mode-independent.
- Any deviation between classic and diskless here is likely a bug unless explicitly documented as unsupported.

## Current Evidence Snapshot

This section captures what is already covered well enough to influence prioritization. Update it after every run.

### HTTP evidence already present

- `test/integration/diskless_test.go`
  - diskless HTTP produce/consume basic path
  - diskless multiple flushes
  - diskless high-level produce routing
  - diskless HTTP consume message-limit parity with large messages
  - diskless HTTP consume empty-topic behavior
  - diskless HTTP consume beyond-end `next_offset` behavior
  - diskless SSE basic streaming behavior
  - diskless SSE `Last-Event-ID` resume behavior
- `test/integration/consume_parity_test.go`
  - side-by-side classic/diskless HTTP consume parity for bounded reads on the same large-message shape
  - side-by-side classic/diskless idle SSE connection behavior
- `test/integration/offsets_parity_test.go`
  - side-by-side classic/diskless standalone consumer offset commit/get behavior
  - side-by-side classic/diskless group offset commit/get behavior
- `internal/server/server_test.go`
  - broad handler-level HTTP coverage
  - producer init
  - classic produce/consume and some list-offset behavior
  - unit guard for diskless HTTP consume message-limit semantics
  - invalid-body handler coverage for HTTP offset commit endpoints

### Kafka evidence already present

- `test/integration/kafka_basic_test.go`
  - classic Kafka produce/consume
  - classic idempotent produce
  - compressed produce codecs
  - list-offsets consumption path
  - timestamp list-offset behavior
- `test/integration/kafka_group_test.go`
  - coordinator routing
  - join/sync/heartbeat/leave flows
  - offset commit/fetch/delete behavior
  - failover coordinator continuity cases
- `test/integration/kafka_leadership_test.go`
  - metadata broker/leader translation
  - follower/not-leader behavior
  - failover and metadata refresh cases
- `test/integration/diskless_kafka_test.go`
  - diskless Kafka produce/fetch basic path
  - diskless Kafka metadata unknown-topic behavior
  - diskless Kafka list-offsets earliest/latest/max-timestamp behavior
  - diskless Kafka timestamp list-offset rejection behavior
  - diskless Kafka empty-fetch watermark behavior
- `internal/server/kafka_transport_test.go`
  - transport-level metadata/produce/fetch/list-offsets/error mapping tests
- `internal/server/kafka_groups_test.go`
  - unit-level coordinator invariants and recent regression coverage

### Known evidence weaknesses

- HTTP stream parity between classic and diskless is now proven for connection establishment, basic streaming, and reconnect behavior; longer-lived idle/replay edge cases are still not exhaustively verified.
- HTTP consume parity is now directly covered side-by-side for bounded reads on the same large-message shape, plus diskless empty and beyond-end cases; the remaining gaps are mostly matrix breadth rather than known mismatches.
- HTTP offsets behavior is now directly proven for the basic standalone and group commit/get contract in both modes; malformed-body and unknown-topic policy breadth is still not exhaustive.
- Kafka admin APIs beyond metadata/list-offsets/groups are mostly API-shape tested, not deeply behavior-verified.
- Kafka transport/framing edge cases are not yet documented as a verification matrix.
- Diskless parity coverage is still thinner than classic for Kafka, but core metadata/list-offsets/fetch edge semantics are now directly covered.

## Prioritized Verification Backlog

This is the execution backlog derived from current gaps and evidence weaknesses.

### P0: Behavior mismatches or parity risks

| Priority | Area | Why first | Verification target | Cleanup expectation |
|---|---|---|---|---|
| P0 | HTTP stream parity | Current diskless stream path is custom and may diverge semantically from classic SSE behavior. | Add explicit classic vs diskless stream tests for start offset, reconnect via `Last-Event-ID`, empty polling, and ordering. | Remove dead SSE special cases if one path can be unified. |
| P0 | HTTP consume parity | Diskless consume currently translates `limit` into a byte budget before decode, which may not match classic message-count behavior. | Add side-by-side classic/diskless consume tests for `limit`, empty reads, beyond-end reads, and `next_offset`. | Remove transitional branching or document permanent semantic difference. |
| P0 | HTTP offsets matrix | Offset handlers look mode-agnostic but are not proven that way. | Add shared tests that run for topics created in both modes, covering commit, fetch, missing data, and malformed body cases. | Remove duplicate test scaffolding or helper branches after parity is proven. |
| P0 | Kafka diskless parity edges | Diskless Kafka coverage is currently basic-path only. | Add diskless Kafka tests for metadata, list-offsets earliest/latest, empty fetch, unsupported timestamp lookup, and error mapping. | Remove any now-unused fallback behavior discovered during parity fixes. |

### P1: Coordinator and metadata hardening

| Priority | Area | Why next | Verification target | Cleanup expectation |
|---|---|---|---|---|
| P1 | Kafka admin semantics | Many admin APIs are present but only lightly validated. | Build per-API tests for create/delete topics, create partitions, config mutation, and unsupported/manual-assignment cases. | Remove dead validation branches and stale comments from earlier admin iterations. |
| P1 | Kafka transport/framing matrix | Core transport works, but request/response framing edge cases are not enumerated. | Add documented cases for flexible/non-flexible headers, invalid size handling, unsupported API handling, and connection-close behavior. | Remove unnecessary compatibility fallbacks once exact supported framing is clear. |
| P1 | Metadata parity matrix | Metadata is covered, but not yet fully documented mode-by-mode. | Verify missing-topic behavior, replicated topic translation, diskless topic visibility, and follower-served metadata behavior. | Remove obsolete metadata workarounds if behavior is unified. |
| P1 | Group coordinator version matrix | Group behavior is better now, but version-specific semantics are not inventoried. | Add or document tests for request versions used by real clients, especially find/join/leave/sync behavior across versions. | Remove stale compatibility code only if clearly unreachable. |

### P2: Optimization-ready areas after correctness

| Priority | Area | Why later | Verification target before optimization | Cleanup expectation |
|---|---|---|---|---|
| P2 | Produce hot path allocations | High leverage, but only safe after parity matrix is green. | Confirm classic/diskless HTTP+Kafka produce semantics and error mapping first. | Delete superseded encode/decode fallback paths during optimization. |
| P2 | Fetch/read decode cost | Likely expensive in both classic and diskless paths. | Confirm fetch parity for empty/partial/bounded reads first. | Remove duplicate decode paths if a shared path emerges. |
| P2 | Group/offset persistence churn | Coordinator writes may be optimized after invariants are locked. | Confirm all coordinator and offset tests are green first. | Remove now-unused persistence workarounds. |
| P2 | Test suite deduplication | Valuable once parity coverage exists in both modes. | Build a stable matrix first, then consolidate shared test harnesses. | Delete duplicated helpers from older mode-specific tests. |

## Next Runs

Recommended immediate run order:

1. HTTP consume and stream parity inventory + tests
2. HTTP offsets mode matrix
3. Kafka diskless parity edge tests
4. Kafka admin verification matrix
5. Kafka transport/framing verification matrix

After each run:

- update matrix statuses
- record the exact evidence added
- note any behavior changes
- remove stale claims and obsolete code

## Run Ledger Template

Use this template after every execution run. Append new entries at the top of the ledger.

```md
### Run YYYY-MM-DD NN

Scope:

- what APIs or runbook items were targeted

Changes made:

- code changes
- test changes
- cleanup/removals

Verification run:

- exact commands run
- result summary

Matrix status changes:

- row X: Gap -> Partial
- row Y: Partial -> Confirmed

Behavior changes discovered:

- new expected behavior clarified
- mismatches found
- unsupported cases made explicit

Cleanup completed:

- removed dead helpers
- removed stale branches
- removed stale tests/comments/TODOs

Follow-up gaps:

- remaining risks
- next run target
```

## Run Ledger

### Run 2026-04-08 01

Scope:

- establish the canonical first-class planning document
- inventory HTTP and Kafka API surfaces
- prioritize the first execution backlog
- add concrete runbooks for the first five planned runs

Changes made:

- created canonical document at `docs/api-correctness-optimization-plan.md`
- moved content out of temporary docs area
- added mandatory documentation-actualization rules
- added HTTP API inventory
- added Kafka API inventory
- added evidence snapshot and prioritized backlog
- added five concrete runbooks

Verification run:

- no code execution or tests were run for these documentation-only updates

Matrix status changes:

- no API rows promoted; this run created the initial planning baseline

Behavior changes discovered:

- diskless HTTP consume `limit` semantics need explicit parity verification
- HTTP offset unknown-topic policy is still undecided and must be made explicit
- diskless Kafka compression support policy is still undecided and must be made explicit
- Kafka admin support level is broader in code than in verified evidence
- Kafka transport/framing contract exists in code but is not yet explicitly proven

Cleanup completed:

- removed the temporary `docs/superpowers/plans/...` copy in favor of the canonical main-doc location

Follow-up gaps:

- execute `HTTP Consume and Stream Parity` runbook first
- then `HTTP Offsets Mode Matrix`
- then `Kafka Diskless Parity Edge Tests`

### Run 2026-04-08 02

Scope:

- execute the first `HTTP Consume and Stream Parity` verification run
- verify whether diskless low-level consume preserves message-count `limit` semantics
- verify whether diskless SSE honors `Last-Event-ID` resume semantics

Changes made:

- reproduced the diskless HTTP consume `limit` mismatch with a targeted integration test using large per-message payloads
- replaced the old diskless consume inline decode path with iterative message-count-based fetching in `internal/server/handlers_consume.go`
- removed the now-unused byte-budget-based consume logic from the diskless HTTP handler
- added a server unit test for diskless consume message-limit handling
- added diskless integration coverage for consume-limit parity and SSE resume via `Last-Event-ID`
- added diskless integration coverage for empty-topic reads, beyond-end reads, and basic SSE streaming

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestHandleConsumeLowLevel_DisklessHonorsMessageLimit$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_(HTTPConsumeHonorsMessageLimit|SSELastEventIDResumesAfterSeenEvent)$' -count=1`

Matrix status changes:

- HTTP consume parity moved from `Gap` to `Mostly Confirmed`
- HTTP stream parity moved from `Gap` to `Partial`
- confirmed: diskless low-level consume now treats HTTP `limit` as message count for large-message multi-segment reads
- confirmed: diskless low-level consume returns stable empty and beyond-end `next_offset` values in the exercised cases
- confirmed: diskless SSE streams ordered events from the requested start offset in the exercised basic case
- confirmed: diskless SSE resumes from `Last-Event-ID + 1`

Behavior changes discovered:

- the prior diskless HTTP consume implementation was not parity-correct: `limit` was effectively constrained by a `limit * 1024` byte budget and could return fewer messages than requested
- diskless SSE resume semantics already matched the expected contract in the exercised reconnect case
- diskless empty-read and beyond-end consume behavior matched the expected contract in the exercised cases
- idle SSE behavior still needs explicit parity verification

Cleanup completed:

- removed the obsolete diskless consume branch that manually decoded a single fetched byte window and truncated after decode

Follow-up gaps:

- finish the remaining `HTTP Consume and Stream Parity` runbook cases for idle SSE behavior and side-by-side classic/diskless matrix cleanup
- then execute `HTTP Offsets Mode Matrix`
- then execute `Kafka Diskless Parity Edge Tests`

### Run 2026-04-08 03

Scope:

- finish the remaining `HTTP Consume and Stream Parity` gaps around side-by-side mode comparison
- verify idle SSE connection behavior in both classic and diskless modes

Changes made:

- added `test/integration/consume_parity_test.go` for direct classic/diskless consume and SSE parity checks
- verified bounded consume parity in both modes on the same large-message shape for `limit=1` and `limit=3`
- reproduced an idle SSE bug where both stream handlers withheld headers until the first event, causing clients to block on stream establishment
- fixed both classic and diskless SSE handlers to flush headers immediately on connection open

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestHTTPSSEIdleParityClassicAndDiskless$' -count=1 -timeout 45s`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestHTTPConsumeLimitParityClassicAndDiskless$' -count=1 -timeout 90s`

Matrix status changes:

- HTTP stream parity moved from `Partial` to `Mostly Confirmed`
- confirmed: classic and diskless SSE connections both establish cleanly on idle topics
- confirmed: classic and diskless bounded consume behavior matches on the same large-message shape for the exercised limits

Behavior changes discovered:

- both stream handlers had the same latent bug: headers were not flushed until the first event, so idle SSE clients could hang before the stream was established
- after immediate header flush, the exercised idle SSE behavior matches expectations in both modes

Cleanup completed:

- no dead test scaffolding remained after adding the shared parity test file

Follow-up gaps:

- longer-lived idle/reconnect SSE behavior is still not exhaustively verified
- `HTTP Consume and Stream Parity` is now mostly closed and no longer blocks moving to `HTTP Offsets Mode Matrix`
- next execution target should be `HTTP Offsets Mode Matrix`

### Run 2026-04-08 04

Scope:

- execute the `HTTP Offsets Mode Matrix` runbook
- verify standalone consumer offset behavior in both classic and diskless modes
- verify group offset behavior in both classic and diskless modes

Changes made:

- added `test/integration/offsets_parity_test.go` for direct classic/diskless offsets parity checks
- added handler tests for malformed JSON on the standalone and group offset commit endpoints
- no production offset logic changes were needed

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestCommit(Consumer|Group)OffsetsRejectsInvalidBody$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestHTTP(ConsumerOffsetsParityClassicAndDiskless|GroupOffsetsParityClassicAndDiskless)$' -count=1`

Matrix status changes:

- HTTP offsets mode matrix moved from `Gap` to `Mostly Confirmed`
- confirmed: standalone consumer offsets commit and fetch identically in classic and diskless modes for the exercised cases
- confirmed: group offsets commit and fetch identically in classic and diskless modes for the exercised cases
- confirmed: malformed JSON on both commit endpoints returns `400`

Behavior changes discovered:

- no storage-mode divergence was found in the exercised offset handlers
- missing consumer/group fetch behavior remains empty-map based in the exercised cases

Cleanup completed:

- no obsolete offset code paths were left behind because no production changes were necessary

Follow-up gaps:

- unknown-topic offset policy is still not explicitly matrixed
- broader malformed-body and mixed-topic edge coverage is still optional hardening rather than a current blocker
- next execution target should be `Kafka Diskless Parity Edge Tests`

### Run 2026-04-08 05

Scope:

- execute the `Kafka Diskless Parity Edge Tests` runbook
- verify diskless Kafka metadata, list-offsets, and empty-fetch edge behavior

Changes made:

- extended `test/integration/diskless_kafka_test.go` with explicit diskless Kafka edge cases
- added coverage for:
  - requested missing-topic metadata response
  - earliest/latest/max-timestamp list-offsets
  - unsupported timestamp list-offset lookup
  - empty fetch watermark reporting
- no production Kafka changes were needed

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_Kafka(ProduceAndFetch|MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)$' -count=1 -timeout 90s`

Matrix status changes:

- Kafka diskless parity edges moved from `Gap` to `Mostly Confirmed`
- confirmed: diskless Kafka metadata returns explicit unknown-topic entries for requested missing topics
- confirmed: diskless Kafka list-offsets returns `0` for earliest and head offset for latest / max-timestamp in the exercised cases
- confirmed: diskless Kafka timestamp lookup returns `INVALID_REQUEST` in the exercised unsupported case
- confirmed: diskless Kafka empty fetch reports stable high-watermark / last-stable-offset values without phantom record batches

Behavior changes discovered:

- no new classic vs diskless mismatch was found in the exercised Kafka edge cases
- diskless timestamp list-offset support remains intentionally unsupported and is now backed by integration evidence

Cleanup completed:

- no obsolete Kafka code paths were left behind because no production changes were necessary

Follow-up gaps:

- compression policy in diskless Kafka path is still not explicitly matrixed
- fetch breadth beyond the exercised empty-read / basic-read cases is still lighter than classic
- next execution target should be `Kafka Admin Verification Matrix` or `Kafka Transport and Framing Verification Matrix`

### Run 2026-04-08 06

Scope:

- make cross-protocol bootstrap explicit in Kafka integration helpers
- remove ambiguity about whether HTTP-created topics in Kafka tests are part of the protocol under test

Changes made:

- split the generic Kafka integration helper into:
  - `newKafkaEnv(...)` for a bare Kafka-enabled environment
  - `newKafkaHTTPBootstrappedEnv(...)` for tests that intentionally provision topics over HTTP before exercising Kafka APIs
- updated affected Kafka integration tests to use the explicit HTTP-bootstrap helper name
- added short comments to affected tests clarifying that HTTP topic creation is fixture setup, not Kafka admin verification

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ProduceConsumeWithFranzGo|ProduceHTTPConsume|HTTPProduceKafkaConsume|OffsetCommitFetchWithFranzGoRequests|MetadataAdvertisesLeaderAndBroker)$' -count=1 -timeout 90s`

Matrix status changes:

- no protocol behavior rows changed
- test intent is now explicit for HTTP-bootstrapped Kafka integration coverage

Behavior changes discovered:

- none; this was a test-clarity and fixture-boundary refactor

Cleanup completed:

- removed the ambiguous `newKafkaTestEnv(...)` naming that obscured HTTP fixture setup inside Kafka tests

Follow-up gaps:

- more Kafka tests still create topics over HTTP directly instead of through explicit helper naming
- if desired, the same naming cleanup can be applied more broadly across the remaining Kafka integration suite

### Run 2026-04-08 07

Scope:

- broaden the explicit HTTP-fixture cleanup across more Kafka integration tests
- replace ad hoc HTTP topic creation with named fixture helpers where Kafka admin behavior is not under test

Changes made:

- added `createHTTPFixtureTopic(...)` in `test/integration/kafka_test_helpers.go`
- replaced direct `httpClient.CreateTopic(...)` fixture setup in non-admin Kafka integration tests with the explicit helper
- updated additional Kafka basic and group tests to use the explicit fixture path

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(IdempotentProduceWithFranzGo|CompressedProduceWithFranzGo|ConsumeTopicsWithListOffsets|ListOffsetsByTimestamp|FetchReportsOffsetWatermarks|OffsetCommitFetchWithFranzGoRequests)$' -count=1 -timeout 120s`

Matrix status changes:

- no protocol behavior rows changed
- fixture provenance is now explicit across a larger portion of the Kafka integration suite

Behavior changes discovered:

- none; this was a readability and intent-clarification cleanup only

Cleanup completed:

- removed more ad hoc HTTP bootstrap calls in favor of explicit fixture helper usage

Follow-up gaps:

- some Kafka tests still perform raw HTTP diskless topic creation because Kafka admin does not expose diskless mode
- remaining direct HTTP bootstrap in Kafka tests should either use explicit fixture helpers or stay only in true admin/bridge setup cases

### Run 2026-04-08 08

Scope:

- move classic Kafka-protocol test topic creation from HTTP fixture setup to Kafka `CreateTopics`
- keep diskless Kafka tests on HTTP setup as the explicit exception because Kafka admin does not expose diskless mode

Changes made:

- changed the classic Kafka bootstrap helper to provision topics through Kafka `CreateTopics`
- updated classic Kafka basic, group, bridge, metadata, and create-partitions integration tests to use Kafka-admin topic bootstrap
- retained explicit HTTP setup only where diskless topic creation is required

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ProduceConsumeWithFranzGo|IdempotentProduceWithFranzGo|CompressedProduceWithFranzGo|ConsumeTopicsWithListOffsets|ListOffsetsByTimestamp|FetchReportsOffsetWatermarks|CreatePartitionsAndDescribeConfigs|ProduceHTTPConsume|HTTPProduceKafkaConsume|OffsetCommitFetchWithFranzGoRequests|MetadataAdvertisesLeaderAndBroker)$' -count=1 -timeout 120s`

Matrix status changes:

- no protocol behavior rows changed
- classic Kafka-protocol integration tests now bootstrap classic topics through Kafka admin instead of HTTP fixture setup in the exercised set

Behavior changes discovered:

- none; this was a fixture-path correction and test-intent alignment change

Cleanup completed:

- removed classic-topic HTTP bootstrap from the exercised Kafka protocol tests in favor of Kafka-admin fixture creation

Follow-up gaps:

- diskless Kafka tests still require HTTP topic creation because storage mode is not configurable via Kafka admin
- some remaining Kafka integration cases outside the exercised set may still need the same Kafka-admin bootstrap cleanup

### Run 2026-04-08 09

Scope:

- implement retention enforcement for diskless topics
- ensure diskless earliest-offset reporting advances after expired data is removed

Changes made:

- extended the diskless metastore contract with:
  - `GetPartitionStart(...)`
  - `DeleteExpiredSegments(...)`
- implemented those methods for memory and DynamoDB metastore backends
- added leader-side diskless retention cleanup to the coordination GC path
- updated diskless Kafka earliest-offset handling to use the first retained offset instead of hardcoded `0`
- kept deletion safe for shared multi-partition diskless data files by only deleting backing files after the last live metastore reference is gone

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/diskless -run 'TestMemoryMetaStore_(GetPartitionStartUsesHeadWhenNoSegmentsRemain|DeleteExpiredSegmentsAdvancesStart|DeleteExpiredSegmentsKeepsSharedFileUntilLastReference)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DisklessRetentionCleanupDeletesExpiredDataAndAdvancesEarliestOffset|HandleKafkaListOffsets_DisklessTimestampLookupReturnsInvalidRequest)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_Kafka(ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)$' -count=1 -timeout 90s`

Matrix status changes:

- diskless retention moved from `Gap` to `Implemented / Partially Verified`
- confirmed: expired diskless segment references are removed and unreferenced backing files are deleted
- confirmed: diskless Kafka earliest offset advances to the first retained offset after cleanup
- confirmed: existing diskless Kafka earliest/latest and empty-fetch behavior still works for non-expired data

Behavior changes discovered:

- prior to this run, diskless topics stored retention config but did not enforce it
- diskless earliest-offset reporting previously returned `0` unconditionally, which would have become incorrect once retention began deleting old data

Cleanup completed:

- removed the old implicit assumption that diskless earliest offset is always `0`

Follow-up gaps:

- there is still no full end-to-end integration test that waits for diskless retention expiry on a live server cadence
- diskless retention is now implemented, but expiry timing semantics are still verified mainly at unit/server level rather than through a long-running integration flow

### Run 2026-04-09 10

Scope:

- simplify the diskless retention implementation where possible
- make the Kafka admin retention contract explicit as time-based only

Changes made:

- simplified the in-memory diskless metastore earliest-offset lookup to return the first live segment directly instead of rescanning the partition slice
- made Kafka admin create/alter config handling reject `retention.bytes` with an explicit `INVALID_CONFIG` message that points callers to `retention.ms`
- kept the public retention surface narrow: age-based retention only, with no partial size-based behavior

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(KafkaCreateTopicRequestRejectsRetentionBytes|ApplyKafkaTopicConfigsRejectsRetentionBytes)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(CreateTopicsRejectsRetentionBytes|AlterConfigsRejectsRetentionBytes|IncrementalAlterConfigsRejectsRetentionBytes)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka admin path exposes only time-based retention controls
- confirmed: unsupported size-based retention config is rejected explicitly rather than falling through as a generic unknown config case

Cleanup completed:

- removed an unnecessary earliest-offset scan from the in-memory diskless metastore
- removed ambiguity in Kafka admin retention error behavior

Follow-up gaps:

- diskless topics still cannot be created through Kafka admin with `storage_mode`; HTTP remains the only diskless topic creation path
- retention expiry timing is still not covered by a full long-running integration test

### Run 2026-04-09 11

Scope:

- tighten Kafka admin mutation authority to match controller-bound semantics

Changes made:

- updated Kafka `CreateTopics` to return `NOT_CONTROLLER` on follower brokers instead of mutating shared topic state
- updated Kafka `DeleteTopics` to return `NOT_CONTROLLER` on follower brokers instead of deleting topics from any broker
- added direct handler tests proving follower requests do not create or delete topics

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestHandleKafka(CreateTopicsRequiresController|DeleteTopicsRequiresController)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(CreateAndDeleteTopics|CreatePartitionsAndDescribeConfigs|AlterConfigsAndIncrementalAlterConfigs)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka topic creation and topic deletion are controller-only mutations

Cleanup completed:

- removed the old non-Kafka shortcut that allowed any broker to mutate topics through Kafka admin handlers

Follow-up gaps:

- diskless topic creation over Kafka admin is still unsupported because `storage_mode` is not configurable on the Kafka path

### Run 2026-04-09 13

Scope:

- expose diskless topic creation explicitly through Kafka admin
- remove the last HTTP-only bootstrap dependency from the basic diskless Kafka path

Changes made:

- added Kafka `CreateTopics` support for the explicit custom topic config `camu.storage.mode`
- exposed `camu.storage.mode` through `DescribeConfigs`
- made `camu.storage.mode` immutable through Kafka config mutation APIs
- added `storage_mode` to HTTP topic responses so stored mode can be verified directly
- moved the basic diskless Kafka produce/fetch integration test from HTTP topic creation to Kafka admin topic creation

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(KafkaCreateTopicRequestAcceptsDisklessStorageMode|ApplyKafkaTopicConfigsRejectsStorageModeMutation)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(KafkaCreateDisklessTopicAndDescribeStorageMode|KafkaAlterConfigsRejectsStorageModeMutation|Diskless_KafkaProduceAndFetch)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka admin can create diskless topics explicitly via `camu.storage.mode=diskless`
- confirmed: Kafka `DescribeConfigs` reports `camu.storage.mode`
- confirmed: Kafka config mutation does not allow changing topic storage mode after creation
- confirmed: HTTP topic reads now expose `storage_mode`, allowing direct cross-API verification

Cleanup completed:

- removed the old HTTP bootstrap from the basic diskless Kafka produce/fetch test
- removed the response-shape omission that hid `storage_mode` from HTTP topic reads

Follow-up gaps:

- more diskless Kafka tests still use HTTP bootstrap and can now be migrated incrementally to Kafka admin
- Kafka admin still needs broader unsupported-path coverage for ACLs and any remaining constrained config combinations

### Run 2026-04-09 14

Scope:

- remove the remaining HTTP topic bootstrap from the dedicated diskless Kafka integration suite

Changes made:

- added a shared `createDisklessKafkaFixtureTopic(...)` helper for Kafka-admin diskless topic setup
- migrated the remaining tests in `test/integration/diskless_kafka_test.go` from HTTP topic creation to Kafka `CreateTopics` with `camu.storage.mode=diskless`
- added direct `storage_mode` verification on the created topics so the tests confirm the fixture mode instead of assuming it

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_Kafka(ProduceAndFetch|MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: the dedicated diskless Kafka integration suite now provisions diskless topics through Kafka admin rather than HTTP bootstrap

Cleanup completed:

- removed the remaining HTTP-bootstrap dependency from `test/integration/diskless_kafka_test.go`
- consolidated diskless Kafka topic setup on the same helper path

Follow-up gaps:

- HTTP-only diskless topic creation is still used in the HTTP integration suite, which is correct for HTTP API coverage
- Kafka admin still needs broader unsupported-path coverage for ACLs and any remaining constrained config combinations

### Run 2026-04-09 15

Scope:

- tighten Kafka ACL verification around invalid requests and follower-broker mutation behavior

Changes made:

- added integration coverage for invalid ACL requests across create, describe, and delete paths
- added a two-node integration test proving `CreateACLs` and `DeleteACLs` return `NOT_CONTROLLER` on follower brokers and do not mutate ACL state
- factored controller/follower Kafka address discovery into a shared helper so admin follower tests do not duplicate cluster-discovery logic

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ACLsRejectInvalidRequests|ACLMutationsOnFollowerReturnNotController)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: ACL create, describe, and delete reject invalid request shapes with explicit protocol errors
- confirmed: ACL mutations are controller-only over a real two-node Kafka setup

Cleanup completed:

- removed duplicated controller/follower discovery logic from follower-targeted Kafka admin tests

Follow-up gaps:

- Kafka admin still needs spot checks for the remaining constrained config/resource combinations outside ACLs

### Run 2026-04-09 16

Scope:

- verify the remaining constrained Kafka config-admin error paths

Changes made:

- added integration coverage proving `DescribeConfigs`, `AlterConfigs`, and `IncrementalAlterConfigs` reject unsupported non-topic resource types with explicit request errors
- added integration coverage proving `IncrementalAlterConfigs` cannot delete immutable `camu.storage.mode`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ConfigAPIsRejectUnsupportedResourceTypes|IncrementalAlterConfigsRejectsStorageModeDelete)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: config-admin APIs reject unsupported resource types consistently
- confirmed: storage mode immutability holds for incremental delete as well as set-to-different-value

Cleanup completed:

- removed another class of admin-surface ambiguity by turning constrained config combinations into explicit protocol-level tests

Follow-up gaps:

- Kafka admin spot checks are now mostly down to any remaining low-probability unsupported combinations, rather than core mutation or config semantics

### Run 2026-04-09 17

Scope:

- verify the HTTP topic admin response shape for classic and diskless topics

Changes made:

- added integration coverage for `GET /v1/topics/{topic}` and `GET /v1/topics` proving both classic and diskless topics expose the expected `storage_mode`
- verified that the same responses still preserve the core topic fields such as partition count

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestTopic(ReadResponsesExposeStorageMode|CRUD|CreateDuplicate)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: HTTP topic get/list responses expose `storage_mode` consistently for both classic and diskless topics

Cleanup completed:

- removed another documentation/testing mismatch where `storage_mode` was expected in the HTTP contract but not directly tested

Follow-up gaps:

- handler-level topic-response tests are still optional cleanup, not a correctness blocker

### Run 2026-04-09 18

Scope:

- remove the remaining HTTP topic readback from Kafka-focused integration tests

Changes made:

- replaced Kafka-test `httpClient.GetTopic(...)` assertions with Kafka `Metadata` and `DescribeConfigs` assertions
- added shared Kafka-side helpers for topic metadata and topic config readback
- kept HTTP topic readback only in HTTP-focused and explicit cross-API verification tests

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(Kafka(CreateAndDeleteTopics|CreatePartitionsAndDescribeConfigs|CreateDisklessTopicAndDescribeStorageMode)|Diskless_Kafka(ProduceAndFetch|MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)|KafkaAdminMutationsOnFollowerReturnNotController)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka-focused integration tests no longer depend on HTTP topic readback for verification

Cleanup completed:

- removed the remaining `GetTopic(...)` usage from Kafka-focused integration files
- tightened the boundary between Kafka-only verification and HTTP/cross-API verification

Follow-up gaps:

- remaining Kafka fixture-boundary work is no longer about core topic creation in the current Kafka-focused integration files

### Run 2026-04-09 19

Scope:

- remove the remaining HTTP replicated-topic fixture creation from the Kafka leadership suite

Changes made:

- added Kafka helper support for controller-routed replicated topic creation
- migrated the remaining multi-node Kafka leadership tests from HTTP replicated topic creation to Kafka `CreateTopics` with explicit replication config
- kept controller discovery inside shared Kafka helpers so repeated multi-node admin setup does not drift across tests

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(MetadataTranslatesReplicatedCamuState|ProduceToFollowerReturnsNotLeader|FetchFromFollowerReturnsNotLeader|MetadataUpdatesAfterLeaderFailover|ClientRecoversAcrossLeaderFailover)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: the Kafka leadership integration suite now provisions replicated topics through Kafka admin rather than HTTP setup

Cleanup completed:

- removed the remaining `CreateTopicWithReplication(...)` usage from Kafka-focused integration files
- consolidated replicated Kafka fixture creation behind shared controller-aware helpers

Follow-up gaps:

- Kafka-focused integration tests no longer rely on HTTP topic creation or HTTP topic readback; remaining work is now about smaller unsupported-path spot checks and non-Kafka suites

### Run 2026-04-09 20

Scope:

- verify Kafka admin `ValidateOnly` semantics for topic and config mutations

Changes made:

- added protocol-level integration coverage proving `CreateTopics ValidateOnly` does not create the topic
- added protocol-level integration coverage proving `CreatePartitions ValidateOnly` does not change partition count
- added protocol-level integration coverage proving `AlterConfigs ValidateOnly` and `IncrementalAlterConfigs ValidateOnly` do not persist config changes

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(CreateTopicsValidateOnlyDoesNotCreateTopic|CreatePartitionsValidateOnlyDoesNotMutate|AlterConfigsValidateOnlyDoesNotMutate|IncrementalAlterConfigsValidateOnlyDoesNotMutate)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka admin `ValidateOnly` paths validate requests without mutating stored topic or config state

Cleanup completed:

- removed another unverified branch from the Kafka admin surface by turning `ValidateOnly` behavior into direct protocol tests

Follow-up gaps:

- remaining correctness work is increasingly about smaller mode-specific or endpoint-specific spot checks rather than broad API-surface uncertainty

### Run 2026-04-09 21

Scope:

- add one compact proof that the currently supported Kafka topic settings are actually applied, not just accepted

Changes made:

- added a two-node Kafka integration test that creates a replicated topic with the supported create-time settings, reads them back through `DescribeConfigs`, alters the mutable ones, and verifies the new values are applied
- covered the currently supported topic settings together in one place:
  - `cleanup.policy`
  - `retention.ms`
  - `min.insync.replicas`
  - `unclean.leader.election.enable`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaSupportedTopicConfigsRoundTrip$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: the supported Kafka topic settings round-trip through create/describe and alter/describe

Cleanup completed:

- reduced the need to infer config application from scattered one-off tests by adding one compact matrix-style round-trip test

Follow-up gaps:

- the remaining gaps are mostly narrow spot checks outside the core supported Kafka topic-config surface

### Run 2026-04-09 22

Scope:

- tighten the Kafka partition-count mutation contract

Changes made:

- added integration coverage proving `CreatePartitions` scales topic metadata up when the requested count increases
- added integration coverage proving `CreatePartitions` rejects a decrease and leaves topic metadata unchanged

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaCreatePartitions(AndDescribeConfigs|RejectsDecreaseAndLeavesMetadataUnchanged)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka topic partitions can only increase, not decrease
- confirmed: successful partition expansion is reflected in Kafka metadata

Cleanup completed:

- removed another behavioral assumption from the partition-admin surface by turning the no-shrink rule into a direct integration check

Follow-up gaps:

- remaining correctness work is now primarily about smaller endpoint-specific parity checks rather than core admin mutation rules

### Run 2026-04-09 23

Scope:

- verify that newly provisioned partitions are actually ready to use after partition expansion in both storage modes

Changes made:

- added integration coverage proving a newly added classic partition becomes writable and readable after `CreatePartitions`
- added integration coverage proving a newly added diskless partition becomes writable and readable after `CreatePartitions`
- kept the checks client-visible: write to the new partition through the public produce path and read it back through the public consume path

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestCreatePartitions(Classic|Diskless)NewPartitionIsReady$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: partition expansion not only updates metadata, but produces ready-to-use new partitions in both `classic` and `diskless` modes

Cleanup completed:

- removed another implicit assumption from the partition-scaling surface by testing actual readiness rather than only metadata shape

Follow-up gaps:

- remaining work is now mostly smaller parity and edge-case checks outside the core provisioning and mutation flows

### Run 2026-04-09 24

Scope:

- full verification pass across the current server and integration suites

Changes made:

- no production or test code changes in this run
- executed the broader suites to verify that the accumulated correctness work is green end to end rather than only through targeted slices

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -count=1 -timeout 180s`

Matrix status changes:

- confirmed: the current `internal/server` package tests pass as a whole
- confirmed: the current integration test package passes as a whole

Cleanup completed:

- no code cleanup in this run; this was a verification-only actualization pass

Follow-up gaps:

- the current focused plan items are green in the executed suites; remaining work is about new coverage, not outstanding failures in the verified suites
- Kafka admin still needs explicit multi-node follower-broker coverage for unsupported or constrained diskless admin behavior

### Run 2026-04-09 12

Scope:

- verify Kafka admin mutation errors against a real follower broker in a multi-node cluster

Changes made:

- added a two-node Kafka integration test that discovers the controller via `DescribeCluster`, sends `CreateTopics` and `DeleteTopics` to the follower broker, and verifies `NOT_CONTROLLER`
- verified that follower-targeted create does not create the topic and follower-targeted delete does not remove an existing topic

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestHandleKafka(CreateTopicsRequiresController|DeleteTopicsRequiresController)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaAdminMutationsOnFollowerReturnNotController$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: controller-only Kafka admin mutation semantics now have both direct handler proof and real follower-broker integration proof

Cleanup completed:

- removed the last major ambiguity around whether topic admin mutations were only locally enforced or actually observable over the network

Follow-up gaps:

- diskless topic creation over Kafka admin is still unsupported because `storage_mode` is not configurable on the Kafka path

## Concrete Runbook: HTTP Consume and Stream Parity

This is the first execution sub-plan under the P0 backlog.

### Why this run is first

- HTTP low-level consume is a core user-facing API in both modes.
- Diskless consume currently uses a different fetch/decode strategy than classic.
- Classic SSE uses `consumer.StreamSSE`, while diskless SSE uses a custom polling loop.
- We already have classic SSE integration coverage in `test/integration/consume_test.go`, but no equivalent diskless SSE parity proof.

### Current implementation split

Classic consume path:

- `internal/server/handlers_consume.go`
- `internal/server/consume_stream.go`
- `internal/consumer/fetcher.go`
- `internal/consumer/sse.go`

Diskless consume path:

- `internal/server/handlers_consume.go`
- diskless branch uses `disklessEngine.Fetch(...)`
- diskless SSE branch uses a custom loop with direct `WriteSSEEvent(...)`

### Expected parity to verify

#### Low-level JSON consume parity

- same query params:
  - `offset`
  - `limit`
- same input validation behavior:
  - invalid partition -> `400`
  - invalid offset -> `400`
  - invalid limit -> `400`
- same empty-read contract:
  - no messages
  - correct `next_offset`
- same beyond-end contract:
  - no messages
  - stable `next_offset`
- messages must be returned in offset order
- `next_offset` must equal the next readable offset after the final returned message, not merely a storage watermark unless the response is empty

#### SSE stream parity

- same start position semantics for:
  - explicit `offset`
  - `Last-Event-ID`
- same event ordering
- same event ID semantics:
  - `id` equals offset
- same payload shape:
  - offset
  - timestamp
  - key
  - value
  - headers
- same reconnect behavior:
  - resuming after `Last-Event-ID = N` starts from `N+1`
- same empty-poll behavior:
  - no malformed events
  - no duplicate replay on idle loop

### Known risk to resolve explicitly

Run 2026-04-08 02 resolved the main low-level consume risk:

- the old diskless handler did not preserve message-count `limit` semantics for large-message multi-segment reads
- the handler was changed to fetch iteratively by decoded offset progress instead of relying on a single `limit * 1024` byte window
- the obsolete byte-budget-based handler logic was removed

Remaining questions in this runbook:

- idle SSE behavior parity
- side-by-side classic/diskless matrix coverage for the same message shapes

This runbook still needs to answer the remaining parity questions without reopening the consume-limit bug.

The original decision branch for this run was:

1. The current behavior is equivalent enough and can be confirmed by tests.
2. The current behavior is not equivalent and must be changed.
3. The current behavior remains intentionally different and must be documented explicitly.

Option 3 is the least desirable outcome.

### Required tests for this run

#### Integration tests

- [ ] Add diskless consume parity tests mirroring classic consume cases from `test/integration/consume_test.go`
- [x] Add diskless SSE streaming test mirroring classic SSE behavior end-to-end
- [x] Add diskless reconnect test using `Last-Event-ID`
- [x] Add diskless empty-read / beyond-end tests
- [x] Add side-by-side classic and diskless tests for `limit=2` with large messages
- [x] Add side-by-side classic and diskless tests for `limit=1` and a larger limit on the same message shape

#### Unit or handler-level tests

- [ ] Add direct handler tests for consume validation in both mode branches if integration coverage is too coarse
- [x] Add direct handler test for diskless bounded-read `limit` behavior
- [x] Add direct handler tests for diskless `next_offset` behavior on empty and beyond-end reads

### Cleanup targets for this run

- [ ] Remove any duplicated SSE formatting logic if the diskless and classic paths can share more code
- [ ] Remove stale comments describing old diskless behavior if the implementation changes
- [x] Remove any now-unused helper branches after parity is established
- [ ] Update this plan with final expected consume/stream semantics immediately after the run

### Success criteria

This run is complete only when:

- consume parity expectations are explicit in this document
- diskless SSE behavior is proven or explicitly documented as intentionally different
- `limit` semantics are resolved and documented
- obsolete stream/consume branches are removed where possible
- evidence links in this document are updated

Current status after Run 2026-04-08 02:

- low-level consume `limit` semantics are fixed and verified
- diskless basic SSE and reconnect behavior are verified
- obsolete consume logic has been removed
- idle SSE verification and the final side-by-side consume matrix cases were completed in Run 2026-04-08 03
- remaining work is broader long-lived SSE edge coverage, which no longer blocks moving to the offsets runbook

## Concrete Runbook: HTTP Offsets Mode Matrix

This is the second execution sub-plan under the P0 backlog.

### Why this run is next

- The HTTP offset handlers appear storage-mode-agnostic, which is exactly the kind of area where silent divergence can hide.
- They are simpler than consume/stream semantics, so we should be able to fully close this part of the matrix quickly.
- A proven mode-independent offsets contract gives us a stable baseline before optimizing offset storage or coordinator persistence behavior elsewhere.

### APIs in scope

- `POST /v1/topics/{topic}/offsets/{consumer_id}`
- `GET /v1/topics/{topic}/offsets/{consumer_id}`
- `POST /v1/groups/{group_id}/commit`
- `GET /v1/groups/{group_id}/offsets`

### Current implementation summary

Standalone consumer offsets:

- `handleCommitConsumerOffsets`
- `handleGetConsumerOffsets`
- stored via `offsetStore.CommitConsumer(...)`
- read via `offsetStore.GetConsumer(...)`

Group offsets:

- `handleCommitOffsets`
- `handleGetOffsets`
- stored via `offsetStore.CommitGroupTopics(...)`
- read via `offsetStore.GetGroupTopics(...)`

These handlers do not branch on storage mode directly. The purpose of this run is to prove that the contract is truly mode-independent in practice.

Run 2026-04-08 04 materially confirmed this for the base contract:

- standalone consumer offset commit/get is parity-correct across classic and diskless in the exercised cases
- group offset commit/get is parity-correct across classic and diskless in the exercised cases
- malformed JSON returns `400` for both commit endpoints
- remaining work is policy breadth, not a known mode mismatch

### Expected behavior to lock down

#### Standalone consumer offsets

- commit accepts JSON body with per-partition offsets keyed by string partition IDs
- malformed JSON returns `400`
- successful commit returns `200`
- fetch returns committed offsets exactly as last written
- fetching before any commit returns an empty offsets map rather than an error
- behavior is identical whether the topic is `classic` or `diskless`

#### Group offsets

- commit accepts topic -> partition -> offset JSON mapping
- malformed JSON returns `400`
- successful commit returns `200`
- fetch returns committed offsets exactly as last written
- fetching before any commit returns an empty topics map rather than an error
- behavior is identical whether the referenced topics are `classic`, `diskless`, or mixed

### Edge cases to verify explicitly

- commit only one partition, then fetch all for that consumer/group
- commit multiple topics in one group request
- overwrite an existing committed offset
- mixed classic + diskless topics in one group commit request
- unknown topic names:
  - determine whether HTTP offset APIs intentionally allow them
  - if they do, document that offsets are decoupled from topic existence
  - if they should not, change the handlers and add explicit tests

The unknown-topic rule must be explicit after this run.

### Required tests for this run

#### Integration tests

- [ ] Add standalone consumer offset round-trip test for a classic topic
- [ ] Add standalone consumer offset round-trip test for a diskless topic
- [ ] Add group offset round-trip test for classic topics
- [ ] Add group offset round-trip test for diskless topics
- [ ] Add mixed-mode group offset round-trip test
- [ ] Add overwrite test proving the last commit wins

#### Handler or unit-level tests

- [ ] Add malformed body tests for standalone consumer commit
- [ ] Add malformed body tests for group commit
- [ ] Add no-data-yet fetch tests for both standalone and group offsets
- [ ] Add explicit unknown-topic behavior tests once the intended rule is chosen

### Cleanup targets for this run

- [ ] Remove duplicated offset test scaffolding if classic and diskless can share one matrix helper
- [ ] Remove stale comments implying mode-specific behavior if the contract is confirmed invariant
- [ ] Remove dead validation branches if the unknown-topic rule is clarified and simplified
- [ ] Update this plan immediately with the final unknown-topic policy

### Success criteria

This run is complete only when:

- all four HTTP offset endpoints have explicit expected behavior in this document
- classic and diskless behavior is proven identical or the differences are documented explicitly
- unknown-topic behavior is decided and tested
- test helpers are consolidated where possible
- this document is actualized with the evidence added in the run

## Concrete Runbook: Kafka Diskless Parity Edge Tests

This is the third execution sub-plan under the P0 backlog.

### Why this run is next

- Diskless Kafka coverage currently proves only the basic produce/fetch path.
- The classic Kafka path already has materially broader coverage for metadata, list-offsets, idempotency, compression, coordinator behavior, and leadership errors.
- Without a diskless edge matrix, we risk treating classic Kafka semantics as proven for diskless topics when they are not.

### APIs in scope

- `Metadata`
- `ListOffsets`
- `Produce`
- `Fetch`

Group APIs are intentionally excluded from this run because they are meant to be storage-mode-independent and are already covered elsewhere. This run focuses on topic/partition data-plane parity and metadata discovery for diskless topics.

### Current evidence baseline

Already present:

- `test/integration/diskless_kafka_test.go`
  - basic diskless Kafka produce and fetch
- `test/integration/kafka_basic_test.go`
  - classic produce/fetch
  - classic compressed produce
  - classic list-offsets behavior
  - classic timestamp-based list-offsets
- `test/integration/kafka_leadership_test.go`
  - metadata and leader translation for classic / replicated topics
- `internal/server/kafka_transport_test.go`
  - unit-level metadata/fetch/list-offsets/error mapping checks

Missing for diskless:

- compression parity checks
- explicit error mapping checks on invalid or unsupported diskless semantics

Run 2026-04-08 05 materially improved this baseline:

- metadata visibility and missing-topic shape are explicitly verified
- earliest/latest/max-timestamp list-offset behavior is explicitly verified
- unsupported timestamp lookup is explicitly verified
- empty fetch watermark behavior is explicitly verified
- remaining work is breadth and compression policy, not a known diskless protocol mismatch

### Expected behavior to lock down

#### Metadata

- diskless topics must appear as normal topics in Kafka metadata
- partitions must be discoverable through metadata exactly like classic topics
- missing-topic metadata requests must return explicit unknown-topic entries for diskless just like classic

#### ListOffsets

- earliest (`-2`) must work for diskless topics
- latest (`-1`) must work for diskless topics
- max-timestamp (`-4`) must follow the current documented diskless behavior
- timestamp lookup with ordinary timestamps must return explicit invalid request for diskless topics

#### Produce / Fetch

- basic produce and fetch already work and should remain covered
- empty fetch from the current log end must return a valid empty records response
- bounded fetch should not return malformed batches
- fetch response fields (`HighWatermark`, `LastStableOffset`, `RecordBatches`) must have client-acceptable values

#### Compression

- If compressed Kafka produce is intended to work for diskless topics, prove it with integration tests.
- If it is not yet intended to work, return explicit errors and document that as unsupported.

This run must resolve that policy.

### Edge cases to verify explicitly

- metadata request for an existing diskless topic
- metadata request for a missing topic when diskless topics also exist
- list-offsets earliest on diskless topic
- list-offsets latest on diskless topic
- list-offsets timestamp lookup on diskless topic -> explicit error
- fetch at offset equal to current head on diskless topic -> valid empty fetch
- fetch with a small max-bytes budget on diskless topic -> valid bounded response
- compressed produce to diskless topic -> either proven supported or explicitly rejected

### Required tests for this run

#### Integration tests

- [x] Add diskless Kafka metadata test
- [x] Add diskless Kafka list-offsets earliest/latest test
- [x] Add diskless Kafka unsupported timestamp lookup test
- [x] Add diskless Kafka empty fetch test
- [ ] Add diskless Kafka bounded fetch test
- [ ] Add diskless Kafka compressed produce test or explicit unsupported-behavior test

#### Unit or transport-level tests

- [ ] Add transport-level tests for diskless invalid-request list-offset mapping if integration coverage is too coarse
- [ ] Add unit or transport checks for empty diskless fetch response shape if needed

### Cleanup targets for this run

- [ ] Remove any hidden classic-only assumptions in metadata or list-offset code paths
- [ ] Remove stale comments implying timestamp lookup support for diskless if unsupported remains the contract
- [ ] Remove dead fallback branches if compression support policy is clarified
- [ ] Consolidate shared classic/diskless Kafka test helpers where practical

### Success criteria

This run is complete only when:

- diskless Kafka metadata behavior is explicitly proven
- diskless list-offset behavior is explicit for earliest/latest/timestamp cases
- empty and bounded fetch behavior is proven
- compression support policy for diskless Kafka topics is no longer ambiguous
- obsolete compatibility or fallback code discovered during the run is removed
- this document is actualized with the new evidence

Current status after Run 2026-04-08 05:

- diskless Kafka metadata behavior is proven for existing and missing requested topics
- diskless list-offset behavior is proven for earliest/latest/max-timestamp and unsupported timestamp cases
- empty fetch watermark behavior is proven
- bounded fetch breadth and compression policy remain open

## Concrete Runbook: Kafka Admin Verification Matrix

This is the fourth execution sub-plan from the current `P1` backlog.

### Why this run matters

- The Kafka admin surface is broad enough to drift silently if we only rely on API-shape tests.
- Several handlers already exist and return plausible responses, but "plausible" is not enough for admin clients.
- We need an explicit supported subset and explicit error behavior for unsupported or constrained operations.

### APIs in scope

- `CreateTopics`
- `DeleteTopics`
- `CreatePartitions`
- `DescribeConfigs`
- `AlterConfigs`
- `IncrementalAlterConfigs`
- `DescribeCluster`
- `CreateACLs`
- `DescribeACLs`
- `DeleteACLs`

### Current evidence baseline

Already present:

- `internal/server/kafka_api_test.go`
  - API presence and basic request/response plumbing
  - basic callback forwarding checks
- some server-backed handler logic in `internal/server/server.go`

Not yet sufficiently proven:

- topic validation behavior under real server-backed state
- diskless topic behavior under Kafka admin APIs
- exact error mapping for unsupported operations
- controller-only semantics for mutation APIs
- config mutation behavior and mode compatibility
- delete semantics including diskless cleanup expectations

### Required outcome of this run

At the end of this run, every admin API must be classified into one of:

1. fully supported
2. partially supported with explicit constraints
3. unsupported with explicit error behavior

No admin API should remain in an ambiguous "probably works" state.

### Expected behavior to lock down

#### CreateTopics

- valid create request succeeds
- duplicate topics in one request are invalid
- invalid partitions / replication are rejected explicitly
- unsupported config keys are rejected explicitly
- only the controller may create topics; followers must return `NOT_CONTROLLER`
- diskless-specific topic creation constraints must be decided and tested:
  - whether `storage_mode` can be selected through Kafka admin path
  - whether diskless topics are classic-only on Kafka admin path for now

This run must resolve that policy.

#### DeleteTopics

- deleting an existing topic succeeds
- deleting a missing topic returns unknown-topic error
- only the controller may delete topics; followers must return `NOT_CONTROLLER`
- deleting a diskless topic must either:
  - perform the same cleanup as HTTP delete, or
  - be documented and tested as intentionally unsupported/incomplete

This run must resolve that policy.

#### CreatePartitions

- only controller may mutate partition count
- count must strictly increase
- manual assignment remains unsupported
- diskless-specific partition-expansion behavior must be made explicit

#### Config APIs

- `DescribeConfigs` must state exactly which topic configs are supported
- `AlterConfigs` and `IncrementalAlterConfigs` must reject unsupported resource types
- topic config mutation must be validated consistently
- diskless-specific config compatibility must be explicit
- retention must remain age-based only:
  - `retention.ms` is the supported retention control
  - `retention.bytes` must return an explicit invalid-config error with guidance to use time-based retention

#### DescribeCluster

- returns cluster ID, controller, brokers
- should be mode-independent

#### ACL APIs

- supported resource and filter subset must be explicit
- unsupported combinations must return explicit errors
- CRUD parity must be proven for the implemented subset

### Edge cases to verify explicitly

- create classic topic via Kafka admin path
- create diskless topic via Kafka admin path, if intended
- create diskless topic via Kafka admin path, with explicit rejection if not intended
- delete missing topic
- delete diskless topic and verify cleanup policy
- create partitions on non-controller
- alter configs on non-controller
- unsupported config name
- unsupported resource type
- ACL filter combinations outside supported subset

### Required tests for this run

#### Integration tests

- [ ] Add Kafka admin create-topic integration test against server-backed state
- [ ] Add Kafka admin delete-topic integration test
- [ ] Add Kafka admin create-partitions integration test
- [ ] Add Kafka config mutation integration tests for supported and unsupported cases
- [ ] Add Kafka ACL integration tests for implemented subset if current unit tests are not sufficient
- [ ] Add diskless-topic admin-path tests once diskless policy is chosen

#### Unit or handler-level tests

- [ ] Add direct handler tests for controller-only rejection paths
- [ ] Add direct handler tests for invalid config and unsupported resource cases
- [ ] Add direct handler tests for diskless-specific admin constraints once policy is fixed

### Cleanup targets for this run

- [ ] Remove stale comments implying broader Kafka admin support than actually exists
- [ ] Remove dead validation branches once the supported subset is finalized
- [ ] Remove duplicate admin-path helper code if classic and diskless policy becomes simpler
- [ ] Update `ApiVersions` support claims if any API is intentionally not ready for real clients

### Success criteria

This run is complete only when:

- the supported Kafka admin subset is explicit in this document
- diskless behavior for Kafka admin APIs is explicit
- controller-only mutation semantics are proven
- unsupported admin operations have explicit tested error behavior
- stale or misleading admin-path code/comments are removed
- this document is actualized with the evidence added

## Concrete Runbook: Kafka Transport and Framing Verification Matrix

This is the fifth execution sub-plan from the current `P1` backlog.

### Why this run matters

- The Kafka transport layer is the narrow waist for every broker API.
- A handler can be logically correct while the broker is still incompatible because of framing, version, or flexible-header edge behavior.
- Wire-level assumptions tend to accumulate compatibility hacks unless they are written down and verified explicitly.

### Areas in scope

- request frame length handling
- request decode behavior
- response framing and correlation IDs
- flexible vs non-flexible headers
- supported vs unsupported API handling
- connection behavior on malformed input

### Current implementation baseline

Primary code:

- `internal/server/kafka_wire.go`
- `internal/server/kafka_codec.go`

Current evidence:

- `internal/server/kafka_transport_test.go`
  - request/response round-trip
  - metadata/produce/fetch transport checks
  - compression decode cases
- `internal/server/kafka_api_test.go`
  - API presence and request dispatch

Current ambiguity:

- exact supported framing matrix is not documented
- malformed request behavior is not explicitly enumerated
- flexible-header assumptions are present but not yet laid out as an intentional compatibility contract

### Required outcome of this run

At the end of this run, the transport layer must have an explicit compatibility statement for:

1. what request forms are accepted
2. what response forms are emitted
3. how malformed frames are handled
4. what happens for unsupported APIs and versions

### Expected behavior to lock down

#### Frame boundaries

- broker reads a 4-byte size prefix followed by the exact frame body
- zero or negative size is invalid and closes the connection
- oversized size beyond `maxKafkaRequestSize` is invalid and closes the connection

#### Decode behavior

- supported API key + version should decode into the right request type
- unsupported API key should fail explicitly
- malformed bodies should fail decode and terminate the connection
- request header handling for flexible and non-flexible requests must be explicit

#### Response behavior

- response correlation ID must match the request
- flexible responses must include the correct header shape for the supported APIs
- empty fetch records field must still encode in a client-acceptable way for flexible fetch versions

#### Error behavior

- malformed transport input closes the connection rather than returning a partial response
- unsupported APIs should not corrupt connection state
- handler errors currently terminate the connection; this behavior should either be documented as intentional or improved later

This run must decide whether handler-error connection termination remains the contract for now.

### Edge cases to verify explicitly

- invalid frame length
- truncated request body
- unsupported API key
- supported API with valid correlation ID round-trip
- flexible request decode path
- non-flexible request decode path
- flexible response header emission
- fetch empty-records encoding for flexible versions
- malformed request followed by no extra response bytes

### Required tests for this run

#### Unit or transport-level tests

- [ ] Add invalid-length transport test
- [ ] Add truncated-body transport test
- [ ] Add unsupported-API transport test
- [ ] Add explicit flexible-request decode test
- [ ] Add explicit non-flexible-request decode test
- [ ] Add explicit flexible-response framing test
- [ ] Add empty flexible fetch response encoding test if current coverage is indirect
- [ ] Add connection-close behavior tests for malformed input

#### Documentation tasks

- [ ] Write down the currently supported wire compatibility contract in this document
- [ ] Record any known intentional deviations from full Kafka broker behavior

### Cleanup targets for this run

- [ ] Remove unnecessary decode fallbacks if a stricter supported-header matrix is sufficient
- [ ] Remove stale comments implying support for unverified framing behavior
- [ ] Consolidate duplicate transport helper logic in tests
- [ ] Remove compatibility code that exists only for no-longer-supported assumptions

### Success criteria

This run is complete only when:

- the accepted request/response framing contract is explicit
- malformed input behavior is proven by tests
- flexible/non-flexible header behavior is explicit and tested
- connection-close semantics on fatal decode/handle errors are documented
- obsolete transport compatibility hacks are removed where possible
- this document is actualized with the new evidence

## Detailed Workstreams

### Workstream A: Inventory and behavior contract

- [ ] Create a concrete API inventory from `internal/server/routes.go` and Kafka handlers.
- [ ] For each API, write exact expected semantics for:
  - success response shape
  - validation failures
  - not found behavior
  - leadership/coordinator errors
  - unsupported mode combinations
- [ ] Fill the matrix above with an initial `Expected` entry for every row.

### Workstream B: Correctness verification

- [ ] Add a direct unit or integration test for every matrix row that is still `Gap`.
- [ ] Ensure each API is exercised in both `classic` and `diskless` modes when applicable.
- [ ] Add negative-path tests for intentionally unsupported combinations.
- [ ] Keep expected and confirmed behavior synchronized in this document after every test addition.

### Workstream C: Cleanup of previous iterations

- [ ] Audit `internal/server` for fallback paths that were useful during migration but are no longer needed.
- [ ] Remove duplicate routing logic between classic and diskless paths where one branch is dead.
- [ ] Remove helpers that are no longer referenced after correctness fixes.
- [ ] Remove stale comments and TODOs that describe superseded behavior.
- [ ] Delete tests that only protect removed transitional behavior.

### Workstream D: Optimization after correctness

Optimize only after the API row is `Confirmed`.

Priority targets:

- Produce path allocations and double-decode work
- Fetch/read path redundant batch decoding
- Diskless timestamp and range-scan efficiency
- Metadata and coordinator request hot paths
- Offset/group persistence churn
- Test runtime and duplication across classic/diskless coverage

Each optimization must:

- identify the hot path
- state the invariant it relies on
- list the correctness tests that guard it
- include cleanup of any superseded implementation

### Workstream E: Documentation actualization

- [ ] After every run, update this file before stopping.
- [ ] Record what was verified in that run.
- [ ] Record what changed in actual behavior.
- [ ] Record what cleanup was completed.
- [ ] Record any newly opened gaps or unsupported cases.
- [ ] Remove stale statements from earlier runs.

## Required Test Matrix

Minimum matrix to complete before broad optimization:

- [ ] HTTP topic CRUD in classic
- [ ] HTTP topic CRUD in diskless
- [ ] HTTP produce/consume in classic
- [ ] HTTP produce/consume in diskless
- [ ] HTTP offsets in classic
- [ ] HTTP offsets in diskless
- [ ] Kafka produce/fetch in classic
- [ ] Kafka produce/fetch in diskless
- [ ] Kafka metadata/list-offsets in classic
- [ ] Kafka metadata/list-offsets in diskless
- [ ] Kafka group flows in classic-backed topics
- [ ] Kafka group flows with diskless topics where behavior should be identical
- [ ] Explicit unsupported behavior tests for every unsupported mode/API combination

## Cleanup Checklist For Every API Change

Before closing any API task, answer:

- [ ] Did we remove now-unused helpers or branches?
- [ ] Did we remove stale tests?
- [ ] Did we remove stale comments/TODOs?
- [ ] Did we document the final expected behavior here?
- [ ] Did we confirm behavior with tests in every applicable mode?

## Run Completion Checklist

Every execution run must end with:

- [ ] code/tests updated as needed
- [ ] obsolete code removed or explicitly deferred with reason
- [ ] this document actualized
- [ ] matrix statuses updated
- [ ] evidence updated
- [ ] new gaps recorded

## Suggested Next Concrete Steps

1. Build the initial row-by-row HTTP API matrix from `routes.go`.
2. Build the initial row-by-row Kafka API matrix from `kafka_wire.go` and related handlers.
3. Mark each row `Confirmed`, `Gap`, or `Unsupported`.
4. Start with APIs that already exist in both `test/integration/diskless_test.go` and `test/integration/diskless_kafka_test.go`.
5. After the matrix is stable, begin optimization on the highest-traffic confirmed paths only.

### Run 2026-04-09 25

Scope:

- review Kafka-focused tests for stale fixture comments and redundant assertions after the Kafka-only bootstrap cleanup

Changes made:

- removed the stale comment in `test/integration/kafka_leadership_test.go` that still described topic provisioning as HTTP-based
- trimmed `TestKafkaCreatePartitionsAndDescribeConfigs` so it no longer duplicates partition-scale assertions now covered by the dedicated no-shrink and new-partition-readiness tests

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(MetadataAdvertisesLeaderAndBroker|CreatePartitionsAndDescribeConfigs)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: Kafka-focused integration tests and comments now consistently describe Kafka-admin topic provisioning

Cleanup completed:

- removed one stale test comment
- removed one redundant partition-shape assertion from a mixed-purpose Kafka admin test

Follow-up gaps:

- further test cleanup is now mostly about optional deduplication, not incorrect fixture semantics

### Run 2026-04-09 26

Scope:

- remove another small layer of duplicated topic-mode assertions from the integration suite

Changes made:

- added a shared Kafka-side `camu.storage.mode` assertion helper for diskless topic tests
- replaced repeated inline storage-mode checks in the diskless Kafka integration suite with that helper
- trimmed `TestTopicReadResponsesExposeStorageMode` so it stays focused on the `storage_mode` contract instead of rechecking partition counts already covered by CRUD tests

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(Diskless_Kafka(ProduceAndFetch|MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)|Topic(ReadResponsesExposeStorageMode|CRUD))$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: topic-mode assertions are still covered, with less duplicated test logic

Cleanup completed:

- removed repeated inline Kafka `storage_mode` checks
- removed redundant partition-count assertions from the HTTP storage-mode response-shape test

Follow-up gaps:

- remaining cleanup is mostly broader deduplication across classic and diskless fixture setup, not missing correctness checks

### Run 2026-04-09 27

Scope:

- reduce repeated single-node diskless Kafka fixture setup across the dedicated diskless Kafka integration suite

Changes made:

- added `newDisklessKafkaEnv(...)` in `test/integration/kafka_test_helpers.go` to create a single-node Kafka-enabled env, provision a diskless topic over Kafka admin, and verify the topic mode once
- migrated the dedicated diskless Kafka integration tests to that shared helper instead of repeating the same bootstrap sequence in each test

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_Kafka(ProduceAndFetch|MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp|FetchEmptyReportsWatermarks)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: diskless Kafka fixture setup is still Kafka-admin provisioned, with less repeated setup code

Cleanup completed:

- removed repeated single-node diskless Kafka env/bootstrap code from four integration tests

Follow-up gaps:

- broader integration cleanup can still unify some classic single-node Kafka setup, but that is optional deduplication rather than a correctness gap

### Run 2026-04-09 28

Scope:

- reduce repeated single-node classic Kafka fixture setup in the basic Kafka integration suite

Changes made:

- added `newKafkaFixtureEnv(...)` in `test/integration/kafka_test_helpers.go` for the common case of a single-node Kafka env with one classic topic provisioned over Kafka admin
- migrated several classic Kafka integration tests to that helper instead of repeating the same env creation and topic bootstrap sequence inline

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(IdempotentProduceWithFranzGo|CompressedProduceWithFranzGo|ConsumeTopicsWithListOffsets|ListOffsetsByTimestamp|FetchReportsOffsetWatermarks)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: classic single-node Kafka fixture setup remains Kafka-admin provisioned after helper deduplication

Cleanup completed:

- removed repeated single-node classic Kafka env/bootstrap code from five integration test paths

Follow-up gaps:

- the remaining cleanup opportunities in `kafka_basic_test.go` are mostly larger structural refactors and are optional

### Run 2026-04-09 29

Scope:

- reduce repeated single-node admin-only Kafka setup and remove helper overlap in the Kafka integration helpers

Changes made:

- added `newKafkaReadyEnv(...)` in `test/integration/kafka_test_helpers.go` for the common single-node admin-only case that just needs a Kafka listener up and reachable
- updated `newKafkaTopicBootstrappedEnv(...)` to build on top of `newKafkaFixtureEnv(...)` instead of repeating the same classic topic bootstrap internally
- migrated several admin/config tests in `test/integration/kafka_basic_test.go` to `newKafkaReadyEnv(...)`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ACLs|ACLsRejectInvalidRequests|CreateAndDeleteTopics|CreateTopicsRejectsRetentionBytes|CreateTopicsValidateOnlyDoesNotCreateTopic|ConfigAPIsRejectUnsupportedResourceTypes|DescribeCluster)$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: admin-only Kafka integration tests still exercise the same protocol behavior after setup deduplication

Cleanup completed:

- removed repeated single-node admin-only Kafka bootstrap code from several integration tests
- removed one layer of helper duplication between the classic fixture helpers

Follow-up gaps:

- the remaining cleanup opportunities are mostly broader file-structure refactors in `kafka_basic_test.go`, not stale behavior or incorrect setup

### Run 2026-04-09 30

Scope:

- continue reducing repeated single-topic fixture setup in the Kafka mutation and partition-admin integration tests

Changes made:

- migrated several classic topic-mutation tests in `test/integration/kafka_basic_test.go` to `newKafkaFixtureEnv(...)`
- migrated the diskless new-partition readiness test to `newDisklessKafkaEnv(...)`
- removed another block of repeated inline single-node env creation and one-topic bootstrap code from the same file

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(CreatePartitions(Classic|Diskless)NewPartitionIsReady|Kafka(AlterConfigsAndIncrementalAlterConfigs|CreatePartitionsValidateOnlyDoesNotMutate|AlterConfigsValidateOnlyDoesNotMutate|IncrementalAlterConfigsValidateOnlyDoesNotMutate|AlterConfigsRejectsRetentionBytes|IncrementalAlterConfigsRejectsRetentionBytes))$' -count=1 -timeout 120s`

Matrix status changes:

- confirmed: the Kafka mutation and partition-admin tests still cover the same behavior after fixture-helper deduplication

Cleanup completed:

- removed repeated classic and diskless single-topic fixture setup from another cluster of Kafka integration tests

Follow-up gaps:

- remaining cleanup is mostly about larger file organization in `kafka_basic_test.go`, not repeated semantics or stale setup paths

### Run 2026-04-09 31

Scope:

- create a dedicated canonical API status matrix under `docs/` for current provided, partial, missing, and unsupported behavior

Changes made:

- added `docs/api-support-matrix.md` as the primary status document for HTTP and Kafka API support
- separated `Verified`, `Partial`, `Gap`, and `Unsupported` status from the execution plan so the current product surface is easier to read
- summarized the main current evidence and remaining gaps for both HTTP and Kafka surfaces
- linked the execution plan back to the new canonical status document

Verification run:

- documentation readback only; no code or tests changed

Matrix status changes:

- the canonical matrix now lives in `docs/api-support-matrix.md`

Cleanup completed:

- removed the need to treat the execution plan itself as the primary status matrix

Follow-up gaps:

- the new matrix should be updated whenever tests move an API from `Partial` or `Gap` to `Verified`

### Run 2026-04-09 32

Scope:

- tighten Kafka transport and negotiation partial proofs without changing server behavior

Changes made:

- added exact `ApiVersions` min/max range verification in `internal/server/kafka_api_test.go`
- added direct unit coverage proving default `InitProducerID` rejects transactional IDs with `INVALID_REQUEST`
- added transport coverage proving `HandleConn` closes the connection on an unsupported API key
- updated `docs/api-support-matrix.md` to mark `ApiVersions` as verified and narrow the remaining `InitProducerID` gap

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestKafka(ApiVersionsAdvertisesExpectedVersionRanges|InitProducerIDRejectsTransactionalID)|TestKafkaHandleConn_ClosesOnUnsupportedAPIKey' -count=1`

Matrix status changes:

- `ApiVersions` moved from `Partial` to `Verified`
- `InitProducerID` remains `Partial`, but the remaining gap is now integration depth rather than basic handler behavior

Cleanup completed:

- converted one documentation-level Kafka protocol partial into direct unit evidence

Follow-up gaps:

- transport is still `Partial` until unsupported-version and framing-compatibility edges are explicitly tested

### Run 2026-04-09 33

Scope:

- continue narrowing Kafka transport partials with direct decode/write-path assertions

Changes made:

- added direct unit coverage proving `decodeKafkaRequest(...)` rejects unsupported API keys
- added direct unit coverage proving `writeKafkaResponse(...)` clamps the response version to the response max version even when the request version is much higher
- narrowed the transport note in `docs/api-support-matrix.md` to reflect what is now directly covered

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DecodeKafkaRequestRejectsUnsupportedAPIKey|WriteKafkaResponseClampsToResponseMaxVersion|KafkaHandleConn_ClosesOnUnsupportedAPIKey)' -count=1`

Matrix status changes:

- Kafka transport remains `Partial`, but unsupported API-key handling is now covered at both decode and connection levels

Cleanup completed:

- converted two more implicit transport assumptions into direct test evidence

Follow-up gaps:

- unsupported-version handling and flexible/non-flexible framing compatibility are still the main remaining transport proof gaps

### Run 2026-04-09 34

Scope:

- continue narrowing Kafka transport partials with unsupported-version tests

Changes made:

- added direct unit coverage proving `decodeKafkaRequest(...)` accepts a high request version for `ApiVersions`
- added transport coverage proving `HandleConn` responds to a high-version `ApiVersions` request instead of closing the connection
- updated `docs/api-support-matrix.md` to record the current high-version transport behavior accurately

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DecodeKafkaRequestAcceptsHighKnownVersion|KafkaHandleConn_RespondsToHighKnownVersion|DecodeKafkaRequestRejectsUnsupportedAPIKey|WriteKafkaResponseClampsToResponseMaxVersion|KafkaHandleConn_ClosesOnUnsupportedAPIKey)' -count=1`

Matrix status changes:

- Kafka transport remains `Partial`, but high-version request handling is now directly covered and documented accurately

Cleanup completed:

- converted another transport edge from an implicit assumption into direct test evidence

Follow-up gaps:

- flexible/non-flexible framing compatibility is still the main remaining transport proof gap

### Run 2026-04-09 35

Scope:

- strengthen the Kafka `InitProducerID` partial from dispatcher-only proof to direct integration evidence

Changes made:

- added `TestKafkaInitProducerIDIntegration` in `test/integration/kafka_basic_test.go`
- verified the real Kafka protocol path for:
  - non-transactional producer ID allocation
  - transactional ID rejection with `INVALID_REQUEST`
- updated `docs/api-support-matrix.md` to reflect that `InitProducerID` now has integration evidence

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaInitProducerIDIntegration$' -count=1 -timeout 120s`

Matrix status changes:

- `InitProducerID` remains `Partial`, but the remaining gap is now end-to-end diskless/idempotent semantics rather than missing direct protocol coverage

Cleanup completed:

- converted another Kafka protocol partial from implied behavior into direct integration evidence

Follow-up gaps:

- the next highest-value Kafka partial is still either diskless compressed produce or flexible/non-flexible framing compatibility

### Run 2026-04-09 36

Scope:

- strengthen the Kafka `Produce` partial by proving compressed produce works on diskless topics

Changes made:

- added `TestDiskless_KafkaCompressedProduce` in `test/integration/diskless_kafka_test.go`
- verified diskless Kafka produce/fetch for `snappy`, `gzip`, `lz4`, and `zstd`
- also verified HTTP consume can read those diskless compressed Kafka-written records correctly
- updated `docs/api-support-matrix.md` to narrow the remaining Kafka produce gap to diskless idempotent semantics rather than compression support

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestDiskless_KafkaCompressedProduce$' -count=1 -timeout 120s`

Matrix status changes:

- Kafka `Produce` remains `Partial`, but diskless compression support is now directly verified

Cleanup completed:

- converted another major Kafka produce assumption into direct integration evidence

Follow-up gaps:

- the main remaining Kafka produce gap is broader diskless idempotent semantics, not compression support

### Run 2026-04-09 37

Scope:

- close the Kafka `ListGroups` partial by proving its state/type filter behavior directly

Changes made:

- added `TestKafkaListGroupsFiltersByStateAndType` in `test/integration/kafka_group_test.go`
- verified:
  - baseline `ListGroups` returns the joined group with non-empty state and `consumer` type
  - matching `StatesFilter` plus `TypesFilter` returns the group
  - mismatched state filter excludes the group
  - mismatched type filter excludes the group
- updated `docs/api-support-matrix.md` to move `ListGroups` from `Partial` to `Verified`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaListGroups(FiltersByStateAndType|AndDescribeGroups)$' -count=1 -timeout 120s`

Matrix status changes:

- `ListGroups` moved from `Partial` to `Verified`

Cleanup completed:

- converted a remaining Kafka group introspection partial into explicit integration evidence

Follow-up gaps:

- the main remaining Kafka group-side partial is broader `DescribeGroups` rebalance-state coverage, not `ListGroups` filtering

### Run 2026-04-09 38

Scope:

- close the Kafka `CreatePartitions` partial by proving manual replica assignment rejection directly

Changes made:

- added `TestKafkaCreatePartitionsRejectsManualReplicaAssignment` in `test/integration/kafka_basic_test.go`
- verified Kafka `CreatePartitions` returns `INVALID_REPLICA_ASSIGNMENT` for manual assignment input
- verified the rejected request does not change topic partition metadata
- updated `docs/api-support-matrix.md` to move `CreatePartitions` from `Partial` to `Verified`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafkaCreatePartitions(RejectsManualReplicaAssignment|RejectsDecreaseAndLeavesMetadataUnchanged|ValidateOnlyDoesNotMutate)$|TestCreatePartitions(Classic|Diskless)NewPartitionIsReady$' -count=1 -timeout 120s`

Matrix status changes:

- `CreatePartitions` moved from `Partial` to `Verified`

Cleanup completed:

- converted the last major unproven Kafka `CreatePartitions` rule into direct integration evidence

Follow-up gaps:

- remaining Kafka partials are now mostly transport framing compatibility, `DescribeGroups` rebalance-state depth, diskless delete cleanup audit, and broader config-admin unsupported-config coverage

### Run 2026-04-09 39

Scope:

- narrow the remaining `DescribeGroups` partial by proving its rebalance-state transition directly

Changes made:

- added `TestKafkaDescribeGroupsReflectsRebalanceStateTransitions` in `test/integration/kafka_group_test.go`
- verified `DescribeGroups` reports:
  - `PreparingRebalance` after `JoinGroup` and before `SyncGroup`
  - `Stable` after leader assignment is installed through `SyncGroup`
- added a shared consumer-assignment helper in `test/integration/kafka_test_helpers.go` to avoid re-encoding member assignment bytes inline
- updated `docs/api-support-matrix.md` to narrow the remaining `DescribeGroups` gap to broader multi-member rebalance coverage

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(DescribeGroupsReflectsRebalanceStateTransitions|ListGroups(FiltersByStateAndType|AndDescribeGroups))$' -count=1 -timeout 120s`

Matrix status changes:

- `DescribeGroups` remains `Partial`, but the remaining gap is now specifically broader multi-member rebalance depth rather than basic state transitions

Cleanup completed:

- removed one more inline group-assignment encoding pattern from the integration suite

Follow-up gaps:

- the next highest-value Kafka partial is likely diskless delete cleanup audit or broader config-admin unsupported-config coverage

### Run 2026-04-09 40

Scope:

- close the Kafka `DeleteTopics` partial by auditing diskless metadata cleanup through the Kafka delete path

Changes made:

- added `TestHandleKafkaDeleteTopicsCleansDisklessMeta` in `internal/server/server_test.go`
- seeded real diskless metastore state for a diskless topic, deleted it through Kafka `DeleteTopics`, and verified:
  - topic metadata is removed
  - diskless partition head resets
  - diskless segment references are removed
- updated `docs/api-support-matrix.md` to move `DeleteTopics` from `Partial` to `Verified`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestHandleKafkaDeleteTopics(CleansDisklessMeta|RequiresController)$' -count=1`

Matrix status changes:

- `DeleteTopics` moved from `Partial` to `Verified`

Cleanup completed:

- converted the remaining diskless delete-audit gap into direct server-level Kafka-path evidence

Follow-up gaps:

- remaining Kafka partials are now mainly transport framing compatibility, broader config-admin unsupported-config coverage, and broader multi-member `DescribeGroups` rebalance depth

- flexible/non-flexible framing compatibility is now the main remaining transport proof gap

### Run 2026-04-09 41

Scope:

- correct diskless topic deletion ordering so cleanup is async, resumable, and clears diskless metastore state only after S3 topic data is gone

Changes made:

- replaced the old synchronous `deleteTopic` flow with a durable topic-deletion marker under `_coordination/topic_deletions/`
- changed topic deletion to:
  - persist the deletion marker with the topic config
  - remove topic metadata immediately so the topic disappears from HTTP and Kafka metadata paths
  - defer S3 object cleanup and diskless metastore cleanup to leader GC
- added a pending-delete GC pass that:
  - deletes topic S3 data, assignment files, and epoch files first
  - only then deletes diskless metastore state
  - removes the marker last so cleanup is resumable across interruptions and restarts
- added runtime teardown for deleted topics so local partition state and routing caches are dropped immediately
- tightened create/consume behavior:
  - topic recreation is rejected while a deletion marker exists
  - low-level consume now revalidates topic existence instead of trusting stale local runtime state
- removed the stale synchronous-delete assumptions from the server test suite
- updated `docs/api-support-matrix.md` so both HTTP and Kafka delete rows describe the actual async/resumable contract

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DeleteTopicEnqueuesAsyncDisklessCleanupAndPreservesMetaUntilS3Deleted|TopicDeletionGCResumesFromMarkerAfterRestart|CreateTopicRejectsPendingDeletion|HandleConsumeLowLevelRejectsDeletedTopicDespiteStaleRuntime|HandleKafkaDeleteTopicsEnqueuesDisklessCleanup|HandleDeleteTopicEnqueuesAsyncCleanup)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(Handle(DeleteTopicEnqueuesAsyncCleanup|DeleteTopic_NotFound|KafkaDeleteTopicsEnqueuesDisklessCleanup|KafkaDeleteTopicsRequiresController)|HandleConsumeLowLevelRejectsDeletedTopicDespiteStaleRuntime|CreateTopicRejectsPendingDeletion|DeleteTopicEnqueuesAsyncDisklessCleanupAndPreservesMetaUntilS3Deleted|TopicDeletionGCResumesFromMarkerAfterRestart|DisklessRetentionCleanupDeletesExpiredDataAndAdvancesEarliestOffset)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(Topic(CRUD|CreateDuplicate|ReadResponsesExposeStorageMode)|Kafka(CreateAndDeleteTopics|AdminMutationsOnFollowerReturnNotController))$' -count=1 -timeout 120s`

Matrix status changes:

- HTTP `DELETE /v1/topics/{topic}` moved from `Partial` to `Verified`
- Kafka `DeleteTopics` stays `Verified`, but the documented contract is now corrected to async/resumable S3-first cleanup instead of immediate diskless metastore deletion

Cleanup completed:

- removed stale synchronous-delete assumptions from the delete-focused server tests
- removed the remaining direct-consume reliance on stale local runtime after topic deletion

Follow-up gaps:

- remaining Kafka partials are still transport framing compatibility, broader config-admin unsupported-config coverage, and broader multi-member `DescribeGroups` rebalance depth

### Run 2026-04-09 42

Scope:

- reduce `internal/server/server.go` size by extracting low-risk Kafka admin/topic-config logic and coordination GC helpers into dedicated files without behavior changes

Changes made:

- moved coordination cleanup helpers from `internal/server/server.go` to `internal/server/coordination_gc.go`
- moved Kafka topic-admin/config logic from `internal/server/server.go` to `internal/server/kafka_topic_admin.go`
- kept method signatures unchanged so the surrounding server wiring and tests remain intact
- trimmed stale imports left behind in `internal/server/server.go`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(Kafka(CreateTopicRequestRejectsRetentionBytes|CreateTopicRequestAcceptsDisklessStorageMode|HandleKafka(CreateTopicsRequiresController|DeleteTopicsRequiresController))|DisklessRetentionCleanupDeletesExpiredDataAndAdvancesEarliestOffset|DeleteTopicEnqueuesAsyncDisklessCleanupAndPreservesMetaUntilS3Deleted|TopicDeletionGCResumesFromMarkerAfterRestart)$' -count=1`

Matrix status changes:

- none; this was a structural cleanup run only

Cleanup completed:

- removed another large mixed-responsibility block from `internal/server/server.go`

Follow-up gaps:

- `internal/server/server.go` still contains additional extractable areas, especially Kafka ACL/admin-adjacent handlers and cluster/controller helper logic

### Run 2026-04-09 43

Scope:

- continue shrinking `internal/server/server.go` by moving Kafka ACL admin handlers and their request/filter helpers into a dedicated file

Changes made:

- moved Kafka ACL handlers and ACL request/filter helpers from `internal/server/server.go` to `internal/server/kafka_acl_admin.go`
- preserved the existing handler and helper behavior, including follower `NOT_CONTROLLER` behavior and invalid-request validation paths
- kept the rest of `server.go` unchanged apart from removing the extracted block

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(Kafka(ACLsRejectInvalidRequests|HandleKafka(CreateTopicsRequiresController|DeleteTopicsRequiresController))|KafkaHandleConn_ClosesOnUnsupportedAPIKey)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ACLs|ACLsRejectInvalidRequests|ACLMutationsOnFollowerReturnNotController)$' -count=1 -timeout 120s`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- none; this was another structural cleanup run only

Cleanup completed:

- removed another mixed-responsibility block from `internal/server/server.go`

Follow-up gaps:

- `internal/server/server.go` still has more extractable metadata/discovery and controller-helper logic, but the highest-value mixed admin blocks are now out

### Run 2026-04-09 44

Scope:

- continue shrinking `internal/server/server.go` by moving Kafka metadata/discovery and list-offsets helpers into a dedicated file

Changes made:

- moved Kafka metadata/discovery and offset-lookup logic from `internal/server/server.go` to `internal/server/kafka_metadata_discovery.go`
- extracted:
  - `handleKafkaMetadata`
  - `handleKafkaFindCoordinator`
  - `kafkaControllerBroker`
  - `handleKafkaListOffsets`
  - timestamp-search helpers for list-offsets
  - timestamp normalization helper
- trimmed import fallout from `internal/server/server.go`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(Kafka(ApiVersionsAdvertisesExpectedVersionRanges|ControllerBrokerUsesLeaderLease|HandleKafkaListOffsets_(DisklessTimestampLookupReturnsInvalidRequest|ByTimestampFromWAL|ReplicatedPartitionNotReady))|HandleKafkaMetadataIncludesUnknownRequestedTopic|DisklessRetentionCleanupDeletesExpiredDataAndAdvancesEarliestOffset)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(MetadataAdvertisesLeaderAndBroker|ListOffsetsByTimestamp|Diskless_Kafka(MetadataIncludesUnknownRequestedTopic|ListOffsetsEarliestLatestAndTimestamp))$' -count=1 -timeout 120s`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- none; this was another structural cleanup run only

Cleanup completed:

- removed another large mixed Kafka protocol block from `internal/server/server.go`

Follow-up gaps:

- the remaining `internal/server/server.go` bulk is now mostly group/offset/controller coordination logic rather than mixed admin/discovery code

### Run 2026-04-09 45

Scope:

- continue shrinking `internal/server/server.go` by moving Kafka offset APIs and their small coordinator/partition helpers into a dedicated file

Changes made:

- moved Kafka offset logic from `internal/server/server.go` to `internal/server/kafka_offsets.go`
- extracted:
  - `handleKafkaOffsetDelete`
  - `handleKafkaOffsetCommit`
  - `handleKafkaOffsetFetch`
  - `isLocalKafkaCoordinator`
  - `currentControllerEpoch`
  - `kafkaPartitionError`
  - `kafkaPartitionExists`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(KafkaHandleKafkaDeleteTopicsRequiresController|HandleKafkaListOffsets_DisklessTimestampLookupReturnsInvalidRequest)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(OffsetCommitFetchWithFranzGoRequests|ListGroupsAndDescribeGroups|DeleteGroups|ACLs)$' -count=1 -timeout 120s`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- none; this was another structural cleanup run only

Cleanup completed:

- removed another Kafka protocol block from `internal/server/server.go`

Follow-up gaps:

- `internal/server/server.go` is now mostly lifecycle, assignment, and remaining group/controller orchestration rather than mixed protocol handlers

### Run 2026-04-09 46

Scope:

- continue shrinking `internal/server/server.go` by moving Kafka group/coordinator handler wrappers into a dedicated file

Changes made:

- moved Kafka group/coordinator handler wrappers from `internal/server/server.go` to `internal/server/kafka_group_handlers.go`
- extracted:
  - `handleKafkaJoinGroup`
  - `handleKafkaDescribeGroups`
  - `handleKafkaListGroups`
  - `handleKafkaDeleteGroups`
  - `handleKafkaSyncGroup`
  - `handleKafkaHeartbeat`
  - `handleKafkaLeaveGroup`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(FindCoordinatorAndGroupLifecycle|ListGroupsAndDescribeGroups|ListGroupsFiltersByStateAndType|DescribeGroupsReflectsRebalanceStateTransitions|DeleteGroups|HeartbeatSurvivesLeaderFailover)$' -count=1 -timeout 120s`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- none; this was another structural cleanup run only

Cleanup completed:

- removed another protocol-handler block from `internal/server/server.go`

Follow-up gaps:

- `internal/server/server.go` is now close to lifecycle, address helpers, and assignment/controller internals rather than mixed Kafka protocol surface

### Run 2026-04-09 47

Scope:

- strengthen the Kafka transport/framing negative matrix with direct low-level tests

Changes made:

- added transport-level coverage in `internal/server/kafka_transport_test.go` for:
  - truncated flexible request decode rejection
  - flexible response-header tagged-fields byte emission
  - non-flexible response-header behavior
  - invalid frame length causing connection close
  - truncated flexible request causing connection close
- updated `docs/api-support-matrix.md` so the transport row reflects the narrower remaining gap

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DecodeKafkaRequest(RejectsUnsupportedAPIKey|AcceptsHighKnownVersion|RejectsTruncatedFlexibleRequest)|WriteKafkaResponse(ClampsToResponseMaxVersion|WritesTaggedHeaderForFlexibleRequest|OmitsTaggedHeaderForNonFlexibleRequest)|KafkaHandleConn_(ClosesOnUnsupportedAPIKey|ClosesOnInvalidFrameLength|RejectsTruncatedFlexibleRequest|RespondsToHighKnownVersion))$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- Kafka transport/framing remains `Partial`, but the remaining gap is now narrower: broader malformed-on-reused-connection and additional version-edge cases

Cleanup completed:

- none; this was a verification-focused run

Follow-up gaps:

- next best API-support verification targets are unsupported config-name matrix, broader multi-member `DescribeGroups`, broader ACL matrix, and additional reused-connection transport corruption cases

### Run 2026-04-09 48

Scope:

- continue the Kafka transport/framing negative matrix with explicit reused-connection behavior

Changes made:

- added transport-level coverage in `internal/server/kafka_transport_test.go` for:
  - oversized frame length causing connection close
  - serving multiple valid requests on the same connection
  - malformed second request on a reused connection causing close
- updated `docs/api-support-matrix.md` to reflect that reused-connection framing behavior is now directly covered

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'TestKafkaHandleConn_(ClosesOnInvalidFrameLength|ClosesOnOversizedFrameLength|RejectsTruncatedFlexibleRequest|ServesMultipleRequestsOnSameConnection|ClosesOnMalformedSecondRequestOnReusedConnection)$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- Kafka transport/framing remains `Partial`, but the remaining gap is now mostly additional version-edge coverage

Cleanup completed:

- none; this was a verification-focused run

Follow-up gaps:

- next best API-support verification targets are unsupported config-name matrix, broader multi-member `DescribeGroups`, broader ACL matrix, and additional Kafka version-edge coverage

### Run 2026-04-09 49

Scope:

- continue the Kafka transport matrix with explicit version-edge coverage on a non-trivial API (`Metadata`)

Changes made:

- added transport-level coverage in `internal/server/kafka_transport_test.go` proving that current transport behavior:
  - accepts negative `Metadata` versions at decode time
  - accepts future `Metadata` versions at decode time
  - responds on connection for both negative and future `Metadata` versions
- updated `docs/api-support-matrix.md` so the transport row reflects the real observed version behavior instead of an assumed strict rejection model

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -run 'Test(DecodeKafkaRequest(AcceptsNegativeMetadataVersion|AcceptsFutureMetadataVersion)|KafkaHandleConn_(RespondsToNegativeMetadataVersion|RespondsToFutureMetadataVersion))$' -count=1`
- `env GOCACHE=/tmp/camu-go-cache go test ./internal/server -count=1`

Matrix status changes:

- Kafka transport/framing remains `Partial`, but the remaining gap is now characterized more accurately as broader per-API version-surface exhaustiveness

Cleanup completed:

- none; this was a verification-focused run

Follow-up gaps:

- next best API-support verification targets are unsupported config-name matrix, broader multi-member `DescribeGroups`, broader ACL matrix, and broader per-API Kafka version-surface coverage

### Run 2026-04-09 50

Scope:

- close five more Kafka protocol proof steps: unsupported config-name behavior, broader ACL filter behavior, and multi-member `DescribeGroups` rebalance depth

Changes made:

- added `TestKafkaDescribeConfigsFiltersUnknownConfigNames` in `test/integration/kafka_basic_test.go`
- added `TestKafkaAlterConfigsRejectsUnsupportedConfigNameWithoutMutation` in `test/integration/kafka_basic_test.go`
- added `TestKafkaIncrementalAlterConfigsRejectsUnsupportedConfigNameWithoutMutation` in `test/integration/kafka_basic_test.go`
- added `TestKafkaACLsFilterMatrix` in `test/integration/kafka_basic_test.go`
- added `TestKafkaDescribeGroupsReflectsTwoMemberRebalance` in `test/integration/kafka_group_test.go`
- updated `docs/api-support-matrix.md` to:
  - move `DescribeGroups` from `Partial` to `Verified`
  - narrow the remaining config-admin gap from unsupported-name proof to broader exhaustiveness
  - narrow the ACL gap from basic filter proof to broader resource/operation breadth

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'TestKafka(ACLs(FilterMatrix|RejectInvalidRequests)|DescribeConfigsFiltersUnknownConfigNames|AlterConfigsRejectsUnsupportedConfigNameWithoutMutation|IncrementalAlterConfigsRejectsUnsupportedConfigNameWithoutMutation|DescribeGroupsReflectsTwoMemberRebalance)$' -count=1 -timeout 120s`

Matrix status changes:

- `DescribeGroups` moved from `Partial` to `Verified`
- `DescribeConfigs`, `AlterConfigs`, and `IncrementalAlterConfigs` remain `Partial`, but the unsupported-config-name gap is now directly covered
- ACL APIs remain `Partial`, but `MATCH` filter behavior is now directly covered

Cleanup completed:

- none; this was a verification-focused run

Follow-up gaps:

- broader Kafka ACL resource/operation matrix
- broader diskless `ListOffsets` version/client behavior
- broader per-API Kafka version-surface coverage

### Run 2026-04-09 51

Scope:

- continue narrowing the Kafka protocol partials for ACLs and diskless `ListOffsets`

Changes made:

- added `TestDiskless_KafkaListOffsetsVersionCompatibility` in `test/integration/diskless_kafka_test.go`
- verified that diskless `ListOffsets`:
  - uses legacy `OldStyleOffsets` on version `0`
  - returns `INVALID_REQUEST` for unsupported timestamp lookup on versions `0`, `1`, and `4`
- added `TestKafkaACLsResourceAndOperationMatrix` in `test/integration/kafka_basic_test.go`
- verified group and cluster ACL create/describe/delete filtering in addition to the earlier topic and `MATCH` coverage
- updated `docs/api-support-matrix.md` to narrow the remaining gaps for ACLs and diskless `ListOffsets`

Verification run:

- `env GOCACHE=/tmp/camu-go-cache go test -tags integration ./test/integration -run 'Test(Diskless_KafkaListOffsetsVersionCompatibility|KafkaACLsResourceAndOperationMatrix|KafkaACLsFilterMatrix)$' -count=1 -timeout 120s`

Matrix status changes:

- `ListOffsets` timestamp lookup remains `Partial`, but the diskless unsupported path is now covered across legacy and newer request versions
- ACL APIs remain `Partial`, but the current proven subset now includes topic, group, and cluster resources plus `MATCH` and exact filtering

Cleanup completed:

- none; this was a verification-focused run

Follow-up gaps:

- broader per-API Kafka version-surface coverage
- broader config-admin duplicate/exhaustiveness matrix
- broader ACL matrix outside the currently proven subset
