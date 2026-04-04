# Camu TODO

## Coordination Bugs (verified, not yet fixed)

- [ ] **CRITICAL: `checkISRLag` data race** — reads `ps.isLeader`, `ps.replicaState`, `ps.epoch` without `ps.mu` (`server.go:1474`). Fix: add `ps.mu.RLock`.
- [ ] **CRITICAL: `attemptPartitionLeadership` data race** — reads `ps.fetchCancel`/`ps.fetchDone` without `ps.mu` (`server.go:1292`). Fix: copy pattern from `initPartitionAsLeader:664`.
- [ ] **HIGH: HW regression on failover** — `attemptPartitionLeadership` sets recovered HW from local log/index state but omits `isrState.HighWatermark`. `initPartitionAsLeader` does this correctly.
- [ ] **HIGH: ISR expansion not persisted** — `UpdateFollower` adds to ISR in memory but never writes to S3 (`replica_state.go:85`). Only ISR shrinkage triggers S3 write.
- [ ] **HIGH: TOCTOU in `initPartitionAsLeader`** — checks `ps.isLeader` under `RLock`, then proceeds unlocked (`server.go:649`). Fix: re-check under `Lock`.
- [ ] **HIGH: Phantom leader** — produce fencing uses local cache (`s.myPartitions`) updated on renewal tick. Old leader accepts writes for up to `heartbeat_interval` after CAS loss.
- [ ] **MEDIUM: `state.json` flush without CAS** — stale leader can overwrite new leader's state
- [ ] **MEDIUM: ISR writes use unconditional PUT** — no CAS guard, last writer wins under split brain
- [ ] **MEDIUM: Epoch history `StartOffset`** — uses recovered local log end instead of `recoveredHW`

## ISR Replication — Post-Review Fixes

### Critical

- [x] Guard unsigned underflow in ISR expansion (`internal/replication/replica_state.go:70`) — add `offset <= rs.leaderOffset` before subtraction
- [x] Remove dead `time.AfterFunc` in purgatory (`internal/replication/purgatory.go:39-43`) — callback is empty, `time.After` on line 54 handles timeout
- [x] Remove duplicate `SetLeaderOffset`/`NotifyNewData` in low-level produce (`internal/server/handlers_produce.go:302-305`) — `AppendBatch` already calls both

## Important

- [x] Log `WriteMessageFrames` error in `handleReplicaFetch` (`internal/server/handlers_replicate.go:104`)
- [x] Set `Content-Type: application/json` in `writeJSON` (`internal/server/handlers_topic.go:38`)
- [x] Extract ISR expansion threshold (1000) to config or named constant (`internal/replication/replica_state.go:71`)
- [x] Make ISR lag timeout configurable, currently hardcoded 30s (`internal/server/server.go:1080`)
- [x] Make replication/purgatory timeout configurable, currently hardcoded 30s (`internal/server/handlers_produce.go:166,307`)
- [ ] Add `entryLen` bounds check in replication protocol, cap at 64MB

## High-Throughput (10MB/s target)

- [ ] **In-memory buffer for flush path** — keep unsealed native batches in a ring buffer to reduce rereads during flush/recovery
- [ ] **Pipeline S3 uploads** — `onFlush` blocks on `s3Client.Put` for 50-200ms (`partition_manager.go:652`); decouple accumulation from upload with per-partition upload queue
- [x] **Batch index updates** — every flush does GET+PUT to S3 for index.json (`partition_manager.go:673-727`); batch updates across N segments or use append-only format
- [ ] **`sync.Pool` for segment buffers** — `WriteSegment` allocates new `bytes.Buffer` per message frame (`segment.go:42-47`); pool and reuse buffers to reduce GC pressure
- [ ] **Binary produce protocol** — JSON parsing allocates heavily with string→[]byte copies (`handlers_produce.go:54-70`); add protobuf or custom framing for high-throughput clients
- [ ] **Server-side request coalescing** — no linger across concurrent HTTP requests; hold response for up to N ms, coalesce into fewer active-segment appends/fsyncs
- [x] **Multi-segment fetch** — when consumer requests more messages than a single segment holds, load multiple segments from S3/disk in one fetch response. Currently requires one request per segment boundary

## Diskless Topics (WarpStream-style)

Leaderless topic type where S3 is the sole durable storage layer. Any broker accepts produces for any partition. Viable for workloads tolerating ~1s latency.

- [ ] **In-memory batch buffer** — accumulate records in memory per partition. Flush to S3 on interval (~1s) or size threshold (~1-10MB)
- [ ] **S3-as-sequencer** — on flush, CAS the partition index to allocate offsets (existing `ConditionalPut` + ETags). Works at 1 PUT/s/partition with low contention
- [ ] **Pluggable sequencer interface** — `AllocateOffsets(topic, partition, count) -> (startOffset, etag, error)`. Start with S3, swap to DynamoDB/etcd when throughput demands it
- [ ] **Leaderless produce path** — any broker writes to any partition. No leader election, no ISR, no replication (S3 = 11 nines durability)
- [ ] **Direct S3 consume path** — consumers read partition index then fetch segments from S3. Broker optional (just proxies or serves from cache)
- [ ] **Topic type config** — `storage_mode: "diskless" | "replicated"` in topic metadata. Different produce/consume paths per type
- [ ] **Pluggable coordination storage** — abstract lease acquisition and sequencer behind an interface. Current S3 CAS works but becomes a bottleneck at scale. Backends: S3 (default), DynamoDB, etcd, FoundationDB, PostgreSQL advisory locks

## Request Proxying

Replace 307 redirects with internal proxying — any broker accepts any request and forwards to the partition owner internally.

- [x] **Internal HTTP/2 connection pool** — persistent h2/h2c connections between brokers. Shared for both request proxying and replica fetches. Multiplexed streams eliminate head-of-line blocking across partitions
- [x] **Proxy produce/consume to partition owner** — broker looks up owner from assignment table, forwards request, returns response. One round trip instead of two
- [x] **Single LB endpoint** — clients hit any broker via one DNS name. No need to know topology. Kubernetes-friendly

## Consumer Groups

Client-side consumer groups using S3 leases for coordination. No server-side group coordinator.

- [x] **Offset commit/fetch API** — `POST /v1/groups/{gid}/commit`, `GET /v1/groups/{gid}/offsets`. Store at `s3://bucket/offsets/{group_id}/{topic}/{partition}`
- [ ] **Group membership via S3 leases** — consumers heartbeat a lease key at `s3://groups/{gid}/members/{cid}`. Dead when lease expires
- [ ] **Coordinator election** — first member to CAS a coordinator lease becomes coordinator. Reads live members, computes partition assignment (range/round-robin), writes assignments to member keys
- [ ] **Rebalance on membership change** — coordinator detects join/leave via lease expiry on sweep, reassigns partitions. Stop-the-world initially, cooperative/incremental later
- [ ] **Client library** — consumer group logic lives client-side. Server only provides offset storage and lease primitives

## Idempotent Produce (Exactly-Once)

- [x] **`Batch` struct** — per-batch producer metadata (`ProducerID`, `Sequence`)
- [x] **Idempotency manager** — per-(producer, partition) sequence tracking, S3-based ID allocation, checkpoint/load/rebuild, stale eviction
- [x] **Segment batch header v2** — 16 bytes producer metadata per batch
- [x] **Batch metadata propagation** — carries `ProducerID`/`Sequence` through native batch and replication format
- [x] **`POST /v1/producers/init`** — S3 atomic counter for globally unique producer IDs
- [x] **Idempotency gate** — partition-specific endpoint only; duplicate → join replication purgatory → `{"duplicate": true}`; gap → 422
- [x] **S3 per-partition checkpoint** — uploaded during flush, downloaded on leader promotion
- [x] **Native recovery filtered by HW** — only committed batches rebuild idempotency state on failover
- [x] **Stale producer eviction** — 30min TTL, runs on coordination tick
- [x] **Jepsen exactly-once checker** — 7 test scenarios (combined faults, high concurrency, pause, S3 degradation, membership, strict quorum, soak)
- [ ] **Per-partition sequence in segment flush** — segment batches carry `ProducerID`/`Sequence` in header but currently write 0/0; wire actual metadata through flush path for segment-level dedup on read
- [ ] **Idempotent produce on high-level endpoint** — currently rejected with 400; could support by requiring client-side partition routing or by tracking sequences per-request (not per-partition)

## Future: Protocol & Transport

- [ ] **Fast failure detection** — HTTP/2 PING health checking + connection error classification in fetcher (plan at `docs/superpowers/plans/2026-04-02-fast-failure-detection.md`)
- [ ] **Internal TCP replication** — push-based, zero-copy RecordBatch forwarding (spec at `docs/superpowers/specs/2026-03-27-internal-tcp-protocol-design.md`)
- [x] **Kafka protocol support** — broker-compatible subset for produce, fetch, metadata, groups, and admin flows
- [x] **Kafka RecordBatch as canonical format** — native format for produce/fetch/replicate

## Suggestions

- [x] Map iteration non-determinism in header serialization
- [x] Add concurrency protection to `EpochHistory` or document invariant (`internal/replication/epoch_history.go`)
- [x] Add test for `CheckDivergence` "future epoch" edge case
- [x] Add comment about `Purgatory.Complete` double-close safety under current locking
- [x] Use `context.Context` in `Purgatory.Wait` for client disconnect cleanup
- [ ] Allow separate S3 buckets for coordination/leases vs data segments/indexes, so lease traffic can use a different bucket class such as S3 Express One Zone
