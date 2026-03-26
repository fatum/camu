# ISR Replication — Post-Review Fixes

## Critical

- [x] Guard unsigned underflow in ISR expansion (`internal/replication/replica_state.go:70`) — add `offset <= rs.leaderOffset` before subtraction
- [x] Remove dead `time.AfterFunc` in purgatory (`internal/replication/purgatory.go:39-43`) — callback is empty, `time.After` on line 54 handles timeout
- [x] Remove duplicate `SetLeaderOffset`/`NotifyNewData` in low-level produce (`internal/server/handlers_produce.go:302-305`) — `AppendBatch` already calls both

## Important

- [x] Log `WriteMessageFrames` error in `handleReplicaFetch` (`internal/server/handlers_replicate.go:104`)
- [x] Set `Content-Type: application/json` in `writeJSON` (`internal/server/handlers_topic.go:38`)
- [x] Extract ISR expansion threshold (1000) to config or named constant (`internal/replication/replica_state.go:71`)
- [x] Make ISR lag timeout configurable, currently hardcoded 30s (`internal/server/server.go:1080`)
- [x] Make replication/purgatory timeout configurable, currently hardcoded 30s (`internal/server/handlers_produce.go:166,307`)
- [ ] Add `entryLen` bounds check in wire protocol, cap at 64MB (`internal/replication/wire.go:54`)
- [ ] Note: `WAL.ReadFrom` replays entire WAL per fetch — optimization target (`internal/log/wal_reader.go:5-22`)

## High-Throughput (10MB/s target)

- [ ] **In-memory buffer for flush path** — `onFlush` replays entire WAL from disk (`partition_manager.go:600`); keep unflushed messages in a ring buffer, use WAL only for crash recovery
- [x] **Grouped fsync (linger.ms)** — currently every `AppendBatch` fsyncs independently (`wal.go:127-131`); accumulate writes and fsync on interval (e.g. 5ms) or size threshold to amortize disk latency

- [x] **Segmented WAL** — `TruncateBefore` rewrites entire WAL file while holding lock (`wal.go:187-261`); use multiple small WAL files, truncation = delete old files
- [ ] **Pipeline S3 uploads** — `onFlush` blocks on `s3Client.Put` for 50-200ms (`partition_manager.go:652`); decouple accumulation from upload with per-partition upload queue
- [ ] **Batch index updates** — every flush does GET+PUT to S3 for index.json (`partition_manager.go:673-727`); batch updates across N segments or use append-only format
- [ ] **`sync.Pool` for segment buffers** — `WriteSegment` allocates new `bytes.Buffer` per message frame (`segment.go:42-47`); pool and reuse buffers to reduce GC pressure
- [ ] **Binary produce protocol** — JSON parsing allocates heavily with string→[]byte copies (`handlers_produce.go:54-70`); add protobuf or custom framing for high-throughput clients
- [ ] **Server-side request coalescing** — no linger across concurrent HTTP requests; hold response for up to N ms, coalesce into single WAL write + fsync (standard Kafka/Redpanda approach)
- [x] **Multi-segment fetch** — when consumer requests more messages than a single segment holds, load multiple segments from S3/disk in one fetch response. Currently requires one request per segment boundary

## Diskless Topics (WarpStream-style)

Leaderless, zero-WAL topic type where S3 is the sole storage and durability layer. Any broker accepts produces for any partition. Viable for workloads tolerating ~1s latency.

- [ ] **In-memory batch buffer** — accumulate records in memory per partition, no WAL. Flush to S3 on interval (~1s) or size threshold (~1-10MB)
- [ ] **S3-as-sequencer** — on flush, CAS the partition index to allocate offsets (existing `ConditionalPut` + ETags). Works at 1 PUT/s/partition with low contention
- [ ] **Pluggable sequencer interface** — `AllocateOffsets(topic, partition, count) -> (startOffset, etag, error)`. Start with S3, swap to DynamoDB/etcd when throughput demands it
- [ ] **Leaderless produce path** — any broker writes to any partition. No leader election, no ISR, no replication (S3 = 11 nines durability)
- [ ] **Direct S3 consume path** — consumers read partition index then fetch segments from S3. Broker optional (just proxies or serves from cache)
- [ ] **Topic type config** — `storage_mode: "diskless" | "replicated"` in topic metadata. Different produce/consume paths per type
- [ ] **Pluggable coordination storage** — abstract lease acquisition and sequencer behind an interface. Current S3 CAS works but becomes a bottleneck at scale. Backends: S3 (default), DynamoDB, etcd, FoundationDB, PostgreSQL advisory locks

## Request Proxying

Replace 307 redirects with internal proxying — any broker accepts any request and forwards to the partition owner internally.

- [ ] **Internal HTTP/2 connection pool** — persistent h2/h2c connections between brokers. Shared for both request proxying and replica fetches. Multiplexed streams eliminate head-of-line blocking across partitions
- [ ] **Proxy produce/consume to partition owner** — broker looks up owner from assignment table, forwards request, returns response. One round trip instead of two
- [ ] **Single LB endpoint** — clients hit any broker via one DNS name. No need to know topology. Kubernetes-friendly
- [ ] **Optional direct mode** — `?redirect=true` for clients that want to bypass proxy and go direct to partition owner

## Consumer Groups

Client-side consumer groups using S3 leases for coordination. No server-side group coordinator.

- [ ] **Offset commit/fetch API** — `POST /v1/groups/{gid}/offsets`, `GET /v1/groups/{gid}/offsets`. Store at `s3://bucket/offsets/{group_id}/{topic}/{partition}`
- [ ] **Group membership via S3 leases** — consumers heartbeat a lease key at `s3://groups/{gid}/members/{cid}`. Dead when lease expires
- [ ] **Coordinator election** — first member to CAS a coordinator lease becomes coordinator. Reads live members, computes partition assignment (range/round-robin), writes assignments to member keys
- [ ] **Rebalance on membership change** — coordinator detects join/leave via lease expiry on sweep, reassigns partitions. Stop-the-world initially, cooperative/incremental later
- [ ] **Client library** — consumer group logic lives client-side. Server only provides offset storage and lease primitives

## Suggestions

- [x] Map iteration non-determinism in header serialization (`internal/replication/wire.go:98-106`)
- [x] Add concurrency protection to `EpochHistory` or document invariant (`internal/replication/epoch_history.go`)
- [x] Add test for `CheckDivergence` "future epoch" edge case
- [x] Add comment about `Purgatory.Complete` double-close safety under current locking
- [x] Use `context.Context` in `Purgatory.Wait` for client disconnect cleanup
- [ ] Allow separate S3 buckets for coordination/leases vs data segments/indexes, so lease traffic can use a different bucket class such as S3 Express One Zone
