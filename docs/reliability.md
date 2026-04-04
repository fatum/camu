# Reliability and Correctness

Camu's correctness model is built on three verification layers: unit tests, integration tests, and a repository-local Jepsen harness. Every distributed behavior claim has a reproducible artifact.

## Correctness Claims

For replicated topics (`rf > 1`), Camu claims:

- **Acknowledged writes survive the checked Jepsen fault matrix.** Every acknowledged produce is present in the final partition drain after recovery.
- **Leader failover preserves committed prefixes.** The high watermark only advances when the ISR quorum confirms, and promoted leaders recover native local tail data correctly.
- **No ghost reads.** Consumers never observe data that was not acknowledged by a successful produce.
- **Offset monotonicity.** Offsets within a partition never duplicate, regress, or have gaps.
- **High watermark monotonicity.** The observable high watermark never goes backward.

For `rf=1`, the claim is narrower: local active-segment recovery and sealed-segment durability are covered by integration tests, but there is no replication-based availability guarantee.

## Verification Layers

### Unit Tests

Unit coverage focuses on the correctness of individual components:

- Active-segment recovery and truncation at RecordBatch boundaries
- Segment encoding, decoding, and sidecar generation
- Epoch fencing and leader initialization
- Partition append serialization and offset assignment
- Idempotent producer sequence tracking, deduplication, and checkpoint recovery
- RecordBatch codec (encode/decode with gzip, snappy, lz4, zstd compression)
- Multi-source consume iterator merge and deduplication
- Leadership proxy and routing logic

### Integration Tests

Integration coverage exercises end-to-end paths with real servers:

- Restart recovery from local active segments
- Flush persistence through object storage
- Leader failover and reassignment
- HTTP and Kafka protocol interoperability
- Consumer-group coordinator recovery across failover
- Idempotent produce deduplication with replication purgatory

### Jepsen

Jepsen is the strongest evidence for distributed behavior. The harness runs a five-node Docker cluster against MinIO-backed storage.

**22 passing fault scenarios:**

| Mode | Faults | Duration | Availability |
|------|--------|----------|--------------|
| `rf=3`, `minISR=2` | `kill` | 10s | 0.938 |
| `rf=3`, `minISR=2` | `partition` | 10s | 1.0 |
| `rf=3`, `minISR=2` | `pause` | 10s | 0.925 |
| `rf=3`, `minISR=2` | `leader-kill` | 10s | 0.991 |
| `rf=3`, `minISR=2` | `leave` | 10s | 0.935 |
| `rf=3`, `minISR=2` | `membership` | 10s | 1.0 |
| `rf=3`, `minISR=2` | `rejoin` | 10s | 0.918 |
| `rf=3`, `minISR=2` | `s3-partition` | 10s | 0.938 |
| `rf=3`, `minISR=2` | `clock-skew` | 10s | 1.0 |
| `rf=3`, `minISR=2` | `kill` | 45s | 0.996 |
| `rf=3`, `minISR=2` | `leader-kill` | 45s | 1.0 |
| `rf=3`, `minISR=2` | `membership` | 45s | 1.0 |
| `rf=3`, `minISR=2` | `rejoin` | 45s | 0.996 |
| `rf=3`, `minISR=2` | `s3-partition` | 45s | 0.938 |
| `rf=3`, `minISR=2` | `kill,partition` | 45s | 1.0 |
| `rf=3`, `minISR=2` | `leader-kill,s3-partition` | 45s | 0.980 |
| `rf=3`, `minISR=3` | `kill` | 45s | 1.0 |
| `rf=3`, `minISR=3` | `leader-kill` | 45s | 1.0 |
| `rf=3`, `minISR=3` | `membership` | 45s | 1.0 |
| `rf=3`, `minISR=3` | `s3-partition` | 45s | 0.948 |
| `rf=3`, `minISR=3` | `leader-kill,s3-partition` | 45s | 0.936 |
| `rf=1`, `minISR=1` | `kill` | 10s | n/a |

**9 checkers verify every run:**

| Checker | What it proves |
|---------|----------------|
| `committed-durability` | Acknowledged writes survive to final drain |
| `single-leader` | No conflicting values at the same (partition, offset) |
| `total-order` | Partition histories remain ordered and contiguous |
| `offset-monotonicity` | Offsets never duplicate or regress |
| `truncation-safety` | Committed suffixes are not lost after failover |
| `hw-monotonicity` | Observed high watermarks do not go backward |
| `no-ghost-reads` | Reads do not invent unacknowledged data |
| `availability` | Successful operation ratio during faults |
| `recovery-time` | Latency from injected fault to next success |

## Read Model

The strongest correctness checks use leader-directed reads. That is deliberate:

- Acknowledged durability is tied to the replicated high watermark
- Random-node read freshness is tied to segment flush visibility

The Jepsen matrix is primarily a **durability and failover claim**, not a blanket low-latency replica-read claim. Follower reads are available and correct (capped by high watermark), but their freshness depends on flush cadence.

## Fault Coverage

| Fault | What it stresses |
|-------|-----------------|
| `kill` | Local-tail recovery, reassignment, durable ack path |
| `leave` | Clean flush, deregistration, rebalance |
| `membership` | Topology churn and reassignment correctness |
| `partition` | Routing, stale-owner fencing, leader continuity |
| `pause` | Lease expiry and heartbeat failure detection |
| `rejoin` | Epoch fencing and stale-local-state rejection |
| `s3-partition` | Object-store isolation handling |
| `clock-skew` | Lease timing assumptions |
| `leader-kill` | Promoted-leader recovery and readable failover |
| Combined faults | Simultaneous failure modes (kill+partition, leader-kill+s3-partition) |

## Practical Limits

- No transactional semantics across topics or partitions
- Failover speed is bounded by lease TTL and fetch timing
- Random-node read freshness depends on flush cadence
- The broadest Jepsen evidence is concentrated around `rf=3`, `minISR=2`

## Bottom Line

Camu's correctness model is grounded in native-segment durability with ISR-quorum acknowledgment:

1. Writes land in local active segments first
2. Replicated commits advance through ISR high-watermark tracking
3. Writes are only acknowledged after the ISR quorum confirms
4. Sealed segments and sidecars persist shared history to object storage
5. Restarts and failovers recover from native batch data directly
6. Every claim is backed by a reproducible Jepsen artifact
