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

Jepsen is the strongest evidence for distributed behavior. The repository-local
harness runs five Camu nodes against MinIO and writes every run's
`results.edn`, history, and node logs under `jepsen/camu/store/`.

The fault matrix covers kill, leader kill, leader pause-then-ack, network
partition, pause, membership changes, rejoin, object-store partition, clock
skew, and selected combined faults, against both the HTTP and the Kafka
wire-protocol API. The current commands live in
[jepsen/camu/README.md](../jepsen/camu/README.md); rely on the persisted result
for a specific run rather than a stale aggregate availability table.

Replicated HTTP and Kafka runs check:

| Checker | What it establishes |
| --- | --- |
| `committed-durability` | Acknowledged writes appear in the final drain. |
| `truncation-safety` | A committed suffix is not lost during recovery. |
| `single-leader`, `no-split-brain` | Conflicting leadership is not observed. |
| `total-order`, `offset-monotonicity` | Partition histories remain ordered. |
| `hw-monotonicity`, `no-ghost-reads` | Reads respect the committed boundary. |
| `replica-convergence` | Recovered replicas contain the final leader data. |

`read-your-writes` is a leader-read property. Replica-read workloads instead
check committed boundaries and final convergence, because a follower may
legitimately lag a newly acknowledged leader write.

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
| `partition-ring` | Allow only ring-neighbor node connectivity during the fault |
| `pause` | Lease expiry and heartbeat failure detection |
| `rejoin` | Epoch fencing and stale-local-state rejection |
| `s3-partition` | Object-store isolation handling |
| `clock-skew` | Lease timing assumptions |
| `leader-kill` | Promoted-leader recovery and readable failover |
| `leader-pause-then-ack` | Stale-leader fencing: a resumed leader must not acknowledge a write the current ISR quorum does not hold |
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
3. Writes are only acknowledged after the ISR quorum confirms — on both the HTTP API and the Kafka wire protocol (`acks=0` excepted)
4. Sealed segments and sidecars persist shared history to object storage
5. Restarts and failovers recover from native batch data directly, with promotions persisting the local leader epoch so demoted leaders report the correct epoch
6. Every claim is backed by a reproducible Jepsen artifact, including Kafka-API leader-kill and leader-pause-then-ack runs
