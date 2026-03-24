# Reliability and Correctness

Camu is designed around a simple promise: if a write is acknowledged, it should survive crashes, failovers, reassignment, and recovery without silently disappearing or being rewritten as something else.

This document explains what is currently verified, how it is verified, and where the evidence lives in the repository.

## What Camu is claiming

For the replicated mode currently tested (`rf=3`, `minISR=2`):

- acknowledged writes survive the tested fault matrix
- recovered partitions do not truncate committed suffixes
- failover preserves single-writer ordering per partition
- final partition drains remain consistent with acknowledged history

For the single-replica control mode (`rf=1`, `minISR=1`):

- acknowledged writes survive node kill and restart in the tested control run

This is a durability and correctness claim, not a claim of perfect availability under all conditions.

## How correctness is verified

The project includes three layers of verification:

1. Unit tests
   - WAL replay
   - leader initialization and epoch fencing
   - index replacement and overlapping-tail handling
   - partition append serialization and flush behavior

2. Integration tests
   - real multi-instance leadership transitions
   - reassignment preserving committed prefixes
   - failover recovery with actual server instances

3. Jepsen
   - five-node fault-injection cluster
   - replicated and control configurations
   - random produce/consume workload plus final full-partition drain

Jepsen is the highest-signal evidence here because it validates the real distributed system under process death, network faults, churn, and storage isolation.

## Jepsen read model

The correctness suite uses leader-directed reads.

That choice is deliberate. Camu acknowledges writes after they are durable in the local WAL and replicated according to the configured quorum rules. Replica reads are a separate semantic question: a follower may lag in flush visibility even when the durable committed prefix already exists.

So the main Jepsen suite verifies the primary client contract:

- leader-visible committed data is preserved
- leader failover does not lose acknowledged writes
- drained partitions match acknowledged history after recovery

This avoids conflating follower visibility lag with actual data loss.

## What the Jepsen checkers prove

The checked-in Jepsen harness verifies:

- `committed-durability`
  - every acknowledged produce appears in the final drain
- `truncation-safety`
  - no committed suffix disappears after recovery
- `single-leader`
  - no conflicting leaders produce different values at the same partition/offset
- `hw-monotonicity`
  - observed high-watermarks do not move backward
- `offset-monotonicity`
  - partition offsets remain monotonic
- `total-order`
  - partition histories remain ordered and contiguous
- `no-ghost-reads`
  - reads do not fabricate data that was never acknowledged
- `recovery-time`
  - time from fault start to next successful client operation
- `availability`
  - operational success rate during the injected fault window

## Verified matrix

### Replicated mode: `rf=3`, `minISR=2`

| Faults | Duration | Result | Availability | Artifact |
|--------|----------|--------|--------------|----------|
| `kill` | 10s | Pass | `0.9375` | `jepsen/camu/store/camu/20260323T082741.545Z/results.edn` |
| `partition` | 10s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T083135.764Z/results.edn` |
| `pause` | 10s | Pass | `0.925` | `jepsen/camu/store/camu/20260323T083231.718Z/results.edn` |
| `leader-kill` | 10s | Pass | `0.991` | `jepsen/camu/store/camu/20260323T083334.317Z/results.edn` |
| `leave` | 10s | Pass | `0.935` | `jepsen/camu/store/camu/20260323T083436.194Z/results.edn` |
| `membership` | 10s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T083541.008Z/results.edn` |
| `rejoin` | 10s | Pass | `0.918` | `jepsen/camu/store/camu/20260323T083641.081Z/results.edn` |
| `s3-partition` | 10s | Pass | `0.938` | `jepsen/camu/store/camu/20260323T083755.883Z/results.edn` |
| `clock-skew` | 10s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T083930.393Z/results.edn` |
| `kill` | 45s | Pass | `0.996` | `jepsen/camu/store/camu/20260323T084623.693Z/results.edn` |
| `leader-kill` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T085050.232Z/results.edn` |
| `membership` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T085250.036Z/results.edn` |
| `rejoin` | 45s | Pass | `0.996` | `jepsen/camu/store/camu/20260323T090506.309Z/results.edn` |
| `s3-partition` | 45s | Pass | `0.938` | `jepsen/camu/store/camu/20260323T090703.652Z/results.edn` |
| `kill,partition` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T090840.074Z/results.edn` |
| `leader-kill,s3-partition` | 45s | Pass | `0.980` | `jepsen/camu/store/camu/20260323T092658.775Z/results.edn` |

### Strict quorum: `rf=3`, `minISR=3`

| Faults | Duration | Result | Availability | Artifact |
|--------|----------|--------|--------------|----------|
| `kill` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T091344.923Z/results.edn` |
| `leader-kill` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T091909.227Z/results.edn` |
| `membership` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T092052.597Z/results.edn` |
| `s3-partition` | 45s | Pass | `0.948` | `jepsen/camu/store/camu/20260323T092501.527Z/results.edn` |
| `leader-kill,s3-partition` | 45s | Pass | `0.936` | `jepsen/camu/store/camu/20260323T093306.441Z/results.edn` |

### Control mode: `rf=1`, `minISR=1`

| Faults | Duration | Result | Artifact |
|--------|----------|--------|----------|
| `kill` | 10s | Pass | `jepsen/camu/store/camu/20260322T195907.172Z/results.edn` |

## Why this evidence matters

These passes are not accidental “happy-path” runs. The current matrix covers:

- abrupt process death
- leader-targeted process death
- network partitions
- process pauses and lease expiry
- graceful leave and rejoin
- full membership churn
- S3 connectivity loss on a node
- clock skew
- overlapping kill plus network partition
- overlapping leader kill plus storage isolation
- strict quorum with `minISR=3`

That is enough to test the difficult parts of Camu’s design:

- WAL durability before flush
- fencing stale leaders after reassignment
- preserving committed data across leader transitions
- recovering readable state from WAL on promotion
- keeping indexes consistent across overlapping tails and failover

## Important fixes that made the matrix pass

The current results are backed by concrete correctness fixes, not by loosening the checker.

Examples:

- high watermark recovery on leader promotion
  - prevents leaders from truncating committed WAL prefixes after reassignment
- batcher error handling
  - prevents silent data drop on flush failure
- append serialization
  - prevents concurrent same-partition writes from producing impossible offset/WAL ordering
- overlapping segment index replacement
  - prevents stale tails from hiding newer committed suffixes after failover
- leader epoch fencing
  - prevents old leaders from continuing to accept writes after reassignment
- WAL-to-segment materialization on promoted leaders
  - makes recovered committed prefixes immediately readable after failover
- harness cleanup and routing fixes
  - ensures Jepsen results represent the system, not harness artifacts

## What is still intentionally narrow

The current evidence is strong, but it is not infinite.

Current limits:

- most workloads were first validated as `10s` smoke runs
- only the riskiest paths currently have `45s` follow-up runs
- the main correctness suite uses leader-directed reads
- the primary replicated configuration is still `rf=3`, `minISR=2`, even though strict-quorum `kill`, `leader-kill`, and `membership` runs are now verified

What would strengthen the case further:

- more strict-quorum coverage such as `rf=3`, `minISR=3` on additional combined fault types
- higher concurrency and longer runs
- more combined faults such as `leader-kill,s3-partition`
- longer rolling churn workloads
- a separate replica-read semantics suite

## How to audit the evidence yourself

The artifact paths referenced in this document are generated local Jepsen outputs from the verification runs. They are not committed repository files.

For any run:

1. open `results.edn`
2. inspect `history.edn`
3. correlate with per-node `camu.log`

The main artifact bundle for each run contains:

- `results.edn`
- `history.edn`
- `jepsen.log`
- `n1/camu.log` through `n5/camu.log`

That means every claim in this document can be independently traced back to checked-in code and stored fault artifacts.

## Bottom line

Camu now has repository-local evidence that its replicated durability path holds under the current Jepsen matrix.

That does not mean “finished forever”. It means the project is in a much stronger state than an unverified broker-shaped prototype:

- the contract is explicit
- the fault model is concrete
- the artifacts are preserved
- the fixes were validated against adversarial tests, not just unit coverage

For an object-storage-native event log, that is the kind of proof you want to see before trusting it with real data.
