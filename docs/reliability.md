# Reliability and Correctness

This document describes what Camu is currently claiming, what has been exercised in tests, and where the strongest evidence lives.

The current repository evidence is anchored to runs completed on **March 22-23, 2026**.

## Current Claim Surface

For the replicated mode that has been exercised most heavily, `rf=3` and `minISR=2`, Camu is claiming:

- acknowledged writes survive the current Jepsen fault matrix
- leader failover preserves committed prefixes
- recovered leaders do not silently truncate committed suffixes
- final partition drains match acknowledged history

For strict quorum, `rf=3` and `minISR=3`, the repository now also has passing evidence for the currently checked scenarios.

For control mode, `rf=1` and `minISR=1`, there is only a narrow durability claim around the tested kill-and-restart path.

This is a correctness and durability claim, not a blanket claim of uninterrupted availability.

## Verification Layers

### Unit Tests

Unit coverage targets the components most likely to break the contract:

- WAL replay and corruption cutoff
- segment encoding, decoding, and sparse indexes
- epoch fencing and leader initialization
- partition append serialization
- overlapping-tail index replacement
- idempotent produce bookkeeping

### Integration Tests

The integration suite exercises real server instances and shared storage:

- leadership transitions
- reassignment preserving committed prefixes
- multi-instance recovery
- replication and readable failover behavior

### Jepsen

Jepsen is the highest-signal layer because it drives the real distributed system under adversarial faults:

- five Docker nodes
- MinIO-backed object storage
- random produce and consume workload
- recovery phase
- full final drain and invariant checking

## What The Current Checkers Prove

The checked-in Jepsen harness verifies:

- `committed-durability`
  - every acknowledged write appears in the final drain
- `truncation-safety`
  - a committed suffix does not disappear after failover or recovery
- `single-leader`
  - no conflicting values appear at the same `(partition, offset)`
- `total-order`
  - partition histories stay ordered and contiguous
- `offset-monotonicity`
  - offsets never move backward or duplicate within a partition history
- `hw-monotonicity`
  - observed high watermarks do not regress
- `no-ghost-reads`
  - reads do not fabricate unacknowledged data
- `availability`
  - fraction of successful operations during the fault window
- `recovery-time`
  - time from fault injection to the next successful client operation

## Read Model Used In Jepsen

The main correctness suite uses leader-directed reads.

That is intentional. In Camu, acknowledged durability and random-node flush visibility are separate semantics:

- ack is tied to WAL durability and replicated high watermark
- non-owner reads may still lag until a segment flush is visible through `index.json`

The current Jepsen suite is therefore aimed at the primary durability contract, not at proving replica-read freshness.

## Verified Matrix

### Replicated Mode: `rf=3`, `minISR=2`

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

### Strict Quorum: `rf=3`, `minISR=3`

| Faults | Duration | Result | Availability | Artifact |
|--------|----------|--------|--------------|----------|
| `kill` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T091344.923Z/results.edn` |
| `leader-kill` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T091909.227Z/results.edn` |
| `membership` | 45s | Pass | `1.0` | `jepsen/camu/store/camu/20260323T092052.597Z/results.edn` |
| `s3-partition` | 45s | Pass | `0.948` | `jepsen/camu/store/camu/20260323T092501.527Z/results.edn` |
| `leader-kill,s3-partition` | 45s | Pass | `0.936` | `jepsen/camu/store/camu/20260323T093306.441Z/results.edn` |

### Control Mode: `rf=1`, `minISR=1`

| Faults | Duration | Result | Artifact |
|--------|----------|--------|----------|
| `kill` | 10s | Pass | `jepsen/camu/store/camu/20260322T195907.172Z/results.edn` |

## Why These Runs Matter

The matrix covers the failure modes that most directly threaten Camu's design:

- abrupt process death
- leader-targeted death
- network partitions
- process pause and lease expiry
- graceful leave and rejoin
- membership churn
- per-node object-store isolation
- clock skew
- overlapping multi-fault windows

Those are the scenarios where stale leaders, overlapping tails, replay bugs, and flush/index races tend to surface.

## Fixes Reflected In These Results

The passing matrix depends on concrete correctness work, including:

- high-watermark recovery on promoted leaders
- append serialization for same-partition concurrent writes
- overlap replacement in the segment index
- stronger stale-leader fencing during reassignment
- WAL-backed readable recovery after failover
- batcher error handling that avoids silent drop on flush failure
- harness cleanup and routing fixes so results represent the system, not the test harness

## Known Limits

The evidence is meaningful, but not exhaustive.

Current boundaries:

- most scenarios still start as short smoke runs
- only selected higher-risk paths have 45-second follow-up runs
- leader-directed reads are the primary correctness path
- the narrowest-tested mode remains `rf=3`, `minISR=2`
- the repository does not yet claim transactional semantics

The strongest next steps would be:

- more combined strict-quorum faults
- longer churn windows
- higher concurrency and larger payload runs
- a dedicated replica-read semantics matrix

## How To Audit A Run

For each artifact bundle:

1. read `results.edn`
2. inspect `history.edn`
3. correlate with per-node `camu.log`

The usual artifact set contains:

- `results.edn`
- `history.edn`
- `jepsen.log`
- `n1/camu.log` through `n5/camu.log`

That is enough to trace a claim from checker output back to operation history and node-level behavior.

## Bottom Line

As of March 23, 2026, Camu has repository-local evidence that its current replicated durability path survives the documented Jepsen matrix without losing acknowledged writes. That is a much stronger statement than "the tests pass": the failure model is explicit, the artifacts are inspectable, and the important corner cases are being exercised under adversarial conditions.
