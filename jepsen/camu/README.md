# Camu Jepsen Tests

This directory contains the Jepsen harness for Camu's distributed durability and failover behavior.

It runs a five-node Docker cluster against MinIO-backed storage, injects faults, recovers the cluster, then drains all partitions and checks the observed history against acknowledged operations.

## Current Status

As of **March 23, 2026**, the checked-in harness is producing stable, interpretable runs:

- unique topic per run
- bucket cleanup before each run
- leader-directed correctness reads
- daemon-PID-based process kill handling
- reusable workload and fault wrappers under `scripts/`

The currently documented passing matrix is:

| Mode | Faults | Duration | Result | Availability |
|------|--------|----------|--------|--------------|
| `rf=3`, `minISR=2` | `kill` | 10s | Pass | `0.9375` |
| `rf=3`, `minISR=2` | `partition` | 10s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `pause` | 10s | Pass | `0.925` |
| `rf=3`, `minISR=2` | `leader-kill` | 10s | Pass | `0.991` |
| `rf=3`, `minISR=2` | `leave` | 10s | Pass | `0.935` |
| `rf=3`, `minISR=2` | `membership` | 10s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `rejoin` | 10s | Pass | `0.918` |
| `rf=3`, `minISR=2` | `s3-partition` | 10s | Pass | `0.938` |
| `rf=3`, `minISR=2` | `clock-skew` | 10s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `kill` | 45s | Pass | `0.996` |
| `rf=3`, `minISR=2` | `leader-kill` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `membership` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `rejoin` | 45s | Pass | `0.996` |
| `rf=3`, `minISR=2` | `s3-partition` | 45s | Pass | `0.938` |
| `rf=3`, `minISR=2` | `kill,partition` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=2` | `leader-kill,s3-partition` | 45s | Pass | `0.980` |
| `rf=3`, `minISR=3` | `kill` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=3` | `leader-kill` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=3` | `membership` | 45s | Pass | `1.0` |
| `rf=3`, `minISR=3` | `s3-partition` | 45s | Pass | `0.948` |
| `rf=3`, `minISR=3` | `leader-kill,s3-partition` | 45s | Pass | `0.936` |
| `rf=1`, `minISR=1` | `kill` | 10s | Pass | n/a |

See [../../docs/reliability.md](../../docs/reliability.md) for the artifact paths and interpretation.

## Test Shape

Each run has three phases:

1. fault phase
   The workload runs while the nemesis injects one or more configured faults.
2. recovery phase
   Faults stop, killed nodes restart, and the cluster is given time to stabilize.
3. verification phase
   All partitions are drained from offset `0`, then checkers compare the final state with the acknowledged history.

## Workloads

The harness currently supports:

- `mixed`
  - the baseline workload: mostly small produces with some consumes
- `large-requests`
  - the same general shape with larger values to pressure WAL and flush behavior
- `replica-flushed-reads`
  - a produce-heavy workload used for follower-read checks after graceful flushes

Read mode can be controlled independently:

- `leader`
- `replica`
- `any`

The main correctness suite uses leader reads because ack durability and random-node flush visibility are distinct semantics in Camu.

## Faults

Pass `--faults` as a comma-separated list or use `run.sh <faults> <seconds>`.

| Fault | Description | What it stresses |
|-------|-------------|------------------|
| `kill` | SIGKILL a random process, then restart it | WAL recovery, reassignment, durable ack path |
| `leave` | Graceful SIGTERM and later restart | Clean flush, deregistration, rebalance |
| `membership` | Leave, wait, then rejoin | Topology churn and reassignment correctness |
| `partition` | Split nodes into network partitions | Routing, stale-owner fencing, leader continuity |
| `pause` | SIGSTOP/SIGCONT a process | Lease expiry and heartbeat failure detection |
| `rejoin` | Kill, wait for lease expiry, then restart | Epoch fencing and stale-local-state rejection |
| `s3-partition` | Block MinIO access on one node | Object-store isolation handling |
| `clock-skew` | Inject clock drift | Lease timing assumptions |
| `leader-kill` | Kill the active leader for busy partitions | Promoted-leader recovery and readable failover |

## Checkers

| Checker | Purpose |
|---------|---------|
| `committed-durability` | acknowledged writes survive to final drain |
| `single-leader` | no conflicting values at the same `(partition, offset)` |
| `total-order` | partition histories remain ordered and contiguous |
| `offset-monotonicity` | offsets never duplicate or regress |
| `truncation-safety` | committed suffixes are not lost after failover |
| `hw-monotonicity` | observed high watermarks do not go backward |
| `no-ghost-reads` | reads do not invent unacknowledged data |
| `availability` | successful operation ratio during faults |
| `recovery-time` | latency from injected fault to next success |

## Prerequisites

- Go
- Docker and `docker-compose`
- enough local resources for one JVM control node, five test nodes, and MinIO

## Running

### Common Commands

```bash
./run.sh
./run.sh kill 60
./run.sh partition 120
./run.sh kill,partition 180
RF=3 MIN_ISR=3 ./run.sh leader-kill,s3-partition 45
WORKLOAD=large-requests ./run.sh kill 120
CONCURRENCY=25 ./run.sh kill 120
CONCURRENCY=25 WORKLOAD=large-requests ./run.sh kill 120
READ_MODE=replica WORKLOAD=replica-flushed-reads ./run.sh leave 10
```

`run.sh`:

1. cross-compiles `camu` for Linux
2. starts MinIO and five test nodes
3. builds and runs the control container
4. writes artifacts under `store/latest/`

### Wrapper Scripts

The `scripts/` directory contains reusable scenarios:

```bash
./scripts/smoke.sh
./scripts/strict-quorum-smoke.sh
./scripts/leader-failover-smoke.sh
./scripts/high-pressure-smoke.sh
./scripts/s3-degraded-smoke.sh
./scripts/large-requests.sh
./scripts/high-concurrency-large-requests.sh
./scripts/replica-flushed-reads.sh
```

Useful shortcuts:

- `strict-quorum-smoke.sh`
  - baseline `rf=3`, `minISR=3`
- `leader-failover-smoke.sh`
  - fast promoted-leader check
- `high-pressure-smoke.sh`
  - larger requests plus higher concurrency
- `s3-degraded-smoke.sh`
  - object-store isolation baseline

## Harness Configuration

The Jepsen harness intentionally uses faster coordination timing and larger flush thresholds than production defaults so it can accumulate meaningful in-flight state during short runs.

| Setting | Jepsen value | Production default | Reason |
|---------|--------------|-------------------|--------|
| `segments.max_age` | `1m` | `5s` | keep more data in WAL during faults |
| `segments.max_size` | `100MB` | `8MB` | reduce early flush-by-size |
| `coordination.lease_ttl` | `6s` | `30s` | faster failover in tests |
| `coordination.instance_ttl` | `8s` | derived from lease TTL | faster membership expiry |
| `coordination.heartbeat_interval` | `2s` | `10s` | faster renewal |
| `coordination.rebalance_delay` | `2s` | `5s` | faster reassignment |

That difference is deliberate. Jepsen is tuned to exercise failover and overlap cases within bounded time windows, not to mimic a recommended production config.

## Artifacts

Passing or failing, the run artifacts are the important output:

- `results.edn`
- `history.edn`
- `jepsen.log`
- `n1/camu.log` through `n5/camu.log`

When debugging a failure:

1. start with `results.edn`
2. inspect the affected operations in `history.edn`
3. correlate them with the per-node `camu.log` files

The most useful server events usually include:

- `partition_leader_init`
- `produce_replicated`
- `flush_begin`
- `segment_flushed`
- `wal_truncated`

## Layout

```text
jepsen/camu/
├── src/jepsen/camu/
│   ├── camu.clj
│   ├── client.clj
│   ├── checker.clj
│   ├── nemesis.clj
│   └── db.clj
├── docker/
├── docker-compose.yml
├── scripts/
├── project.clj
└── run.sh
```
