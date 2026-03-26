# Camu Jepsen Tests

[Jepsen](https://jepsen.io) test suite that verifies camu's consistency guarantees under fault injection on a 5-node Docker cluster.

## Current Status

As of March 23, 2026, the checked-in harness is producing clean, interpretable results:

- unique topic per run
- destructive setup removed
- leader-directed correctness reads
- bucket cleanup before every run
- daemon-PID-based kill handling
- hardened for long runs

Recently verified matrix:

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

## How It Works

The test runs in three phases:

1. **Fault phase** (configurable, default 60-120s) — A selectable workload runs while the nemesis injects faults on random nodes. `mixed` uses the original 70% small produces / 30% consumes mix, `large-requests` uses the same shape with 5 KB produce values across concurrent clients, and `replica-flushed-reads` is a produce-heavy workload meant for follower reads after graceful flushes. The correctness suite can resolve either the current leader or a known replica from routing during verification.

2. **Recovery phase** (15s) — All faults are stopped, killed nodes are restarted, cluster stabilizes.

3. **Verification phase** — All 4 partitions are drained from offset 0 (paginating in batches of 1000). Checkers compare drained data against the history of ack'd operations.

## Fault Types

Composable via `--faults` flag (comma-separated):

| Fault | Description | What it tests |
|-------|-------------|---------------|
| `kill` | SIGKILL a random camu process, restart after 3s | WAL recovery, data durability, partition reassignment |
| `leave` | Graceful SIGTERM (flushes WAL, deregisters), restart later | No data loss on graceful leave, immediate rebalance |
| `membership` | Full leave/wait/rejoin cycle on a random node | Partition reassignment on leave, correct rebalance on rejoin |
| `partition` | Network partition into random halves | No split-brain writes, correct 421 routing |
| `pause` | SIGSTOP/SIGCONT a random process | Lease TTL expiry, heartbeat failure detection |
| `rejoin` | Kill node, wait 20s for lease expiry, restart | Epoch fencing prevents stale writes |
| `s3-partition` | iptables-block MinIO port 9000 on a random node | S3 connectivity loss handling |
| `clock-skew` | Clock scrambler with 10s drift | Lease expiry correctness under time skew |
| `leader-kill` | Kill the busiest partition leader, hold it down long enough for test failover, then restart | Leader failover, reassignment, read visibility after leader change |

## Checkers

| Checker | Pass/Fail | What it verifies |
|---------|-----------|-----------------|
| **committed-durability** | Pass/Fail | Every ack'd produce appears in the final drain. |
| **single-leader** | Pass/Fail | No two different values appear at the same `(partition, offset)`. |
| **total-order** | Pass/Fail | Within each partition, offsets are delivered in order and contiguous from 0 with no gaps. |
| **offset-monotonicity** | Pass/Fail | No duplicate offsets within any partition. |
| **truncation-safety** | Pass/Fail | No committed suffix disappears after recovery. |
| **hw-monotonicity** | Pass/Fail | Observed high-watermarks do not move backward. |
| **no-ghost-reads** | Pass/Fail | Reads never fabricate messages that were not acknowledged. |
| **availability** | Informational | Fraction of successful client operations during faults (excludes nemesis events). |
| **recovery-time** | Informational | Time from each fault-start event to the next successful client operation. |

## Prerequisites

- Go (to cross-compile camu for Linux)
- Docker and docker-compose
- ~4GB RAM (JVM + 5 nodes + MinIO)

## Running

### Quick start

```bash
./run.sh                                # kill faults, 120s
./run.sh kill 60                        # kill faults, 60s
./run.sh partition 120                  # network partitions, 120s
./run.sh kill,partition 180             # combined faults, 180s
RF=3 MIN_ISR=3 ./run.sh leader-kill,s3-partition 45 # strict quorum
WORKLOAD=large-requests ./run.sh kill 120           # 5 KB produce values
CONCURRENCY=25 ./run.sh kill 120                    # higher client concurrency
CONCURRENCY=25 WORKLOAD=large-requests ./run.sh kill 120 # high concurrency + 5 KB values
READ_MODE=replica WORKLOAD=replica-flushed-reads ./run.sh leave 10 # replica reads after graceful flush
```

`run.sh` will:

1. Cross-compile camu for `linux/amd64`
2. Start MinIO + 5 test nodes via docker-compose
3. Build and run the Jepsen control container
4. Print results to stdout; full artifacts in `store/latest/`

`run.sh` also accepts:

- `RF` to override `--replication-factor`
- `MIN_ISR` to override `--min-insync-replicas`
- `WORKLOAD` to override `--workload` (`mixed`, `large-requests`, or `replica-flushed-reads`)
- `CONCURRENCY` to override Jepsen client worker count (default `5`)
- `READ_MODE` to override `--read-mode` (`leader`, `replica`, or `any`)

### Predefined Scripts

Use the wrappers in `jepsen/camu/scripts/` for common scenarios:

```bash
./scripts/smoke.sh
./scripts/large-requests.sh
./scripts/high-concurrency-large-requests.sh
./scripts/high-pressure-smoke.sh
./scripts/leader-failover-smoke.sh
./scripts/replica-flushed-reads.sh
./scripts/s3-degraded-smoke.sh
./scripts/strict-quorum-smoke.sh
```

`replica-flushed-reads.sh` runs `leave` faults with `READ_MODE=replica` and `WORKLOAD=replica-flushed-reads`, which checks that follower consume requests can return flushed data after a graceful leader shutdown.

`strict-quorum-smoke.sh` runs the default smoke scenario with `RF=3` and `MIN_ISR=3`, which is a useful reusable baseline for validating the stricter quorum path after replication or failover changes.

`leader-failover-smoke.sh` runs `leader-kill` for 10 seconds, which is a fast reassignment and promoted-leader read-path check.

`high-pressure-smoke.sh` runs `kill` with `WORKLOAD=large-requests` and `CONCURRENCY=25`, which is a quick pressure test for WAL, flush, and follower recovery behavior.

`s3-degraded-smoke.sh` runs `s3-partition` for 10 seconds, which is a useful baseline after changes to segment upload, cache refresh, or object-store error handling.

### Manual (inside the control container)

```bash
# Start infrastructure
docker-compose up -d minio setup-minio n1 n2 n3 n4 n5

# Run specific test
docker-compose run --rm control bash -c "
  # ... SSH key setup ...
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --ssh-private-key /root/.ssh/id_rsa \
    --concurrency 25 \
    --time-limit 120 \
    --s3-endpoint http://minio:9000 \
    --camu-binary /jepsen/camu/camu \
    --faults kill,partition \
    --workload large-requests
"
```

## Test Configuration

The Jepsen test uses a tuned camu configuration for faster fault detection:

| Setting | Test value | Production default | Why |
|---------|-----------|-------------------|-----|
| `segments.max_age` | 1m | 5s | Keep more unflushed data in WAL so Jepsen exercises larger in-memory/WAL batches |
| `coordination.lease_ttl` | 6s | 30s | Faster coordinator and ownership failover in tests |
| `coordination.instance_ttl` | 8s | `lease_ttl * 3` | Faster membership expiry for reassignment after kill faults |
| `coordination.heartbeat_interval` | 2s | 10s | Faster lease renewal and expiry in tests |
| `coordination.rebalance_delay` | 2s | 5s | Faster reassignment in tests |
| `segments.max_size` | 100MB | 8MB | Delay flush-by-size so large Jepsen writes accumulate before segment upload |

This difference is intentional. Camu has separate production and test defaults, and Jepsen uses more aggressive coordination timing plus larger segment thresholds to exercise failover with heavier in-flight batches inside bounded test windows.

## Output

A passing run prints:

```
Everything looks good! ヽ('ー`)ノ
```

A failing run prints which checkers failed and sample data:

```
Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```

Full results are in `store/latest/`:

- `results.edn` — Checker results summary
- `history.edn` — Full operation history
- `jepsen.log` — Detailed execution log
- `n{1-5}/camu.log` — Per-node camu logs

When a run fails, start with `results.edn`, then trace the affected keys in `history.edn` and correlate them with:

- `partition_leader_init`
- `produce_replicated`
- `flush_begin`
- `segment_flushed`
- `wal_truncated`

in the per-node `camu.log` files.

## Architecture

```
jepsen/camu/
├── src/jepsen/camu/
│   ├── camu.clj        # Test definition: generators, phases, CLI
│   ├── client.clj      # HTTP client: produce, consume, drain with retry
│   ├── checker.clj     # Custom consistency checkers
│   ├── nemesis.clj     # Fault injection: kill, partition, pause, rejoin, s3, clock
│   └── db.clj          # Node setup: binary upload, config, start/stop
├── docker/
│   ├── control/        # Clojure + Jepsen control node
│   └── node/           # Debian nodes with SSH + iptables
├── docker-compose.yml  # 5-node cluster + MinIO + control
├── project.clj         # Leiningen dependencies
└── run.sh              # One-command test runner
```
