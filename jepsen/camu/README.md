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

1. **Fault phase** (configurable, default 60-120s) — Mixed workload (70% produce, 30% consume) while the nemesis injects faults on random nodes. The correctness suite resolves the partition leader from routing and reads from that leader during verification.

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
./run.sh                    # kill faults, 120s
./run.sh kill 60            # kill faults, 60s
./run.sh partition 120      # network partitions, 120s
./run.sh kill,partition 180 # combined faults, 180s
RF=3 MIN_ISR=3 ./run.sh leader-kill,s3-partition 45 # strict quorum
```

`run.sh` will:

1. Cross-compile camu for `linux/amd64`
2. Start MinIO + 5 test nodes via docker-compose
3. Build and run the Jepsen control container
4. Print results to stdout; full artifacts in `store/latest/`

`run.sh` also accepts:

- `RF` to override `--replication-factor`
- `MIN_ISR` to override `--min-insync-replicas`

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
    --time-limit 120 \
    --s3-endpoint http://minio:9000 \
    --camu-binary /jepsen/camu/camu \
    --faults kill,partition
"
```

## Test Configuration

The Jepsen test uses a tuned camu configuration for faster fault detection:

| Setting | Test value | Production default | Why |
|---------|-----------|-------------------|-----|
| `segments.max_age` | 50ms | 5s | Minimize the gap between WAL durability and flushed visibility |
| `coordination.lease_ttl` | 6s | 30s | Faster coordinator and ownership failover in tests |
| `coordination.heartbeat_interval` | 2s | 10s | Faster lease renewal and expiry in tests |
| `coordination.rebalance_delay` | 2s | 5s | Faster reassignment in tests |
| `segments.max_size` | 1MB | 8MB | More frequent flushes for test volume |

This difference is intentional. Camu has separate production and test coordination defaults, and Jepsen uses the shorter timing so failover completes inside bounded test windows.

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
