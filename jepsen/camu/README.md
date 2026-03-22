# Camu Jepsen Tests

[Jepsen](https://jepsen.io) test suite that verifies camu's consistency guarantees under fault injection on a 5-node Docker cluster.

## How It Works

The test runs in three phases:

1. **Fault phase** (configurable, default 60-120s) — Mixed workload (70% produce, 30% consume) while the nemesis injects faults on random nodes. Clients retry across all nodes on misdirected (421) or transient (500) errors.

2. **Recovery phase** (30s) — All faults are stopped, killed nodes are restarted, cluster stabilizes.

3. **Verification phase** — All 4 partitions are drained from offset 0 (paginating in batches of 1000). Checkers compare drained data against the history of ack'd operations.

## Fault Types

Composable via `--faults` flag (comma-separated):

| Fault | Description | What it tests |
|-------|-------------|---------------|
| `kill` | SIGKILL a random camu process, restart after 3s | WAL recovery, data durability, partition reassignment |
| `partition` | Network partition into random halves | No split-brain writes, correct 421 routing |
| `pause` | SIGSTOP/SIGCONT a random process | Lease TTL expiry, heartbeat failure detection |
| `rejoin` | Kill node, wait 20s for lease expiry, restart | Epoch fencing prevents stale writes |
| `s3-partition` | iptables-block MinIO port 9000 on a random node | S3 connectivity loss handling |
| `clock-skew` | Clock scrambler with 10s drift | Lease expiry correctness under time skew |

## Checkers

| Checker | Pass/Fail | What it verifies |
|---------|-----------|-----------------|
| **no-data-loss** | Pass/Fail | Every ack'd produce (HTTP 200) appears in the final drain. Matches on (partition, offset) pairs. |
| **no-split-brain** | Pass/Fail | No two different values at the same (partition, offset). Detects concurrent writers. |
| **total-order** | Pass/Fail | Within each partition, offsets are delivered in order and contiguous from 0 with no gaps. |
| **offset-monotonicity** | Pass/Fail | No duplicate offsets within any partition. |
| **lease-fencing** | Pass/Fail | After rejoin events, epoch fencing prevented conflicting writes. |
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
```

`run.sh` will:
1. Cross-compile camu for `linux/amd64`
2. Start MinIO + 5 test nodes via docker-compose
3. Build and run the Jepsen control container
4. Print results to stdout; full artifacts in `store/latest/`

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
| `segments.max_age` | 50ms | 5s | Minimize data loss window on SIGKILL |
| `coordination.lease_ttl` | 10s | 10s | Same |
| `coordination.heartbeat_interval` | 3s | 3s | Same |
| `coordination.rebalance_delay` | 5s | 5s | Same |
| `segments.max_size` | 1MB | 8MB | More frequent flushes for test volume |

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
