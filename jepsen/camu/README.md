# Camu Jepsen Tests

Jepsen test suite that verifies camu's consistency guarantees under fault injection.

## What This Tests

The suite produces messages to camu while injecting faults (process kills, network partitions, process pauses), then drains all partitions and verifies invariants.

## Test Types

- `kill` — SIGKILL random instances, verify data durability and epoch fencing
- `partition` — Network partitions between nodes, verify no split-brain
- `pause` — SIGSTOP/SIGCONT, verify lease TTL handling
- `combined` — All faults simultaneously, comprehensive stress test

Additional nemesis types (composable via `:faults` option):

- `:rejoin` — kills a node, waits for lease expiry, restarts it; tests epoch fencing
- `:s3-partition` — iptables-blocks MinIO port 9000 on a random node; tests S3 connectivity loss
- `:clock-skew` — introduces clock drift; tests lease expiry correctness under skew

## Checkers

- **no-data-loss** — every ack'd write is consumable after the test
- **offset-monotonicity** — no gaps or duplicates in partition offsets
- **no-split-brain** — no duplicate offsets from different writers
- **availability** — percentage of successful operations during faults (informational)
- **lease-fencing** — epoch fencing prevents conflicting writes after instance rejoin
- **recovery-time** — time from each fault injection to first successful client operation

## Prerequisites

- JVM 11+
- Leiningen
- Docker and docker-compose

## Running

```bash
./run.sh
```

This will:
1. Cross-compile camu for Linux (amd64)
2. Start a 5-node cluster plus MinIO via docker-compose
3. Run the Jepsen test suite (default: 120s time limit)
4. Print results to stdout; full artifacts in `store/latest/`

To run a specific test type:

```bash
lein run kill      -- --time-limit 120
lein run partition -- --time-limit 120
lein run pause     -- --time-limit 120
lein run combined  -- --time-limit 180
```

## Output

A passing run prints:

```
Everything looks good! :-D
```

A failing run prints which checkers failed and why, e.g. missing offsets or duplicate assignments.

Full results including timeline plots and latency histograms are written to `store/latest/`.
