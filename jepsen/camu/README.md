# Camu Jepsen Tests

Jepsen test suite that verifies camu's consistency guarantees under fault injection.

## What This Tests

The suite produces messages to camu while injecting faults (process kills, network partitions, process pauses), then drains all partitions and verifies three invariants:

- **No data loss** -- every acknowledged produce must appear in the final drain
- **Offset monotonicity** -- within each partition, offsets are strictly increasing with no gaps or duplicates
- **No split brain** -- no two messages share the same (partition, offset) with different values

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

## Output

A passing run prints:

```
Everything looks good! :-D
```

A failing run prints which checkers failed and why, e.g. missing offsets or duplicate assignments.

Full results including timeline plots and latency histograms are written to `store/latest/`.
