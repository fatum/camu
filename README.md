# Camu

[![CI](https://github.com/fatum/camu/actions/workflows/ci.yml/badge.svg)](https://github.com/fatum/camu/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fatum/camu.svg)](https://pkg.go.dev/github.com/fatum/camu)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)

**An S3-native event log.** Ordered partitions, durable offsets, ISR replication, and an HTTP API — without running a broker cluster.

```bash
# Produce
curl -X POST http://localhost:8080/v1/topics/events/messages \
  -d '{"key": "user-123", "value": "clicked button"}'

# Consume
curl "http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0"
```

## Why Camu

- **S3-native.** Segments, indexes, offsets, and coordination all live in object storage. No broker state to lose.
- **One binary.** No ZooKeeper, no KRaft quorum, no sidecar control plane.
- **HTTP-first.** Produce, consume, and stream with `curl`. Any language, no client library.
- **Scale to zero.** Storage cost lives in S3, not in permanently hot brokers.
- **ISR replication.** `rf=3/minISR=2` gives you `acks=all` durability. Configurable per topic.
- **Jepsen-tested.** 21 fault scenarios pass. The evidence is in the repo, not a blog post.

## Quick Start

### Docker

```bash
# Start MinIO + Camu
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/camu-data

docker run -d --name camu --net=host \
  -v /tmp/camu:/var/lib/camu \
  -e CAMU_STORAGE_BUCKET=camu-data \
  -e CAMU_STORAGE_ENDPOINT=http://localhost:9000 \
  -e CAMU_STORAGE_CREDENTIALS_ACCESS_KEY=minioadmin \
  -e CAMU_STORAGE_CREDENTIALS_SECRET_KEY=minioadmin \
  ghcr.io/fatum/camu serve
```

### From Source

```bash
go install github.com/fatum/camu/cmd/camu@latest

# Or clone and build
git clone https://github.com/fatum/camu && cd camu
go build -o camu ./cmd/camu
cp camu.yaml.example camu.yaml   # edit with your S3 credentials
./camu serve --config camu.yaml
```

## Jepsen Results

Camu ships with a full [Jepsen](https://jepsen.io) test suite — 5-node Docker cluster, mixed produce/consume workload, fault injection, then full drain and invariant checking.

**Checkers:** no data loss, no split-brain offsets, contiguous total order, offset monotonicity, truncation safety, high-watermark monotonicity, no ghost reads.

| Replication | Faults | Duration | Availability |
|-------------|--------|----------|--------------|
| `rf=3` `minISR=2` | `kill` | 10s | 0.94 |
| `rf=3` `minISR=2` | `partition` | 10s | 1.0 |
| `rf=3` `minISR=2` | `pause` | 10s | 0.93 |
| `rf=3` `minISR=2` | `leader-kill` | 10s | 0.99 |
| `rf=3` `minISR=2` | `leave` | 10s | 0.94 |
| `rf=3` `minISR=2` | `membership` | 10s | 1.0 |
| `rf=3` `minISR=2` | `rejoin` | 10s | 0.92 |
| `rf=3` `minISR=2` | `s3-partition` | 10s | 0.94 |
| `rf=3` `minISR=2` | `clock-skew` | 10s | 1.0 |
| `rf=3` `minISR=2` | `kill` | 45s | 1.0 |
| `rf=3` `minISR=2` | `leader-kill` | 45s | 1.0 |
| `rf=3` `minISR=2` | `membership` | 45s | 1.0 |
| `rf=3` `minISR=2` | `rejoin` | 45s | 1.0 |
| `rf=3` `minISR=2` | `s3-partition` | 45s | 0.94 |
| `rf=3` `minISR=2` | `kill,partition` | 45s | 1.0 |
| `rf=3` `minISR=2` | `leader-kill,s3-partition` | 45s | 0.98 |
| `rf=3` `minISR=3` | `kill` | 45s | 1.0 |
| `rf=3` `minISR=3` | `leader-kill` | 45s | 1.0 |
| `rf=3` `minISR=3` | `membership` | 45s | 1.0 |
| `rf=3` `minISR=3` | `s3-partition` | 45s | 0.95 |
| `rf=3` `minISR=3` | `leader-kill,s3-partition` | 45s | 0.94 |

All 21 runs pass all checkers. Zero lost acked writes. See [`docs/reliability.md`](docs/reliability.md) for methodology, [`jepsen/camu/`](jepsen/camu/README.md) for the harness.

## API

All endpoints return JSON. Responses include `X-Camu-Instance-ID` header.

```bash
# Topics
curl -X POST http://localhost:8080/v1/topics \
  -d '{"name": "orders", "partitions": 4, "retention": "168h"}'
curl http://localhost:8080/v1/topics

# Produce (key-routed)
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -d '{"key": "user-123", "value": "order placed"}'

# Produce batch
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -d '[{"key": "u1", "value": "msg1"}, {"key": "u2", "value": "msg2"}]'

# Consume
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=100"

# Stream (SSE)
curl -N "http://localhost:8080/v1/topics/orders/partitions/0/stream?offset=0" \
  -H "Accept: text/event-stream"

# Consumer group offsets
curl -X POST http://localhost:8080/v1/groups/my-group/commit \
  -d '{"offsets": {"0": 100, "1": 200}}'

# Routing (partition owners)
curl http://localhost:8080/v1/topics/orders/routing
```

Messages with the same key always go to the same partition (FNV-32a hash). No key = round-robin. Writes to a non-owning instance return `421 Misdirected` with the routing map.

## Architecture

```
Producer → [Ownership Check] → [Partition Router] → [WAL fsync] → HTTP 200
                                                          ↓
                                              [Batcher: 8MB or 5s]
                                                          ↓
                                            [S3 Lease Re-verify]
                                                          ↓
                                      [Segment Upload] → [Index Update] → [WAL Truncate]
```

- **Write:** local lease check (zero I/O), WAL append + fsync, HTTP ack. Batcher flushes to S3 in the background.
- **Read:** index lookup, disk cache hit or S3 fetch, segment parse.
- **Coordination:** S3 conditional writes (ETag-based CAS). No Raft, no ZooKeeper.
- **Replication:** Kafka-style ISR. Followers long-poll the leader. Ack after quorum confirms.
- **Fencing:** epoch in segment filenames, local ownership check on every write, S3 lease re-verify on every flush.

Full details in [`docs/architecture.md`](docs/architecture.md).

## Multi-Instance

Multiple instances share the same S3 bucket. Partitions are distributed via round-robin, coordinated through S3 leases with epoch fencing.

- **Node join:** discovers peers, rebalancer assigns fair share, existing nodes release excess partitions
- **Node failure:** lease expires, survivors redistribute orphaned partitions
- **Stale write prevention:** epoch embedded in segment filenames + local ownership check + S3 lease verification on flush

Clients discover owners via `GET /v1/topics/{topic}/routing`.

## Configuration

See [`camu.yaml.example`](camu.yaml.example) for all options.

| Setting | Default | Description |
|---------|---------|-------------|
| `server.address` | `:8080` | HTTP listen address |
| `storage.bucket` | (required) | S3 bucket name |
| `storage.endpoint` | (AWS) | Custom endpoint for MinIO/R2/B2 |
| `wal.fsync` | `true` | Fsync on every write |
| `segments.max_size` | 8MB | Flush size threshold |
| `segments.max_age` | 5s | Flush time threshold |
| `segments.compression` | `none` | Record-batch compression: `none`, `snappy`, or `zstd` |
| `segments.record_batch_target_size` | 16KB | Target uncompressed on-disk record-batch size inside a segment |
| `segments.index_interval_bytes` | 4KB | Sparse per-segment offset index cadence |
| `cache.max_size` | 10GB | Disk cache LRU limit |
| `coordination.lease_ttl` | 30s | Lease TTL |
| `coordination.instance_ttl` | `lease_ttl * 3` | Instance liveness TTL used for active membership |
| `coordination.heartbeat_interval` | 10s | Lease renewal interval |
| `coordination.rebalance_delay` | 5s | Delay before reassignment |

## Client Examples

Plain HTTP — use whatever language you want.

<details>
<summary>Python</summary>

```python
import requests

# Produce
requests.post("http://localhost:8080/v1/topics/events/messages",
    json={"key": "user-1", "value": "signup"})

# Consume
resp = requests.get("http://localhost:8080/v1/topics/events/partitions/0/messages",
    params={"offset": 0, "limit": 50})
for msg in resp.json()["messages"]:
    print(f"offset={msg['offset']} key={msg['key']} value={msg['value']}")
```

</details>

<details>
<summary>Node.js</summary>

```javascript
// Produce
await fetch("http://localhost:8080/v1/topics/events/messages", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({key: "user-1", value: "signup"})
});

// Stream via SSE
const es = new EventSource(
  "http://localhost:8080/v1/topics/events/partitions/0/stream?offset=0"
);
es.onmessage = (e) => console.log(JSON.parse(e.data));
```

</details>

<details>
<summary>Go</summary>

```go
body := `{"key": "user-1", "value": "signup"}`
http.Post("http://localhost:8080/v1/topics/events/messages",
    "application/json", strings.NewReader(body))

resp, _ := http.Get("http://localhost:8080/v1/topics/events/partitions/0/messages?offset=0&limit=50")
```

</details>

## Testing

```bash
go test ./internal/...                                          # Unit tests
go test -tags integration ./test/integration/ -timeout 120s     # Integration tests
go test -tags integration ./test/bench/ -bench=. -benchtime=10s # Benchmarks

# Jepsen
cd jepsen/camu && ./run.sh                                      # kill faults, 120s
cd jepsen/camu && ./run.sh partition 60                          # network partitions
cd jepsen/camu && RF=3 MIN_ISR=3 ./run.sh leader-kill 45        # strict quorum
```

## Project Structure

```
camu/
├── cmd/camu/              CLI entry point
├── internal/
│   ├── config/            YAML config parsing
│   ├── consumer/          Fetcher, SSE streaming
│   ├── coordination/      S3-based leases, rebalancer
│   ├── log/               WAL, segments, index, cache, retention, GC
│   ├── meta/              Topic metadata in S3
│   ├── producer/          Partition router, batcher
│   ├── server/            HTTP handlers, partition manager
│   └── storage/           S3 client, offset storage
├── pkg/camutest/          Test helpers (real HTTP, in-memory S3)
├── test/
│   ├── integration/       Multi-instance durability + chaos tests
│   └── bench/             Throughput benchmarks
├── jepsen/camu/           Jepsen consistency tests (Clojure)
└── docs/                  Architecture + reliability docs
```

## Trade-offs

| Decision | Benefit | Cost |
|----------|---------|------|
| S3 as sole storage | Unlimited, cheap, scale to zero | ~5s visibility delay for cross-instance reads |
| Local WAL for acks | Fast writes (~30us), crash-safe | `rf=1`: unflushed data lost on node death |
| HTTP/REST API | Any language, curl-debuggable | Higher overhead than binary protocols |
| S3-based coordination | Single dependency, no consensus cluster | Failure detection floor = lease TTL |
| No in-memory buffer | WAL is single source of truth, simple recovery | No sub-ms tail reads |

## Contributing

```bash
git clone https://github.com/fatum/camu && cd camu
go test ./internal/...
go test -tags integration ./test/integration/ -timeout 120s
```

Areas we'd love help with: client libraries, Terraform/Helm templates, Grafana dashboards, S3-compatible provider testing (R2, B2, GCS), performance tuning.

Please open an issue before starting large changes.

## License

AGPL-3.0 — see [LICENSE](LICENSE).

**Commercial licensing** available for proprietary use cases. Contact [licensing@camu.dev](mailto:licensing@camu.dev).
