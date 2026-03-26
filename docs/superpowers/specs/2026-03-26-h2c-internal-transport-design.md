# h2c Internal Replication Transport

## Problem

With RF=3 and N partitions, each follower opens one TCP connection per partition per leader. This creates `N_partitions * 2` connections per node, causing connection overhead and HTTP/1.1 head-of-line blocking that limits replication throughput.

## Solution

Replace the internal replication transport with h2c (HTTP/2 over plain TCP). All partition fetches to the same leader multiplex as concurrent HTTP/2 streams over a single connection.

The public-facing HTTP API stays HTTP/1.1 on its existing port.

## Architecture

Two separate listeners:

- **Public listener** (`:8080`) — HTTP/1.1, serves `/v1/topics/...`, `/v1/groups/...`
- **Internal listener** (`:8081`, configurable) — h2c, serves `/v1/internal/...` (replication, health)

Each follower maintains **one h2c connection per leader node** shared across all partition fetchers targeting that leader.

## Components & Changes

### 1. Server startup (`server.go`)

- Add second `http.Server` wrapping internal routes with `h2c.NewHandler` from `golang.org/x/net/http2/h2c`
- Start second TCP listener on `internal_port` in `Start()`
- Shut down both servers in `Shutdown()`

### 2. Route split (`routes.go`)

- `publicRoutes()` — topic CRUD, produce, consume, offsets
- `internalRoutes()` — replication fetch (`/v1/internal/replicate/...`), health probe

### 3. Fetcher transport (`fetcher.go`)

- Replace default `http.Client` with one using `http2.Transport{AllowHTTP: true, DialTLSContext: <plain TCP dial>}` for h2c prior-knowledge connections
- One `http.Client` **per leader address** shared across all partition fetchers to that leader — this is where multiplexing happens
- Remove per-fetcher client creation; inject shared client from server

### 4. Leader address resolution

- Internal communication uses `{instanceID}:{internal_port}` (e.g., `n1:8081`)
- `initPartitionAsFollower` constructs leader address with internal port

### 5. Config (`config.go`)

- `server.internal_port` — default `8081`

### 6. Dependencies

- `golang.org/x/net/http2` — h2c handler and transport
- `golang.org/x/net/http2/h2c` — h2c server wrapper

## What doesn't change

- Wire protocol (binary WAL frames)
- Handler logic (epoch divergence, truncation, long-poll)
- Replication semantics (ISR, HW advancement, purgatory)
- Public API

## Testing

- **Unit test**: verify h2c server accepts HTTP/2 prior-knowledge connections and serves replication responses
- **Jepsen**: update `db.clj` config to include internal port; run all 8 existing test scripts as regression
- **Backward compatibility**: not needed — internal protocol is not public API, all nodes upgrade together
