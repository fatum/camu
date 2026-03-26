# h2c Internal Replication Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the internal replication transport with h2c (HTTP/2 over plain TCP) to reduce connection count and eliminate head-of-line blocking between nodes.

**Architecture:** Split the single HTTP server into two listeners — public (HTTP/1.1 on `:8080`) and internal (h2c on `:8081`). Fetchers share one h2c connection per leader node across all partition fetches. No changes to wire protocol, handlers, or replication semantics.

**Tech Stack:** `golang.org/x/net/http2` and `golang.org/x/net/http2/h2c`

---

### Task 1: Add `internal_address` config field

**Files:**
- Modify: `internal/config/config.go:22-25` (ServerConfig struct)
- Modify: `internal/config/config.go:138+` (Load/defaults)

- [ ] **Step 1: Add field to ServerConfig**

In `internal/config/config.go`, add `InternalAddress` to `ServerConfig`:

```go
type ServerConfig struct {
	Address         string `yaml:"address"`
	InternalAddress string `yaml:"internal_address"`
	InstanceID      string `yaml:"instance_id"`
}
```

- [ ] **Step 2: Add default in Load**

In the `Load` function, after existing defaults, add:

```go
if cfg.Server.InternalAddress == "" {
	cfg.Server.InternalAddress = ":8081"
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/config/config.go
git commit -m "Add internal_address config field for h2c listener"
```

---

### Task 2: Split routes into public and internal

**Files:**
- Modify: `internal/server/routes.go`

- [ ] **Step 1: Rename `routes()` to `publicRoutes()` and extract `internalRoutes()`**

```go
func (s *Server) publicRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", s.handleListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", s.handleGetTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.handleDeleteTopic)
	mux.HandleFunc("GET /v1/ready", s.handleReady)
	mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("GET /v1/topics/{topic}/routing", s.handleRouting)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/messages", s.handleConsumeLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/stream", s.handleStreamLowLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/offsets/{consumer_id}", s.handleCommitConsumerOffsets)
	mux.HandleFunc("GET /v1/topics/{topic}/offsets/{consumer_id}", s.handleGetConsumerOffsets)
	mux.HandleFunc("POST /v1/groups/{group_id}/commit", s.handleCommitOffsets)
	mux.HandleFunc("GET /v1/groups/{group_id}/offsets", s.handleGetOffsets)
	return s.withMiddleware(mux)
}

func (s *Server) internalRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/internal/replicate/{topic}/{pid}", s.handleReplicaFetch)
	mux.HandleFunc("GET /v1/ready", s.handleReady)
	return s.withMiddleware(mux)
}
```

- [ ] **Step 2: Update server.go reference**

In `newServer()` at line 170, change `s.routes()` to `s.publicRoutes()`:

```go
s.httpServer = &http.Server{
	Handler: s.publicRoutes(),
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./...`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add internal/server/routes.go internal/server/server.go
git commit -m "Split routes into public and internal handlers"
```

---

### Task 3: Add h2c internal server

**Files:**
- Modify: `internal/server/server.go` (struct, newServer, startWithListener, Shutdown)
- Modify: `go.mod` / `go.sum`

- [ ] **Step 1: Add dependency**

```bash
go get golang.org/x/net/http2
go get golang.org/x/net/http2/h2c
```

- [ ] **Step 2: Add internalServer field and create it in newServer**

In server struct (line ~31), add:

```go
internalServer   *http.Server
internalListener net.Listener
```

In `newServer()`, after the existing `s.httpServer` creation (line 170), add:

```go
h2s := &http2.Server{}
s.internalServer = &http.Server{
	Handler: h2c.NewHandler(s.internalRoutes(), h2s),
}
```

Add imports:

```go
"golang.org/x/net/http2"
"golang.org/x/net/http2/h2c"
```

- [ ] **Step 3: Start internal listener in startWithListener**

In `startWithListener()`, after the public server starts (line 213), add:

```go
internalLn, err := net.Listen("tcp", s.cfg.Server.InternalAddress)
if err != nil {
	return fmt.Errorf("listen internal on %s: %w", s.cfg.Server.InternalAddress, err)
}
s.internalListener = internalLn
slog.Info("internal_server_started", "address", s.cfg.Server.InternalAddress, "protocol", "h2c")
go func() { _ = s.internalServer.Serve(internalLn) }()
```

- [ ] **Step 4: Add InternalAddress() accessor**

```go
func (s *Server) InternalAddress() string {
	if s.internalListener != nil {
		return s.internalListener.Addr().String()
	}
	return s.cfg.Server.InternalAddress
}
```

- [ ] **Step 5: Shut down internal server in Shutdown**

In `Shutdown()`, after `httpErr := s.httpServer.Shutdown(ctx)` (line 228), add:

```go
internalErr := s.internalServer.Shutdown(ctx)
```

And update the error aggregation at the end to include `internalErr`.

- [ ] **Step 6: Verify it compiles**

Run: `go build ./...`
Expected: success

- [ ] **Step 7: Commit**

```bash
git add internal/server/server.go go.mod go.sum
git commit -m "Add h2c internal server on separate listener"
```

---

### Task 4: Create shared h2c client for fetchers

**Files:**
- Modify: `internal/replication/fetcher.go`

- [ ] **Step 1: Replace default http.Client with h2c transport in NewFollowerFetcher**

Change `NewFollowerFetcher` to accept an `*http.Client` instead of creating its own:

```go
func NewFollowerFetcher(httpClient *http.Client, onLeaderDown OnLeaderDown) *FollowerFetcher {
	return &FollowerFetcher{
		httpClient:   httpClient,
		onLeaderDown: onLeaderDown,
	}
}
```

- [ ] **Step 2: Create h2c client factory**

Add a new function in `internal/replication/fetcher.go`:

```go
// NewH2CClient creates an HTTP client that speaks h2c (HTTP/2 without TLS).
// A single client should be shared across all fetchers to multiplex
// partition fetches over one connection per leader.
func NewH2CClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.DialContext(ctx, network, addr)
			},
		},
	}
}
```

Add imports: `"crypto/tls"`, `"net"`, `"golang.org/x/net/http2"`.

- [ ] **Step 3: Update server.go to create and inject the shared client**

In `newServer()`, change the `NewFollowerFetcher` call to:

```go
h2cClient := replication.NewH2CClient(replicationTimeout)
s.followerFetcher = replication.NewFollowerFetcher(h2cClient, func(topic string, pid int) {
	slog.Warn("leader down detected, attempting leadership", "topic", topic, "pid", pid)
	if err := s.attemptPartitionLeadership(topic, pid); err != nil {
		slog.Error("failed to acquire partition leadership", "topic", topic, "pid", pid, "error", err)
	}
})
```

- [ ] **Step 4: Verify it compiles**

Run: `go build ./...`
Expected: success

- [ ] **Step 5: Commit**

```bash
git add internal/replication/fetcher.go internal/server/server.go
git commit -m "Create shared h2c client for replication fetchers"
```

---

### Task 5: Route follower fetches to internal port

**Files:**
- Modify: `internal/server/server.go:695-710` (initPartitionAsFollower leader address resolution)
- Modify: `internal/server/server.go` (registry registration)

- [ ] **Step 1: Register internal address in the registry**

The registry currently stores the public address. We need to also communicate the internal port. The simplest approach: store the internal address as a separate field.

In `startWithListener()`, after registry registration, store the internal address. First check what `InstanceInfo` looks like:

Look at `internal/coordination/registry.go` for the `InstanceInfo` struct. Add an `InternalAddress` field to it, and set it during `Register()`.

- [ ] **Step 2: Use internal address for follower fetch**

In `initPartitionAsFollower()` (line ~698-710), change the leader address resolution to use the internal address:

```go
leaderInfo, err := s.registry.GetInstanceInfo(ctx, pa.Leader)
if err != nil {
	slog.Warn("initPartitionAsFollower: resolve leader", "leader", pa.Leader, "error", err)
	return
}
// Use internal address for h2c replication traffic.
_, port, _ := net.SplitHostPort(leaderInfo.InternalAddress)
if port == "" {
	port = "8081"
}
leaderAddr := net.JoinHostPort(pa.Leader, port)
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./...`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add internal/server/server.go internal/coordination/registry.go
git commit -m "Route follower fetches to internal h2c port"
```

---

### Task 6: Update Jepsen config

**Files:**
- Modify: `jepsen/camu/src/jepsen/camu/db.clj`

- [ ] **Step 1: Add internal_address to Jepsen config template**

In `write-config!`, add `internal_address` to the server section:

```clojure
config      (str "server:\n"
                 "  address: \":" http-port "\"\n"
                 "  internal_address: \":8081\"\n"
                 "  instance_id: \"" node "\"\n"
                 ...
```

- [ ] **Step 2: Commit**

```bash
git add jepsen/camu/src/jepsen/camu/db.clj
git commit -m "Add internal_address to Jepsen node config"
```

---

### Task 7: Run Jepsen smoke test

**Files:** none (verification only)

- [ ] **Step 1: Run smoke test**

```bash
bash jepsen/camu/scripts/smoke.sh
```

Expected: `Everything looks good! ヽ('ー`)ノ`

- [ ] **Step 2: Run leader-failover test**

This exercises the `attemptPartitionLeadership` path with the new internal port:

```bash
bash jepsen/camu/scripts/leader-failover-smoke.sh
```

Expected: `Everything looks good! ヽ('ー`)ノ`

- [ ] **Step 3: Commit any fixes if needed**

---

### Task 8: Run all Jepsen tests

**Files:** none (verification only)

Run all 8 test scripts one by one to confirm no regressions:

- [ ] `bash jepsen/camu/scripts/smoke.sh`
- [ ] `bash jepsen/camu/scripts/large-requests.sh`
- [ ] `bash jepsen/camu/scripts/high-concurrency-large-requests.sh`
- [ ] `bash jepsen/camu/scripts/high-pressure-smoke.sh`
- [ ] `bash jepsen/camu/scripts/leader-failover-smoke.sh`
- [ ] `bash jepsen/camu/scripts/replica-flushed-reads.sh`
- [ ] `bash jepsen/camu/scripts/s3-degraded-smoke.sh`
- [ ] `bash jepsen/camu/scripts/strict-quorum-smoke.sh`

Expected: all pass with `Everything looks good! ヽ('ー`)ノ`
