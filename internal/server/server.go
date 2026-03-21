package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// Server is the HTTP server for camu.
type Server struct {
	cfg              *config.Config
	httpServer       *http.Server
	s3Client         *storage.S3Client
	topicStore       *meta.TopicStore
	partitionManager *PartitionManager
	fetcher          *consumer.Fetcher
	registry    *coordination.Registry
	offsetStore *storage.OffsetStore
	instanceID  string
	listener    net.Listener

	// Leader-based coordination.
	leaderElection  *coordination.LeaderElection
	assignmentStore *coordination.AssignmentStore
	leaderLease     coordination.LeaderLease

	// assignmentsMu protects myPartitions.
	assignmentsMu sync.RWMutex
	myPartitions  map[string]map[int]bool // topic -> partitionID -> owned

	// leaseStop signals the background coordination goroutine to stop.
	leaseStop chan struct{}
	leaseWg   sync.WaitGroup

	// shuttingDown is set to 1 during shutdown; produce handlers check this
	// and reject new writes with 503 before batcher/WAL are torn down.
	shuttingDown atomic.Bool
}

// New creates a new Server, initializing the S3 client from config.
func New(cfg *config.Config) (*Server, error) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:    cfg.Storage.Bucket,
		Region:    cfg.Storage.Region,
		Endpoint:  cfg.Storage.Endpoint,
		AccessKey: cfg.Storage.Credentials.AccessKey,
		SecretKey: cfg.Storage.Credentials.SecretKey,
	})
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}
	return newServer(cfg, s3Client)
}

// NewWithS3Client creates a new Server using a pre-existing S3 client.
// This is used by camutest to share a single in-memory S3 backend across instances.
func NewWithS3Client(cfg *config.Config, s3Client *storage.S3Client) (*Server, error) {
	return newServer(cfg, s3Client)
}

func newServer(cfg *config.Config, s3Client *storage.S3Client) (*Server, error) {
	instanceID := cfg.Server.InstanceID
	if instanceID == "" {
		instanceID = uuid.NewString()
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		return nil, fmt.Errorf("creating partition manager: %w", err)
	}

	s := &Server{
		cfg:              cfg,
		s3Client:         s3Client,
		topicStore:       meta.NewTopicStore(s3Client),
		partitionManager: pm,
		fetcher:          consumer.NewFetcher(s3Client, pm.GetDiskCache()),
		offsetStore:     storage.NewOffsetStore(s3Client),
		leaderElection:  coordination.NewLeaderElection(s3Client, instanceID, leaseTTL),
		assignmentStore: coordination.NewAssignmentStore(s3Client),
		instanceID:      instanceID,
		myPartitions:    make(map[string]map[int]bool),
		leaseStop:       make(chan struct{}),
	}

	// Wire ownership check into partition manager — verifies from assignment store at flush time.
	// If ownership lost, revokes the partition so future writes are rejected locally.
	pm.SetLeaseChecker(s.verifyOwnershipFromS3)

	s.httpServer = &http.Server{
		Handler: s.routes(),
	}

	return s, nil
}

// Start starts the HTTP server on the configured address.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Server.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Server.Address, err)
	}
	return s.startWithListener(ln)
}

// StartOnPort starts the HTTP server on a specific port.
func (s *Server) StartOnPort(port int) error {
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	return s.startWithListener(ln)
}

// startWithListener completes server startup once a listener is available.
func (s *Server) startWithListener(ln net.Listener) error {
	s.listener = ln
	s.registry = coordination.NewRegistry(s.s3Client, s.instanceID, s.Address(), leaseTTL*3)
	s.registry.Register(context.Background())
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	s.initialCoordination()
	s.startLeaseRenewal()
	go s.httpServer.Serve(ln)
	return nil
}

// Shutdown gracefully shuts down the HTTP server and partition manager.
// Ordering:
//  1. Set shuttingDown so produce handlers reject new writes immediately.
//  2. Stop the HTTP server (drains in-flight requests).
//  3. Stop the batcher — flushes remaining WAL entries to S3.
//  4. Stop lease renewal and release all owned leases.
func (s *Server) Shutdown(ctx context.Context) error {
	// 1. Stop accepting new writes.
	s.shuttingDown.Store(true)

	// 2. Shut down HTTP server (waits for in-flight requests to finish).
	httpErr := s.httpServer.Shutdown(ctx)

	// 3. Flush batcher / close WALs.
	pmErr := s.partitionManager.Shutdown(ctx)

	// 4. Stop coordination goroutine.
	close(s.leaseStop)
	s.leaseWg.Wait()

	// 5. Deregister from cluster.
	s.registry.Deregister(ctx)

	if httpErr != nil {
		return httpErr
	}
	return pmErr
}

// Address returns the actual listening address (host:port).
func (s *Server) Address() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Server.Address
}

// InstanceID returns the server's unique instance ID.
func (s *Server) InstanceID() string {
	return s.instanceID
}

// initExistingTopics loads all topics from the topic store and initializes
// partition state for each one.
func (s *Server) initExistingTopics() error {
	ctx := context.Background()
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, tc := range topics {
		epochs := s.getOwnedEpochs(tc.Name)
		if err := s.partitionManager.InitTopic(ctx, tc, epochs); err != nil {
			slog.Error("failed to init topic", "topic", tc.Name, "error", err)
			return fmt.Errorf("init topic %q: %w", tc.Name, err)
		}
	}
	return nil
}

// getOwnedEpochs returns a map of partitionID -> epoch for owned partitions of a topic.
// Uses the assignment version as the epoch for all owned partitions.
func (s *Server) getOwnedEpochs(topic string) map[int]uint64 {
	assigned, err := s.assignmentStore.Read(context.Background(), topic)
	if err != nil {
		return nil
	}
	epochs := make(map[int]uint64)
	for pid, instanceID := range assigned.Partitions {
		if instanceID == s.instanceID {
			epochs[pid] = assigned.Version
		}
	}
	return epochs
}

const leaseTTL = 30 * time.Second
const leaseRenewalInterval = 10 * time.Second

// amLeader returns true if this instance currently holds a valid leader lease.
func (s *Server) amLeader() bool {
	return s.leaderLease.InstanceID == s.instanceID && time.Now().Before(s.leaderLease.ExpiresAt)
}

// initialCoordination runs leader election and assignment on startup.
func (s *Server) initialCoordination() {
	ctx := context.Background()

	// Try to become leader.
	lease, acquired, err := s.leaderElection.TryAcquire(ctx)
	if err != nil {
		slog.Warn("initialCoordination: leader election failed", "error", err)
	} else if acquired {
		s.leaderLease = lease
		slog.Info("initialCoordination: became leader", "instance", s.instanceID)
	}

	// Leader publishes assignments for all topics.
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Warn("initialCoordination: list topics", "error", err)
	}
	if s.amLeader() {
		s.publishAssignmentsForTopics(ctx, topics)
	}

	// All instances apply assignments (acquire leases for assigned partitions).
	s.applyAssignmentsForTopics(ctx, topics)
}

// AcquireLeasesForTopic is called from handleCreateTopic when a new topic is
// created. If this instance is the leader, it publishes assignments for the
// new topic. Then it applies assignments to acquire its own leases.
func (s *Server) AcquireLeasesForTopic(topic string, numPartitions int) {
	ctx := context.Background()

	// Always write initial assignments on topic creation — the creating
	// instance bootstraps the assignment. Leader will overwrite on next cycle.
	active, err := s.registry.ActiveInstances(ctx)
	if err != nil || len(active) == 0 {
		active = []string{s.instanceID}
	}
	active = ensureInList(active, s.instanceID)
	assignments := coordination.Assign(active, numPartitions)
	ta := coordination.TopicAssignments{
		Partitions: flattenAssignments(assignments),
		Version:    1,
	}
	if err := s.assignmentStore.Write(ctx, topic, ta, ""); err != nil {
		slog.Error("AcquireLeasesForTopic: write assignments", "topic", topic, "error", err)
	}

	s.applyAssignmentsForTopic(ctx, topic, numPartitions)
}

// publishAssignmentsForTopics computes and writes partition assignments for the given topics.
// Only called by the leader. Skips writes when assignments are unchanged.
func (s *Server) publishAssignmentsForTopics(ctx context.Context, topics []meta.TopicConfig) {
	active, err := s.registry.ActiveInstances(ctx)
	if err != nil {
		slog.Warn("publishAssignments: discover instances", "error", err)
		active = []string{s.instanceID}
	}
	active = ensureInList(active, s.instanceID)

	for _, tc := range topics {
		assignments := coordination.Assign(active, tc.Partitions)
		newPartitions := flattenAssignments(assignments)

		// Read existing to check for changes and get version.
		existing, err := s.assignmentStore.Read(ctx, tc.Name)
		if err == nil && reflect.DeepEqual(existing.Partitions, newPartitions) {
			continue // no change
		}

		ta := coordination.TopicAssignments{
			Partitions: newPartitions,
		}
		var etag string
		if err == nil {
			ta.Version = existing.Version + 1
			etag = existing.ETag
		} else {
			ta.Version = 1
		}

		if err := s.assignmentStore.Write(ctx, tc.Name, ta, etag); err != nil {
			slog.Error("publishAssignments: write", "topic", tc.Name, "error", err)
		}
	}

	slog.Info("publishAssignments: completed",
		"instance", s.instanceID,
		"topics", len(topics),
		"active_instances", len(active))
}

// applyAssignmentsForTopics reads assignments from S3 and acquires/releases leases
// for the given topics based on what is assigned to this instance.
func (s *Server) applyAssignmentsForTopics(ctx context.Context, topics []meta.TopicConfig) {
	for _, tc := range topics {
		s.applyAssignmentsForTopic(ctx, tc.Name, tc.Partitions)
	}
}

// applyAssignmentsForTopic reads assignments for a single topic and updates
// the local ownership cache.
func (s *Server) applyAssignmentsForTopic(ctx context.Context, topic string, numPartitions int) {
	assigned, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		// No assignments yet — single-instance fallback: own all partitions.
		s.assignmentsMu.Lock()
		s.myPartitions[topic] = make(map[int]bool)
		for i := 0; i < numPartitions; i++ {
			s.myPartitions[topic][i] = true
		}
		s.assignmentsMu.Unlock()
		return
	}

	owned := make(map[int]bool)
	for pid, instanceID := range assigned.Partitions {
		if instanceID == s.instanceID {
			owned[pid] = true
		}
	}

	s.assignmentsMu.Lock()
	s.myPartitions[topic] = owned
	s.assignmentsMu.Unlock()

	slog.Debug("partition assignment", "topic", topic, "instance", s.instanceID, "owned", len(owned))
}

// startLeaseRenewal starts a background goroutine that renews owned leases.
func (s *Server) startLeaseRenewal() {
	s.leaseWg.Add(1)
	go func() {
		defer s.leaseWg.Done()
		ticker := time.NewTicker(leaseRenewalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.leaseStop:
				return
			case <-ticker.C:
				s.renewLeases()
			}
		}
	}()
}

// renewLeases runs the leader-based coordination cycle:
// 1. Heartbeat registry
// 2. Try to become/stay leader
// 3. Leader: compute and publish assignments
// 4. All: read assignments and acquire/release leases
// 5. Renew owned leases
func (s *Server) renewLeases() {
	ctx := context.Background()

	// Heartbeat registry so other instances see us as active.
	if err := s.registry.Register(ctx); err != nil {
		slog.Warn("renewLeases: registry heartbeat failed", "error", err)
	}

	// Try to become/stay leader.
	if s.amLeader() {
		renewed, err := s.leaderElection.Renew(ctx, s.leaderLease)
		if err != nil {
			slog.Warn("renewLeases: lost leadership", "error", err)
			s.leaderLease = coordination.LeaderLease{} // zero out
		} else {
			s.leaderLease = renewed
		}
	} else {
		lease, acquired, err := s.leaderElection.TryAcquire(ctx)
		if err != nil {
			slog.Debug("renewLeases: leader election failed", "error", err)
		} else if acquired {
			s.leaderLease = lease
			slog.Info("renewLeases: became leader", "instance", s.instanceID)
		}
	}

	// Lift topic list once, pass to both publish and apply.
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Warn("renewLeases: list topics", "error", err)
	}

	// Leader: compute and publish assignments.
	if s.amLeader() {
		s.publishAssignmentsForTopics(ctx, topics)
	}

	// All: read assignments and update local ownership cache.
	s.applyAssignmentsForTopics(ctx, topics)
}

// isOwnedPartition checks if this instance owns the given partition.
// Pure local check — no S3 I/O on write path.
func (s *Server) isOwnedPartition(topic string, partitionID int) bool {
	s.assignmentsMu.RLock()
	defer s.assignmentsMu.RUnlock()
	if parts, ok := s.myPartitions[topic]; ok {
		return parts[partitionID]
	}
	return false
}

// verifyOwnershipFromS3 re-checks partition ownership from the assignment store
// (used at flush time). If ownership has been reassigned to another instance,
// revokes the partition so all future writes are rejected immediately.
func (s *Server) verifyOwnershipFromS3(topic string, partitionID int) bool {
	assigned, err := s.assignmentStore.Read(context.Background(), topic)
	if err != nil {
		// No assignments in S3 — trust local state (bootstrap/single-instance).
		return s.isOwnedPartition(topic, partitionID)
	}
	if assigned.Partitions[partitionID] != s.instanceID {
		slog.Warn("verifyOwnership: lost", "topic", topic, "partition", partitionID,
			"owner", assigned.Partitions[partitionID], "self", s.instanceID)
		s.revokePartition(topic, partitionID)
		return false
	}
	return true
}

// revokePartition removes a partition from myPartitions so all future writes
// to it are rejected via isOwnedPartition.
func (s *Server) revokePartition(topic string, partitionID int) {
	s.assignmentsMu.Lock()
	defer s.assignmentsMu.Unlock()
	if parts, ok := s.myPartitions[topic]; ok {
		delete(parts, partitionID)
	}
}

// getRoutingMap builds the routing response for a topic from the assignment store.
func (s *Server) getRoutingMap(topic string) routingResponse {
	ctx := context.Background()
	resp := routingResponse{
		Partitions: make(map[string]routingPartitionInfo),
	}

	assigned, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		slog.Error("getRoutingMap: read assignments", "topic", topic, "error", err)
		return resp
	}

	// Collect unique instance IDs and resolve their addresses from the registry.
	addressCache := make(map[string]string)
	for _, instanceID := range assigned.Partitions {
		if _, ok := addressCache[instanceID]; ok {
			continue
		}
		if instanceID == s.instanceID {
			addressCache[instanceID] = "http://" + s.Address()
			continue
		}
		info, err := s.registry.GetInstanceInfo(ctx, instanceID)
		if err != nil {
			slog.Debug("getRoutingMap: resolve instance", "instance", instanceID, "error", err)
			continue
		}
		addressCache[instanceID] = "http://" + info.Address
	}

	for pid, instanceID := range assigned.Partitions {
		addr, ok := addressCache[instanceID]
		if !ok {
			continue
		}
		key := fmt.Sprintf("%d", pid)
		resp.Partitions[key] = routingPartitionInfo{
			InstanceID: instanceID,
			Address:    addr,
		}
	}

	return resp
}

// flattenAssignments converts the rebalancer output (instanceID -> []partitionID)
// into a flat map (partitionID -> instanceID).
func flattenAssignments(assignments map[string][]int) map[int]string {
	result := make(map[int]string, len(assignments))
	for instanceID, partitions := range assignments {
		for _, pid := range partitions {
			result[pid] = instanceID
		}
	}
	return result
}

// ensureInList returns the list with instanceID included (appends if missing).
func ensureInList(list []string, instanceID string) []string {
	for _, id := range list {
		if id == instanceID {
			return list
		}
	}
	return append(list, instanceID)
}
