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
	leaseStore       *coordination.LeaseStore
	registry         *coordination.Registry
	offsetStore      *storage.OffsetStore
	instanceID       string
	listener         net.Listener

	// Leader-based coordination.
	leaderElection  *coordination.LeaderElection
	assignmentStore *coordination.AssignmentStore
	leaderLease     coordination.LeaderLease

	// leaseMu protects ownedLeases.
	leaseMu     sync.RWMutex
	ownedLeases map[string]map[int]coordination.Lease // topic -> partitionID -> Lease

	// leaseStop signals the background lease renewal goroutine to stop.
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
		leaseStore:       coordination.NewLeaseStore(s3Client),
		offsetStore:      storage.NewOffsetStore(s3Client),
		leaderElection:   coordination.NewLeaderElection(s3Client, instanceID, leaseTTL),
		assignmentStore:  coordination.NewAssignmentStore(s3Client),
		instanceID:       instanceID,
		ownedLeases:      make(map[string]map[int]coordination.Lease),
		leaseStop:        make(chan struct{}),
	}

	// Wire lease check into partition manager — verifies from S3 at flush time.
	// If ownership lost, revokes the partition so future writes are rejected locally.
	pm.SetLeaseChecker(s.verifyLeaseFromS3)

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

	// 4. Stop lease renewal goroutine and release leases.
	close(s.leaseStop)
	s.leaseWg.Wait()
	s.releaseAllLeases(ctx)

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

// getOwnedEpochs returns a map of partitionID -> epoch for owned leases of a topic.
func (s *Server) getOwnedEpochs(topic string) map[int]uint64 {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	epochs := make(map[int]uint64)
	if partitions, ok := s.ownedLeases[topic]; ok {
		for pid, lease := range partitions {
			epochs[pid] = lease.Epoch
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

	if s.amLeader() {
		// Leader: publish assignments for the new topic.
		active, err := s.registry.ActiveInstances(ctx)
		if err != nil {
			active = []string{s.instanceID}
		}
		active = ensureInList(active, s.instanceID)
		assignments := coordination.Assign(active, numPartitions)
		ta := coordination.TopicAssignments{
			Partitions: flattenAssignments(assignments),
			Version:    1,
		}
		s.assignmentStore.Write(ctx, topic, ta)
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
		if err == nil {
			ta.Version = existing.Version + 1
		} else {
			ta.Version = 1
		}

		if err := s.assignmentStore.Write(ctx, tc.Name, ta); err != nil {
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

// applyAssignmentsForTopic reads assignments for a single topic and acquires
// or releases partition leases accordingly.
func (s *Server) applyAssignmentsForTopic(ctx context.Context, topic string, numPartitions int) {
	assigned, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		slog.Debug("applyAssignmentsForTopic: no assignments found", "topic", topic, "error", err)
		return
	}

	// Determine which partitions are assigned to us.
	var myPartitions []int
	for pid, instanceID := range assigned.Partitions {
		if instanceID == s.instanceID {
			myPartitions = append(myPartitions, pid)
		}
	}

	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if _, exists := s.ownedLeases[topic]; !exists {
		s.ownedLeases[topic] = make(map[int]coordination.Lease)
	}

	// Acquire leases for assigned partitions.
	for _, pid := range myPartitions {
		if _, alreadyOwned := s.ownedLeases[topic][pid]; alreadyOwned {
			continue // already own this partition
		}
		lease, err := s.leaseStore.Acquire(ctx, topic, pid, s.instanceID, s.Address(), leaseTTL)
		if err != nil {
			slog.Debug("applyAssignmentsForTopic: skipping", "topic", topic, "partition", pid, "error", err)
			continue
		}
		s.ownedLeases[topic][pid] = lease
	}

	// Release leases for partitions no longer assigned to us.
	assignedSet := make(map[int]bool, len(myPartitions))
	for _, pid := range myPartitions {
		assignedSet[pid] = true
	}
	for pid, lease := range s.ownedLeases[topic] {
		if !assignedSet[pid] {
			if err := s.leaseStore.Release(ctx, lease); err != nil {
				slog.Debug("applyAssignmentsForTopic: release", "topic", topic, "partition", pid, "error", err)
			}
			delete(s.ownedLeases[topic], pid)
		}
	}

	if len(s.ownedLeases[topic]) == 0 {
		delete(s.ownedLeases, topic)
	}

	slog.Debug("partition assignment",
		"topic", topic,
		"instance", s.instanceID,
		"assigned", myPartitions)
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

	// All: read assignments and acquire/release leases.
	s.applyAssignmentsForTopics(ctx, topics)

	// Renew leases we still own.
	s.renewOwnedLeases(ctx)
}

// renewOwnedLeases renews all currently held leases.
func (s *Server) renewOwnedLeases(ctx context.Context) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	for topic, partitions := range s.ownedLeases {
		for pid, lease := range partitions {
			newLease, err := s.leaseStore.Acquire(ctx, lease.Topic, lease.PartitionID, s.instanceID, s.Address(), leaseTTL)
			if err != nil {
				slog.Warn("renewLeases: lost lease", "topic", topic, "partition", pid, "error", err)
				delete(partitions, pid)
				continue
			}
			partitions[pid] = newLease
		}
	}
}

// releaseAllLeases releases all owned leases.
func (s *Server) releaseAllLeases(ctx context.Context) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	for topic, partitions := range s.ownedLeases {
		for pid, lease := range partitions {
			if err := s.leaseStore.Release(ctx, lease); err != nil {
				slog.Warn("releaseAllLeases: failed", "topic", topic, "partition", pid, "error", err)
			}
		}
	}
	s.ownedLeases = make(map[string]map[int]coordination.Lease)
}

// isOwnedPartition checks if this instance owns the given partition via leases.
func (s *Server) isOwnedPartition(topic string, partitionID int) bool {
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()

	topicLeases, ok := s.ownedLeases[topic]
	if !ok {
		return false
	}
	lease, ok := topicLeases[partitionID]
	if !ok {
		return false
	}
	// Pure local check — no S3 I/O on write path.
	return time.Now().Before(lease.ExpiresAt)
}

// verifyLeaseFromS3 re-checks lease ownership from S3 (used at flush time).
// If the lease has been taken by another instance, removes it from ownedLeases
// so all future writes are rejected immediately.
func (s *Server) verifyLeaseFromS3(topic string, partitionID int) bool {
	s3Lease, err := s.leaseStore.Get(context.Background(), topic, partitionID)
	if err != nil {
		slog.Warn("verifyLeaseFromS3: failed, revoking ownership",
			"topic", topic, "partition", partitionID, "error", err)
		s.revokePartition(topic, partitionID)
		return false
	}

	if s3Lease.InstanceID != s.instanceID || !time.Now().Before(s3Lease.ExpiresAt) {
		slog.Warn("verifyLeaseFromS3: lost ownership",
			"topic", topic, "partition", partitionID,
			"owner", s3Lease.InstanceID, "self", s.instanceID)
		s.revokePartition(topic, partitionID)
		return false
	}
	return true
}

// revokePartition removes a partition from ownedLeases so all future writes
// to it are rejected via isOwnedPartition.
func (s *Server) revokePartition(topic string, partitionID int) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if topicLeases, ok := s.ownedLeases[topic]; ok {
		delete(topicLeases, partitionID)
	}
}

// getRoutingMap builds the routing response for a topic by checking leases in S3.
func (s *Server) getRoutingMap(topic string) routingResponse {
	ctx := context.Background()
	resp := routingResponse{
		Partitions: make(map[string]routingPartitionInfo),
	}

	leases, err := s.leaseStore.ListForTopic(ctx, topic)
	if err != nil {
		slog.Error("getRoutingMap: list leases", "topic", topic, "error", err)
		return resp
	}

	now := time.Now()
	for _, lease := range leases {
		if now.Before(lease.ExpiresAt) {
			key := fmt.Sprintf("%d", lease.PartitionID)
			resp.Partitions[key] = routingPartitionInfo{
				InstanceID: lease.InstanceID,
				Address:    lease.Address,
			}
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
