package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	fetcher     *consumer.Fetcher
	leaseStore  *coordination.LeaseStore
	offsetStore *storage.OffsetStore
	instanceID       string
	listener         net.Listener

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
		offsetStore: storage.NewOffsetStore(s3Client),
		instanceID:       instanceID,
		ownedLeases:      make(map[string]map[int]coordination.Lease),
		leaseStop:        make(chan struct{}),
	}

	s.httpServer = &http.Server{
		Handler: s.routes(),
	}

	return s, nil
}

// Start starts the HTTP server on the configured address.
func (s *Server) Start() error {
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	s.acquireAllLeases()
	s.startLeaseRenewal()
	ln, err := net.Listen("tcp", s.cfg.Server.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Server.Address, err)
	}
	s.listener = ln
	go s.httpServer.Serve(ln)
	return nil
}

// StartOnPort starts the HTTP server on a specific port.
func (s *Server) StartOnPort(port int) error {
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	s.acquireAllLeases()
	s.startLeaseRenewal()
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.listener = ln
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

// acquireAllLeases acquires leases for this instance's assigned partitions
// across all topics, using the rebalancer to distribute evenly.
func (s *Server) acquireAllLeases() {
	ctx := context.Background()
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Error("acquireAllLeases: list topics", "error", err)
		return
	}

	for _, tc := range topics {
		s.acquireLeasesForTopic(ctx, tc.Name, tc.Partitions)
	}
}

// AcquireLeasesForTopic acquires leases for this instance's assigned partitions
// of a specific topic, using the rebalancer for even distribution.
func (s *Server) AcquireLeasesForTopic(topic string, numPartitions int) {
	s.acquireLeasesForTopic(context.Background(), topic, numPartitions)
}

func (s *Server) acquireLeasesForTopic(ctx context.Context, topic string, numPartitions int) {
	// Discover active instances (from existing leases) + always include self.
	registry := coordination.NewRegistry(s.s3Client, s.instanceID, s.Address())
	active, err := registry.ActiveInstances(ctx, topic)
	if err != nil {
		slog.Warn("acquireLeasesForTopic: discover instances", "error", err)
		active = nil
	}

	// Ensure this instance is in the active set.
	found := false
	for _, id := range active {
		if id == s.instanceID {
			found = true
			break
		}
	}
	if !found {
		active = append(active, s.instanceID)
	}

	// Use rebalancer to determine which partitions this instance should own.
	assignments := coordination.Assign(active, numPartitions)
	myPartitions := assignments[s.instanceID]

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
			slog.Debug("acquireLeasesForTopic: skipping", "topic", topic, "partition", pid, "error", err)
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
				slog.Debug("acquireLeasesForTopic: release", "topic", topic, "partition", pid, "error", err)
			}
			delete(s.ownedLeases[topic], pid)
		}
	}

	slog.Info("partition assignment",
		"topic", topic,
		"instance", s.instanceID,
		"assigned", myPartitions,
		"active_instances", len(active))
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

// renewLeases re-runs the rebalancer on each cycle to pick up new instances.
// Renews leases for assigned partitions, releases leases for unassigned ones.
func (s *Server) renewLeases() {
	ctx := context.Background()
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Warn("renewLeases: list topics", "error", err)
		// Fall back to just renewing what we have.
		s.renewOwnedLeases(ctx)
		return
	}

	for _, tc := range topics {
		s.acquireLeasesForTopic(ctx, tc.Name, tc.Partitions)
	}

	// Renew leases we still own after rebalance.
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
	return time.Now().Before(lease.ExpiresAt)
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
