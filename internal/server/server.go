package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
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
	registry         *coordination.Registry
	offsetStore      *storage.OffsetStore
	instanceID       string
	listener         net.Listener

	// Leader-based coordination.
	leaderElection  *coordination.LeaderElection
	assignmentStore *coordination.AssignmentStore
	isrStore        *replication.ISRStore
	leaderLease     coordination.LeaderLease
	readAssignments func(ctx context.Context, topic string) (coordination.TopicAssignments, error)

	followerFetcher *replication.FollowerFetcher

	// assignmentsMu protects myPartitions.
	assignmentsMu sync.RWMutex
	myPartitions  map[string]map[int]localPartitionAssignment // topic -> partitionID -> local assignment view

	// leaseStop signals the background coordination goroutine to stop.
	leaseStop chan struct{}
	leaseWg   sync.WaitGroup

	leaseTTL             time.Duration
	leaseRenewalInterval time.Duration

	// shuttingDown is set to 1 during shutdown; produce handlers check this
	// and reject new writes with 503 before batcher/WAL are torn down.
	shuttingDown atomic.Bool

	// ready is set after initial coordination completes (S3 synced,
	// assignments applied, partitions initialized).
	ready atomic.Bool

	// coordinationGCTick counts renewal ticks; GC runs every 10th tick.
	coordinationGCTick uint64
}

type localPartitionAssignment struct {
	Owned       bool
	LeaderEpoch uint64
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

	leaseTTL, err := cfg.Coordination.LeaseTTLDuration()
	if err != nil {
		return nil, fmt.Errorf("parsing coordination.lease_ttl: %w", err)
	}
	leaseRenewalInterval, err := cfg.Coordination.HeartbeatIntervalDuration()
	if err != nil {
		return nil, fmt.Errorf("parsing coordination.heartbeat_interval: %w", err)
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("coordination.lease_ttl must be > 0")
	}
	if leaseRenewalInterval <= 0 {
		return nil, fmt.Errorf("coordination.heartbeat_interval must be > 0")
	}
	if leaseRenewalInterval >= leaseTTL {
		return nil, fmt.Errorf("coordination.heartbeat_interval (%s) must be less than coordination.lease_ttl (%s)", leaseRenewalInterval, leaseTTL)
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		return nil, fmt.Errorf("creating partition manager: %w", err)
	}

	s := &Server{
		cfg:                  cfg,
		s3Client:             s3Client,
		topicStore:           meta.NewTopicStore(s3Client),
		partitionManager:     pm,
		fetcher:              consumer.NewFetcher(s3Client, pm.GetDiskCache()),
		offsetStore:          storage.NewOffsetStore(s3Client),
		leaderElection:       coordination.NewLeaderElection(s3Client, instanceID, leaseTTL),
		assignmentStore:      coordination.NewAssignmentStore(s3Client),
		isrStore:             replication.NewISRStore(s3Client),
		instanceID:           instanceID,
		myPartitions:         make(map[string]map[int]localPartitionAssignment),
		leaseStop:            make(chan struct{}),
		leaseTTL:             leaseTTL,
		leaseRenewalInterval: leaseRenewalInterval,
	}
	s.readAssignments = s.assignmentStore.Read

	s.followerFetcher = replication.NewFollowerFetcher(func(topic string, pid int) {
		slog.Warn("leader down detected, attempting leadership", "topic", topic, "pid", pid)
		if err := s.attemptPartitionLeadership(topic, pid); err != nil {
			slog.Error("failed to acquire partition leadership", "topic", topic, "pid", pid, "error", err)
		}
	})

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
	s.registry = coordination.NewRegistry(s.s3Client, s.instanceID, s.Address(), s.leaseTTL*3)
	s.registry.Register(context.Background())
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	s.initialCoordination()
	s.ready.Store(true)
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

	// 3. Cancel all follower fetch loops.
	s.partitionManager.CancelAllFetchLoops()

	// 4. Flush batcher / close WALs.
	pmErr := s.partitionManager.Shutdown(ctx)

	// 5. Stop coordination goroutine.
	close(s.leaseStop)
	s.leaseWg.Wait()

	// 6. Deregister from cluster.
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

func routableHTTPAddress(instanceID, rawAddr string) string {
	host, port, err := net.SplitHostPort(rawAddr)
	if err != nil {
		if rawAddr == "" {
			return "http://" + net.JoinHostPort(instanceID, "8080")
		}
		return "http://" + rawAddr
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = instanceID
	}
	if port == "" {
		port = "8080"
	}
	return "http://" + net.JoinHostPort(host, port)
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
	for pid, pa := range assigned.Partitions {
		if pa.Leader == s.instanceID {
			epochs[pid] = pa.LeaderEpoch
		}
	}
	return epochs
}

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
	} else {
		slog.Info("initialCoordination: not leader", "instance", s.instanceID, "leader", lease.InstanceID)
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
func (s *Server) AcquireLeasesForTopic(tc meta.TopicConfig) {
	ctx := context.Background()

	// Always write initial assignments on topic creation — the creating
	// instance bootstraps the assignment. Leader will overwrite on next cycle.
	active, err := s.registry.ActiveInstances(ctx)
	if err != nil || len(active) == 0 {
		active = []string{s.instanceID}
	}
	active = ensureInList(active, s.instanceID)
	newPartitions := coordination.AssignReplicated(active, tc.Partitions, tc.ReplicationFactor, nil)
	ta := coordination.TopicAssignments{
		Partitions: newPartitions,
		Version:    1,
	}
	if err := s.assignmentStore.Write(ctx, tc.Name, ta, ""); err != nil {
		slog.Error("AcquireLeasesForTopic: write assignments", "topic", tc.Name, "error", err)
	}

	s.applyAssignmentsForTopic(ctx, tc.Name, tc.Partitions)
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
		// Read existing to check for changes and get version.
		existing, err := s.assignmentStore.Read(ctx, tc.Name)
		var nextVersion uint64 = 1
		var etag string
		var currentPartitions map[int]coordination.PartitionAssignment
		if err == nil {
			nextVersion = existing.Version + 1
			etag = existing.ETag
			currentPartitions = existing.Partitions
		}

		newPartitions := coordination.AssignReplicated(active, tc.Partitions, tc.ReplicationFactor, currentPartitions)

		if err == nil {
			// Check if leader assignments are unchanged (ignore LeaderEpoch for comparison).
			changed := false
			if len(existing.Partitions) != len(newPartitions) {
				changed = true
			} else {
				for pid, pa := range newPartitions {
					if ep, ok := existing.Partitions[pid]; !ok || ep.Leader != pa.Leader {
						changed = true
						break
					}
				}
			}
			if !changed {
				continue
			}
		}

		ta := coordination.TopicAssignments{
			Partitions: newPartitions,
			Version:    nextVersion,
		}

		if err := s.assignmentStore.Write(ctx, tc.Name, ta, etag); err != nil {
			// CAS conflict — another writer updated assignments. Retry once
			// with a fresh read to pick up the latest version.
			existing2, readErr := s.assignmentStore.Read(ctx, tc.Name)
			if readErr == nil {
				ta.Version = existing2.Version + 1
				ta.Partitions = coordination.AssignReplicated(active, tc.Partitions, tc.ReplicationFactor, existing2.Partitions)
				if retryErr := s.assignmentStore.Write(ctx, tc.Name, ta, existing2.ETag); retryErr != nil {
					slog.Warn("publishAssignments: retry failed", "topic", tc.Name, "error", retryErr)
				}
			} else {
				slog.Error("publishAssignments: write failed", "topic", tc.Name, "error", err)
			}
		}
	}

	slog.Info("publishAssignments: completed",
		"instance", s.instanceID,
		"topics", len(topics),
		"active_instances", len(active))
}

// applyAssignmentsForTopics reads assignments from S3 and acquires/releases leases
// for the given topics based on what is assigned to this instance.
// Also initializes any topics that exist in the topic store but haven't been
// initialized in the local partition manager (e.g. topics created on other nodes).
func (s *Server) applyAssignmentsForTopics(ctx context.Context, topics []meta.TopicConfig) {
	for _, tc := range topics {
		// Ensure topic is initialized locally before applying assignments.
		if s.partitionManager.GetRouter(tc.Name) == nil {
			epochs := s.getOwnedEpochs(tc.Name)
			if err := s.partitionManager.InitTopic(ctx, tc, epochs); err != nil {
				slog.Error("applyAssignments: failed to init topic", "topic", tc.Name, "error", err)
			}
		}
		s.applyAssignmentsForTopic(ctx, tc.Name, tc.Partitions)
	}
}

// applyAssignmentsForTopic reads assignments for a single topic and updates
// the local ownership cache.
func (s *Server) applyAssignmentsForTopic(ctx context.Context, topic string, numPartitions int) {
	assigned, err := s.readAssignments(ctx, topic)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No assignments yet — single-instance fallback: own all partitions.
			s.assignmentsMu.Lock()
			s.myPartitions[topic] = make(map[int]localPartitionAssignment)
			for i := 0; i < numPartitions; i++ {
				s.myPartitions[topic][i] = localPartitionAssignment{Owned: true}
			}
			s.assignmentsMu.Unlock()
			return
		}
		slog.Error("applyAssignments: read assignments", "topic", topic, "error", err)
		s.revokeTopic(topic)
		return
	}

	owned := make(map[int]localPartitionAssignment)
	for pid, pa := range assigned.Partitions {
		isLeader := pa.Leader == s.instanceID
		isReplica := false
		for _, r := range pa.Replicas {
			if r == s.instanceID {
				isReplica = true
				break
			}
		}

		if isLeader {
			owned[pid] = localPartitionAssignment{
				Owned:       true,
				LeaderEpoch: pa.LeaderEpoch,
			}
			s.initPartitionAsLeader(ctx, topic, pid, pa)
		} else if isReplica {
			s.initPartitionAsFollower(ctx, topic, pid, pa)
		}
	}

	s.assignmentsMu.Lock()
	s.myPartitions[topic] = owned
	s.assignmentsMu.Unlock()

	slog.Info("assignments_applied",
		"topic", topic, "instance", s.instanceID,
		"leader_partitions", len(owned),
		"total_partitions", len(assigned.Partitions),
		"version", assigned.Version)
	for pid, pa := range assigned.Partitions {
		slog.Debug("assignment_partition_state",
			"topic", topic,
			"partition", pid,
			"leader", pa.Leader,
			"leader_epoch", pa.LeaderEpoch,
			"replicas", pa.Replicas,
			"self", s.instanceID,
			"is_leader", pa.Leader == s.instanceID,
		)
	}
}

// initPartitionAsLeader sets up replication state for a partition this instance
// leads: loads/appends epoch history, recovers the high watermark, creates
// ReplicaState, and writes the initial ISR to S3.
func (s *Server) initPartitionAsLeader(ctx context.Context, topic string, pid int, pa coordination.PartitionAssignment) {
	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return // not yet initialized, will be set up later
	}
	if ps.isLeader {
		return // already initialized as leader
	}

	ps.isLeader = true
	ps.epoch = pa.LeaderEpoch

	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return
	}

	// WAL replay: recover true log end from WAL.
	// ps.nextOffset may be stale if partition was re-initialized from S3.
	walEnd := ps.nextOffset
	if msgs, err := ps.wal.Replay(); err == nil && len(msgs) > 0 {
		lastOffset := msgs[len(msgs)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}
	ps.nextOffset = walEnd

	// Load epoch history from S3 (authoritative), fall back to local file.
	ehPath := filepath.Join(s.cfg.WAL.Directory, topic, fmt.Sprintf("%d.epochs", pid))
	eh, err := s.isrStore.ReadEpochHistory(ctx, topic, pid)
	if err != nil || len(eh.Entries) == 0 {
		eh, _ = replication.LoadEpochHistory(ehPath)
		if eh == nil {
			eh = &replication.EpochHistory{}
		}
	}
	eh.Append(replication.EpochEntry{Epoch: pa.LeaderEpoch, StartOffset: walEnd})
	if err := eh.SaveToFile(ehPath); err != nil {
		slog.Warn("initPartitionAsLeader: save epoch history locally", "topic", topic, "partition", pid, "error", err)
	}
	if err := s.isrStore.WriteEpochHistory(ctx, topic, pid, eh); err != nil {
		slog.Warn("initPartitionAsLeader: save epoch history to S3", "topic", topic, "partition", pid, "error", err)
	}
	ps.epochHistory = eh

	// HW recovery:
	// rf=1: everything in the local log is committed, so HW = log end.
	// rf>1: recover from the most advanced local/persisted view, capped at log end.
	// Persisted HW metadata can lag a follower's WAL on reassignment; if we drop
	// back to the stale persisted value, the next flush truncates a safe prefix.
	recoveredHW := walEnd
	if topicCfg.ReplicationFactor > 1 {
		recoveredHW = ps.index.HighWatermark()
		isrState, err := s.isrStore.Read(ctx, topic, pid)
		if err == nil && isrState.HighWatermark > recoveredHW {
			recoveredHW = isrState.HighWatermark
		}
		if walEnd > recoveredHW {
			recoveredHW = walEnd
		}
	}
	if recoveredHW > ps.nextOffset {
		recoveredHW = ps.nextOffset
	}

	slog.Info("leader_recovery_state",
		"topic", topic,
		"partition", pid,
		"epoch", pa.LeaderEpoch,
		"wal_end", walEnd,
		"next_offset", ps.nextOffset,
		"index_hw", ps.index.HighWatermark(),
		"recovered_hw", recoveredHW,
		"replication_factor", topicCfg.ReplicationFactor,
		"min_isr", topicCfg.MinInsyncReplicas,
		"isr_store_hw", func() uint64 {
			isrState, err := s.isrStore.Read(ctx, topic, pid)
			if err != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)

	if topicCfg.ReplicationFactor > 1 {
		ps.replicaState = replication.NewReplicaState(s.instanceID, recoveredHW, topicCfg.MinInsyncReplicas)
		for _, r := range pa.Replicas {
			if r != s.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}

		// Write ISR = [self] to S3 so recovery has a consistent source of truth.
		if err := s.isrStore.Write(ctx, topic, replication.ISRState{
			Partition:     pid,
			ISR:           []string{s.instanceID},
			Leader:        s.instanceID,
			LeaderEpoch:   pa.LeaderEpoch,
			HighWatermark: recoveredHW,
		}, ""); err != nil {
			slog.Warn("initPartitionAsLeader: write ISR", "topic", topic, "partition", pid, "error", err)
		}
	}

	// If this replica was promoted with a durable tail only in local WAL,
	// persist that recovered prefix immediately so leader reads can serve it
	// through the normal index/segment path.
	if recoveredHW > ps.index.NextOffset() {
		globalID := s.partitionManager.getGlobalID(topic, pid)
		if err := s.partitionManager.onFlush(globalID); err != nil {
			slog.Warn("initPartitionAsLeader: flush recovered wal",
				"topic", topic,
				"partition", pid,
				"epoch", pa.LeaderEpoch,
				"recovered_hw", recoveredHW,
				"index_next_offset", ps.index.NextOffset(),
				"error", err,
			)
		}
	}

	slog.Info("partition_leader_init", "topic", topic, "partition", pid,
		"epoch", pa.LeaderEpoch, "hw", recoveredHW, "next_offset", ps.nextOffset, "replicas", len(pa.Replicas))
}

// initPartitionAsFollower sets up a fetch loop for a partition this instance
// replicates as a follower. Resolves the leader address from the registry and
// starts a background FollowerFetcher goroutine.
func (s *Server) initPartitionAsFollower(ctx context.Context, topic string, pid int, pa coordination.PartitionAssignment) {
	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps != nil && !ps.isLeader && ps.fetchCancel != nil {
		return // already following with an active fetch loop
	}
	// If fetchCancel is nil but we're supposed to be a follower, the previous
	// fetch loop may have exited (leader-down or error). Re-init.

	if ps == nil {
		return // not yet initialized
	}

	ps.isLeader = false

	slog.Info("follower_transition",
		"topic", topic,
		"partition", pid,
		"leader", pa.Leader,
		"leader_epoch", pa.LeaderEpoch,
		"local_next_offset", ps.nextOffset,
		"local_epoch", ps.epoch,
		"flushed_offset", ps.flushedOffset,
		"index_hw", ps.index.HighWatermark(),
	)

	// Resolve leader address. The registry stores the listener address
	// (e.g. "[::]:8080") which is useless for inter-node comms. Extract
	// the port and combine with the leader's instanceID (hostname).
	leaderInfo, err := s.registry.GetInstanceInfo(ctx, pa.Leader)
	if err != nil {
		slog.Warn("initPartitionAsFollower: resolve leader", "leader", pa.Leader, "error", err)
		return
	}
	_, port, _ := net.SplitHostPort(leaderInfo.Address)
	if port == "" {
		port = "8080"
	}
	leaderAddr := net.JoinHostPort(pa.Leader, port)

	// Cancel existing fetch loop if any (e.g., leader changed).
	if ps.fetchCancel != nil {
		ps.fetchCancel()
	}

	// Start follower fetch loop.
	fetchCtx, cancel := context.WithCancel(context.Background())
	ps.fetchCancel = cancel
	slog.Info("partition_follower_init",
		"topic", topic, "partition", pid,
		"leader", pa.Leader, "leader_addr", leaderAddr,
		"local_offset", ps.nextOffset, "epoch", ps.epoch)
	go s.followerFetcher.Run(fetchCtx, topic, pid, leaderAddr, ps.nextOffset, ps.epoch, s.instanceID, s.partitionManager)
}

// startLeaseRenewal starts a background goroutine that renews owned leases.
func (s *Server) startLeaseRenewal() {
	s.leaseWg.Add(1)
	go func() {
		defer s.leaseWg.Done()
		ticker := time.NewTicker(s.leaseRenewalInterval)
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

	// Check ISR lag for leader partitions and update S3 if changed.
	s.checkISRLag(ctx)

	// Leader: periodically GC stale coordination files in S3.
	s.coordinationGCTick++
	if s.amLeader() && s.coordinationGCTick%10 == 0 {
		s.coordinationGC(ctx, topics)
	}
}

// isOwnedPartition checks if this instance owns the given partition.
// Pure local check — no S3 I/O on write path.
func (s *Server) isOwnedPartition(topic string, partitionID int) bool {
	s.assignmentsMu.RLock()
	defer s.assignmentsMu.RUnlock()
	if parts, ok := s.myPartitions[topic]; ok {
		return parts[partitionID].Owned
	}
	return false
}

// verifyOwnershipFromS3 re-checks partition ownership from the assignment store
// (used at flush time). If ownership has been reassigned to another instance,
// revokes the partition so all future writes are rejected immediately.
func (s *Server) verifyOwnershipFromS3(topic string, partitionID int) bool {
	assigned, err := s.readAssignments(context.Background(), topic)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return s.isOwnedPartition(topic, partitionID)
		}
		slog.Warn("verifyOwnership: read failed", "topic", topic, "partition", partitionID, "error", err)
		s.revokePartition(topic, partitionID)
		return false
	}
	if assigned.Partitions[partitionID].Leader != s.instanceID {
		slog.Warn("verifyOwnership: lost", "topic", topic, "partition", partitionID,
			"owner", assigned.Partitions[partitionID].Leader, "self", s.instanceID)
		s.revokePartition(topic, partitionID)
		return false
	}
	return true
}

// verifyProduceLeadership fences stale leaders on the write path using the
// locally applied assignment epoch. This avoids an assignment-store read on
// every produce while still rejecting any producer request that raced a
// reassignment before the partition state was updated.
func (s *Server) verifyProduceLeadership(topic string, partitionID int, localEpoch uint64) bool {
	s.assignmentsMu.RLock()
	defer s.assignmentsMu.RUnlock()

	parts, ok := s.myPartitions[topic]
	if !ok {
		return false
	}
	assignment, ok := parts[partitionID]
	if !ok || !assignment.Owned {
		return false
	}
	if assignment.LeaderEpoch != localEpoch {
		slog.Warn("verifyProduceLeadership: fenced",
			"topic", topic,
			"partition", partitionID,
			"cached_epoch", assignment.LeaderEpoch,
			"self", s.instanceID,
			"local_epoch", localEpoch,
		)
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

func (s *Server) revokeTopic(topic string) {
	s.assignmentsMu.Lock()
	defer s.assignmentsMu.Unlock()
	delete(s.myPartitions, topic)
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

	// Collect unique leader instance IDs and resolve their addresses from the registry.
	addressCache := make(map[string]string)
	for _, pa := range assigned.Partitions {
		instanceID := pa.Leader
		if _, ok := addressCache[instanceID]; ok {
			continue
		}
		if instanceID == s.instanceID {
			addressCache[instanceID] = routableHTTPAddress(instanceID, s.Address())
			continue
		}
		info, err := s.registry.GetInstanceInfo(ctx, instanceID)
		if err != nil {
			slog.Debug("getRoutingMap: resolve instance", "instance", instanceID, "error", err)
			addressCache[instanceID] = routableHTTPAddress(instanceID, "")
			continue
		}
		addressCache[instanceID] = routableHTTPAddress(instanceID, info.Address)
	}

	for pid, pa := range assigned.Partitions {
		instanceID := pa.Leader
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

// attemptPartitionLeadership is called when a follower detects the leader is
// down. It tries to become the new leader via a CAS write to the assignment
// store and, on success, transitions the local partition state from follower
// to leader.
func (s *Server) attemptPartitionLeadership(topic string, pid int) error {
	ctx := context.Background()

	// 1. Read ISR from S3.
	isrState, isrErr := s.isrStore.Read(ctx, topic, pid)
	if isrErr != nil {
		slog.Warn("attemptLeadership: no ISR state", "topic", topic, "pid", pid)
	}

	// 2. Am I in ISR? (if ISR state exists)
	if isrErr == nil {
		inISR := false
		for _, id := range isrState.ISR {
			if id == s.instanceID {
				inISR = true
				break
			}
		}
		if !inISR {
			topicCfg, _ := s.topicStore.Get(ctx, topic)
			if !topicCfg.UncleanLeaderElection {
				return fmt.Errorf("not in ISR and unclean election disabled")
			}
			slog.Warn("attemptLeadership: unclean election", "topic", topic, "pid", pid)
		}
	}

	// 3. CAS on assignment store.
	assignments, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		return fmt.Errorf("read assignments: %w", err)
	}
	pa := assignments.Partitions[pid]

	// Don't attempt if we're already the leader.
	if pa.Leader == s.instanceID {
		return nil
	}

	newEpoch := pa.LeaderEpoch + 1
	slog.Info("attempt_leadership_begin",
		"topic", topic,
		"partition", pid,
		"current_leader", pa.Leader,
		"current_epoch", pa.LeaderEpoch,
		"candidate", s.instanceID,
		"isr", func() []string {
			if isrErr != nil {
				return nil
			}
			return isrState.ISR
		}(),
		"isr_hw", func() uint64 {
			if isrErr != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)
	pa.Leader = s.instanceID
	pa.LeaderEpoch = newEpoch
	assignments.Partitions[pid] = pa
	assignments.Version++

	if err := s.assignmentStore.Write(ctx, topic, assignments, assignments.ETag); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("lost leadership race (CAS conflict)")
		}
		return fmt.Errorf("write assignments: %w", err)
	}

	// 4. Won! Transition from follower to leader.
	slog.Info("won partition leadership", "topic", topic, "pid", pid, "epoch", newEpoch)

	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition state not found")
	}

	// 4a. Cancel fetch loop.
	if ps.fetchCancel != nil {
		ps.fetchCancel()
		ps.fetchCancel = nil
	}

	// 4b. Replay WAL to recover true log end.
	// ps.nextOffset may be stale if a coordination cycle re-initialized the
	// partition from S3 (which doesn't have follower-specific un-flushed data).
	// The WAL is the source of truth for un-flushed messages.
	walEnd := ps.nextOffset
	if msgs, err := ps.wal.Replay(); err == nil && len(msgs) > 0 {
		lastOffset := msgs[len(msgs)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	slog.Info("failover_recovery_state",
		"topic", topic,
		"partition", pid,
		"new_epoch", newEpoch,
		"previous_epoch", ps.epoch,
		"previous_next_offset", ps.nextOffset,
		"wal_end", walEnd,
		"index_hw", ps.index.HighWatermark(),
		"flushed_offset", ps.flushedOffset,
		"isr_hw", func() uint64 {
			if isrErr != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)

	// 4c. Set HW to log end.
	// The new leader was an ISR member — everything it has is safe to serve.
	// All acked produces were acked because this follower (now leader) had
	// the data (that's what caused HW to advance on the old leader).
	// Setting HW = log end makes all this data immediately visible to
	// consumers and allows the purgatory on new produces to work correctly.
	recoveredHW := walEnd
	slog.Info("failover: set HW to log end",
		"topic", topic, "pid", pid,
		"hw", recoveredHW, "log_end", walEnd)

	// 4d. Epoch history — load from S3 (authoritative), fall back to local.
	ehPath := filepath.Join(s.cfg.WAL.Directory, topic, fmt.Sprintf("%d.epochs", pid))
	if ps.epochHistory == nil {
		ps.epochHistory, _ = s.isrStore.ReadEpochHistory(ctx, topic, pid)
		if ps.epochHistory == nil || len(ps.epochHistory.Entries) == 0 {
			ps.epochHistory, _ = replication.LoadEpochHistory(ehPath)
			if ps.epochHistory == nil {
				ps.epochHistory = &replication.EpochHistory{}
			}
		}
	}
	ps.epochHistory.Append(replication.EpochEntry{Epoch: newEpoch, StartOffset: walEnd})
	ps.epochHistory.SaveToFile(ehPath)
	if err := s.isrStore.WriteEpochHistory(ctx, topic, pid, ps.epochHistory); err != nil {
		slog.Warn("attemptPartitionLeadership: save epoch history to S3", "topic", topic, "pid", pid, "error", err)
	}

	// 4d. Initialize as leader.
	ps.isLeader = true
	ps.epoch = newEpoch
	topicCfg, _ := s.topicStore.Get(ctx, topic)
	if topicCfg.ReplicationFactor > 1 {
		ps.replicaState = replication.NewReplicaState(s.instanceID, recoveredHW, topicCfg.MinInsyncReplicas)
		for _, r := range pa.Replicas {
			if r != s.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}
	}

	// 4e. Write ISR = [self] to S3.
	s.isrStore.Write(ctx, topic, replication.ISRState{
		Partition:     pid,
		ISR:           []string{s.instanceID},
		Leader:        s.instanceID,
		LeaderEpoch:   newEpoch,
		HighWatermark: recoveredHW,
	}, "")

	// 4f. Update ownership cache.
	s.assignmentsMu.Lock()
	if s.myPartitions[topic] == nil {
		s.myPartitions[topic] = make(map[int]localPartitionAssignment)
	}
	s.myPartitions[topic][pid] = localPartitionAssignment{
		Owned:       true,
		LeaderEpoch: newEpoch,
	}
	s.assignmentsMu.Unlock()

	return nil
}

// checkISRLag iterates over all leader partitions and removes followers from
// the ISR set if they have not contacted the leader within the lag timeout.
// When the ISR changes, the updated set is written to S3.
func (s *Server) checkISRLag(ctx context.Context) {
	s.partitionManager.mu.RLock()
	defer s.partitionManager.mu.RUnlock()
	for topic, parts := range s.partitionManager.partitions {
		for pid, ps := range parts {
			if ps.isLeader && ps.replicaState != nil {
				changed := ps.replicaState.CheckISRLag(30 * time.Second)
				if changed {
					isr := ps.replicaState.GetISRMembers()
					s.isrStore.Write(ctx, topic, replication.ISRState{
						Partition:     pid,
						ISR:           isr,
						Leader:        s.instanceID,
						LeaderEpoch:   ps.epoch,
						HighWatermark: ps.replicaState.HighWatermark(),
					}, "")
				}
			}
		}
	}
}

// coordinationGC removes stale coordination files from S3.
// Only called by the leader on a slow cadence (every 10th renewal tick).
func (s *Server) coordinationGC(ctx context.Context, topics []meta.TopicConfig) {
	s.gcStaleInstances(ctx)
	s.gcStaleISR(ctx, topics)
}

// gcStaleInstances deletes instance registration files whose heartbeat
// has expired beyond the registry TTL.
func (s *Server) gcStaleInstances(ctx context.Context) {
	keys, err := s.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		slog.Warn("coordinationGC: list instances", "error", err)
		return
	}
	now := time.Now()
	for _, key := range keys {
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			continue
		}
		var info coordination.InstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		// Use the same TTL the registry uses to filter active instances (leaseTTL * 3).
		if now.Sub(info.HeartbeatAt) > s.leaseTTL*3 {
			if err := s.s3Client.Delete(ctx, key); err != nil {
				slog.Warn("coordinationGC: delete stale instance", "key", key, "error", err)
			} else {
				slog.Info("coordinationGC: removed stale instance", "instance", info.InstanceID)
			}
		}
	}
}

// gcStaleISR deletes ISR state files for topics or partitions that no longer exist.
func (s *Server) gcStaleISR(ctx context.Context, topics []meta.TopicConfig) {
	topicSet := make(map[string]int) // topic name -> partition count
	for _, t := range topics {
		topicSet[t.Name] = t.Partitions
	}

	keys, err := s.s3Client.List(ctx, "_coordination/isr/")
	if err != nil {
		slog.Warn("coordinationGC: list ISR", "error", err)
		return
	}
	for _, key := range keys {
		// Keys look like: _coordination/isr/{topic}/{pid}.json
		rest := key[len("_coordination/isr/"):]
		slashIdx := strings.Index(rest, "/")
		if slashIdx < 0 {
			continue
		}
		topic := rest[:slashIdx]
		var pid int
		if n, _ := fmt.Sscanf(rest[slashIdx+1:], "%d.json", &pid); n != 1 {
			continue
		}
		partCount, topicExists := topicSet[topic]
		if !topicExists || pid >= partCount {
			if err := s.s3Client.Delete(ctx, key); err != nil {
				slog.Warn("coordinationGC: delete stale ISR", "key", key, "error", err)
			} else {
				slog.Info("coordinationGC: removed stale ISR", "topic", topic, "partition", pid)
			}
		}
	}
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
