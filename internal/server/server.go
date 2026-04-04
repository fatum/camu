package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

const headerForwardedBy = "X-Forwarded-By"

// Server is the HTTP server for camu.
type Server struct {
	cfg              *config.Config
	httpServer       *http.Server
	internalServer   *http.Server
	internalListener net.Listener
	s3Client         *storage.S3Client
	topicStore       *meta.TopicStore
	partitionManager *PartitionManager
	fetcher          *consumer.Fetcher
	registry         *coordination.Registry
	offsetStore      *storage.OffsetStore
	aclStore         *storage.ACLStore
	instanceID       string
	listener         net.Listener

	// Leader-based coordination.
	leaderElection  *coordination.LeaderElection
	assignmentStore *coordination.AssignmentStore
	isrStore        *replication.ISRStore
	leaderLease     coordination.LeaderLease
	readAssignments func(ctx context.Context, topic string) (coordination.TopicAssignments, error)

	controllerState  atomic.Pointer[coordination.ControllerState]
	controllerCtx    context.Context
	controllerCancel context.CancelFunc
	assignmentPusher *AssignmentPusher

	idempotencyManager *idempotency.Manager
	followerFetcher    *replication.FollowerFetcher
	internalClient     *http.Client

	kafkaServer *KafkaServer
	groupCoord  *kafkaGroupCoordinator

	// assignmentsMu protects myPartitions.
	assignmentsMu sync.RWMutex
	myPartitions  map[string]map[int]localPartitionAssignment // topic -> partitionID -> local assignment view

	// leaseStop signals the background coordination goroutine to stop.
	leaseStop chan struct{}
	leaseWg   sync.WaitGroup

	leaseTTL             time.Duration
	leaseRenewalInterval time.Duration
	replicationTimeout   time.Duration

	// shuttingDown is set to 1 during shutdown; produce handlers check this
	// and reject new writes with 503 before batcher/local state are torn down.
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
	instanceTTL, err := cfg.Coordination.InstanceTTLDuration()
	if err != nil {
		return nil, fmt.Errorf("parsing coordination.instance_ttl: %w", err)
	}
	replicationTimeout, err := cfg.Coordination.ReplicationTimeoutDuration()
	if err != nil {
		return nil, fmt.Errorf("parsing coordination.replication_timeout: %w", err)
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("coordination.lease_ttl must be > 0")
	}
	if leaseRenewalInterval <= 0 {
		return nil, fmt.Errorf("coordination.heartbeat_interval must be > 0")
	}
	if instanceTTL <= 0 {
		return nil, fmt.Errorf("coordination.instance_ttl must be > 0")
	}
	if leaseRenewalInterval >= leaseTTL {
		return nil, fmt.Errorf("coordination.heartbeat_interval (%s) must be less than coordination.lease_ttl (%s)", leaseRenewalInterval, leaseTTL)
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		return nil, fmt.Errorf("creating partition manager: %w", err)
	}

	idempotencyMgr := idempotency.NewManager(s3Client)

	s := &Server{
		cfg:                  cfg,
		s3Client:             s3Client,
		topicStore:           meta.NewTopicStore(s3Client),
		partitionManager:     pm,
		fetcher:              consumer.NewFetcher(s3Client, pm.GetDiskCache()),
		offsetStore:          storage.NewOffsetStore(s3Client),
		aclStore:             storage.NewACLStore(s3Client),
		groupCoord:           newKafkaGroupCoordinator(s3Client, instanceID),
		leaderElection:       coordination.NewLeaderElection(s3Client, instanceID, leaseTTL),
		assignmentStore:      coordination.NewAssignmentStore(s3Client),
		isrStore:             replication.NewISRStore(s3Client),
		idempotencyManager:   idempotencyMgr,
		instanceID:           instanceID,
		myPartitions:         make(map[string]map[int]localPartitionAssignment),
		leaseStop:            make(chan struct{}),
		leaseTTL:             leaseTTL,
		leaseRenewalInterval: leaseRenewalInterval,
		replicationTimeout:   replicationTimeout,
	}
	s.readAssignments = s.assignmentStore.Read
	s.groupCoord.controllerEpoch = s.currentControllerEpoch

	s.internalClient = replication.NewH2CClient(replicationTimeout)
	s.assignmentPusher = NewAssignmentPusher(s.internalClient)
	s.followerFetcher = replication.NewFollowerFetcher(s.internalClient, func(topic string, pid int) {
		slog.Warn("leader down detected, reporting to controller",
			"topic", topic, "pid", pid)
		if err := s.reportFailureToController(topic, pid); err != nil {
			slog.Error("report failure to controller failed, falling back to self-election",
				"topic", topic, "pid", pid, "err", err)
			// Fallback: try the old path if controller is unreachable
			if err := s.attemptPartitionLeadership(topic, pid); err != nil {
				slog.Error("fallback self-election also failed",
					"topic", topic, "pid", pid, "err", err)
			}
		}
	})

	// Wire ownership check into partition manager — verifies from assignment store at flush time.
	// If ownership lost, revokes the partition so future writes are rejected locally.
	pm.SetLeaseChecker(s.verifyOwnershipFromS3)

	s.httpServer = &http.Server{
		Handler: s.publicRoutes(),
	}

	h2s := &http2.Server{}
	s.internalServer = &http.Server{
		Handler: h2c.NewHandler(s.internalRoutes(), h2s),
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
	internalLn, err := net.Listen("tcp", s.cfg.Server.InternalAddress)
	if err != nil {
		return fmt.Errorf("listen internal on %s: %w", s.cfg.Server.InternalAddress, err)
	}
	s.internalListener = internalLn
	instanceTTL, err := s.cfg.Coordination.InstanceTTLDuration()
	if err != nil {
		return fmt.Errorf("parsing coordination.instance_ttl: %w", err)
	}
	kafkaAddr := ""
	if s.cfg.Server.KafkaPort > 0 {
		kafkaAddr = kafkaAdvertiseAddr(s.instanceID, s.Address(), s.cfg.Server.KafkaPort)
	}
	s.registry = coordination.NewRegistry(s.s3Client, s.instanceID, s.Address(), s.InternalAddress(), kafkaAddr, instanceTTL)
	if err := s.registry.Register(context.Background()); err != nil {
		return fmt.Errorf("register registry: %w", err)
	}
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	s.initialCoordination()
	s.ready.Store(true)
	s.startLeaseRenewal()
	go func() { _ = s.httpServer.Serve(ln) }()
	slog.Info("internal_server_started", "address", s.InternalAddress(), "protocol", "h2c")
	go func() { _ = s.internalServer.Serve(internalLn) }()

	// Start Kafka protocol server if configured
	if s.cfg.Server.KafkaPort > 0 {
		s.startKafkaServer(s.cfg.Server.KafkaPort)
	}

	return nil
}

// startKafkaServer starts the Kafka protocol server on the given port.
func (s *Server) startKafkaServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	brokerAddr := kafkaAdvertiseAddr(s.instanceID, s.Address(), port)
	brokerID := kafkaBrokerID(s.instanceID)

	// Create partition getter that wraps partition manager
	pg := &kafkaPartitionGetter{pm: s.partitionManager, brokerID: brokerID}

	// Create topic lister that wraps topic store
	tl := &kafkaTopicLister{ts: s.topicStore}

	s.kafkaServer = NewKafkaServer(&KafkaServerCfg{
		PartitionGetter:             pg,
		TopicLister:                 tl,
		MetadataFunc:                s.handleKafkaMetadata,
		CreateTopicsFunc:            s.handleKafkaCreateTopics,
		DeleteTopicsFunc:            s.handleKafkaDeleteTopics,
		CreatePartitionsFunc:        s.handleKafkaCreatePartitions,
		DescribeConfigsFunc:         s.handleKafkaDescribeConfigs,
		AlterConfigsFunc:            s.handleKafkaAlterConfigs,
		IncrementalAlterConfigsFunc: s.handleKafkaIncrementalAlterConfigs,
		DescribeClusterFunc:         s.handleKafkaDescribeCluster,
		CreateACLsFunc:              s.handleKafkaCreateACLs,
		DescribeACLsFunc:            s.handleKafkaDescribeACLs,
		DeleteACLsFunc:              s.handleKafkaDeleteACLs,
		FindCoordinatorFunc:         s.handleKafkaFindCoordinator,
		InitProducerIDFunc:          s.handleKafkaInitProducerID,
		DescribeGroupsFunc:          s.handleKafkaDescribeGroups,
		ListGroupsFunc:              s.handleKafkaListGroups,
		DeleteGroupsFunc:            s.handleKafkaDeleteGroups,
		OffsetDeleteFunc:            s.handleKafkaOffsetDelete,
		JoinGroupFunc:               s.handleKafkaJoinGroup,
		SyncGroupFunc:               s.handleKafkaSyncGroup,
		HeartbeatFunc:               s.handleKafkaHeartbeat,
		LeaveGroupFunc:              s.handleKafkaLeaveGroup,
		ListOffsetsFunc:             s.handleKafkaListOffsets,
		OffsetCommitFunc:            s.handleKafkaOffsetCommit,
		OffsetFetchFunc:             s.handleKafkaOffsetFetch,
		PartitionErrorFunc:          s.kafkaPartitionError,
		AppendRawBatchFunc:          s.handleKafkaAppendRawBatch,
		AppendBatchFunc:             s.handleKafkaAppendBatch,
		AppendFunc:                  s.handleKafkaAppend,
		FetchRawBatchesFunc:         s.handleKafkaFetchRawBatches,
		FetchFunc:                   s.handleKafkaFetch,
		BrokerID:                    brokerID,
		BrokerAddr:                  brokerAddr,
	})

	go func() {
		if err := s.kafkaServer.StartListener(addr); err != nil {
			slog.Error("kafka_server_start", "address", addr, "error", err)
		}
	}()

	slog.Info("kafka_server_started", "address", addr, "protocol", "Kafka")
}

// handleKafkaAppend is the Kafka produce path entry point.
// It calls the existing AppendBatch which writes to native local storage and returns immediately.
// S3 flush happens asynchronously via the batcher (already implemented).
func (s *Server) handleKafkaAppend(topic string, partition int, msgs []log.Message) ([]uint64, error) {
	if s.shuttingDown.Load() {
		return nil, fmt.Errorf("server is shutting down")
	}

	pm := s.partitionManager
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	pm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}

	if _, ok := tp[partition]; !ok {
		return nil, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	// Check if we own this partition
	if !s.isOwnedPartition(topic, partition) {
		return nil, fmt.Errorf("%w: partition %d", errKafkaNotLeader, partition)
	}

	// Use the existing AppendBatch - this writes to native local storage and returns immediately
	// S3 flush happens asynchronously via batcher (already implemented)
	return pm.AppendBatch(context.Background(), topic, partition, msgs)
}

func (s *Server) handleKafkaAppendBatch(topic string, partition int, batch log.Batch) ([]uint64, error) {
	if s.shuttingDown.Load() {
		return nil, fmt.Errorf("server is shutting down")
	}

	pm := s.partitionManager
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	pm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}

	if _, ok := tp[partition]; !ok {
		return nil, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	if !s.isOwnedPartition(topic, partition) {
		return nil, fmt.Errorf("%w: partition %d", errKafkaNotLeader, partition)
	}

	if batch.ProducerID == 0 {
		return pm.AppendBatch(context.Background(), topic, partition, batch.Messages)
	}
	return pm.AppendBatchWithMeta(context.Background(), topic, partition, batch, &IdempotencyOpts{
		Sequence: batch.Sequence,
	})
}

func (s *Server) handleKafkaAppendRawBatch(ctx context.Context, topic string, partition int, batch []byte) (int64, error) {
	if s.shuttingDown.Load() {
		return 0, fmt.Errorf("server is shutting down")
	}

	pm := s.partitionManager
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	pm.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}

	if _, ok := tp[partition]; !ok {
		return 0, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	if !s.isOwnedPartition(topic, partition) {
		return 0, fmt.Errorf("%w: partition %d", errKafkaNotLeader, partition)
	}

	return pm.AppendRawBatch(ctx, topic, partition, batch)
}

func (s *Server) handleKafkaInitProducerID(ctx context.Context, req *kmsg.InitProducerIDRequest) (*kmsg.InitProducerIDResponse, error) {
	resp := kmsg.NewPtrInitProducerIDResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ProducerID = -1
	resp.ProducerEpoch = -1

	if req.TransactionalID != nil {
		resp.ErrorCode = kafkaErrorInvalidRequest
		return resp, nil
	}

	id, err := s.idempotencyManager.AllocateProducerID(ctx)
	if err != nil {
		return nil, err
	}
	resp.ProducerID = int64(id)
	resp.ProducerEpoch = 0
	return resp, nil
}

// handleKafkaFetch is the Kafka fetch path entry point.
// For the current simple integration, the handler returns Kafka record batches
// synthesized from retained internal messages plus the partition offset view.
func (s *Server) handleKafkaFetch(topic string, partition int, startOffset uint64, maxBytes int32) (KafkaFetchResult, error) {
	pm := s.partitionManager
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	pm.mu.RUnlock()
	if !ok {
		return KafkaFetchResult{}, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}

	ps, ok := tp[partition]
	if !ok {
		return KafkaFetchResult{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	ps.mu.RLock()
	highWatermark, hwOK := readableHighWatermark(ps)
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()

	raw, _, err := s.partitionManager.ReadRawBatches(context.Background(), topic, partition, int64(startOffset), int(maxBytes))
	if err != nil {
		return KafkaFetchResult{}, err
	}

	msgs := make([]log.Message, 0)
	if len(raw) > 0 {
		batches, err := log.ReadSegmentBatches(raw, startOffset, 0)
		if err != nil {
			return KafkaFetchResult{}, err
		}
		for _, batch := range batches {
			decoded, err := log.DecodeRecordBatch(batch)
			if err != nil {
				return KafkaFetchResult{}, err
			}
			msgs = append(msgs, decoded...)
		}
	}

	var out []byte
	remaining := int(maxBytes)
	if maxBytes <= 0 {
		remaining = 1 << 20
	}
	for len(msgs) > 0 {
		batchSize := len(msgs)
		batch := encodeKafkaRecordBatch(msgs[:batchSize])
		if len(batch) == 0 {
			break
		}
		if len(out) > 0 && len(out)+len(batch) > remaining {
			break
		}
		if len(out) == 0 && len(batch) > remaining {
			return KafkaFetchResult{
				RecordBatches:    batch,
				HighWatermark:    kafkaFetchHighWatermark(highWatermark, hwOK, nextOffset),
				LastStableOffset: kafkaFetchHighWatermark(highWatermark, hwOK, nextOffset),
			}, nil
		}
		out = append(out, batch...)
		break
	}
	hw := kafkaFetchHighWatermark(highWatermark, hwOK, nextOffset)
	return KafkaFetchResult{
		RecordBatches:    out,
		HighWatermark:    hw,
		LastStableOffset: hw,
	}, nil
}

func (s *Server) handleKafkaFetchRawBatches(ctx context.Context, topic string, partition int, startOffset int64, maxBytes int) ([]byte, int64, error) {
	return s.partitionManager.ReadRawBatches(ctx, topic, partition, startOffset, maxBytes)
}

func kafkaFetchHighWatermark(highWatermark uint64, ok bool, nextOffset uint64) int64 {
	if ok {
		return int64(highWatermark)
	}
	return int64(nextOffset)
}

func (s *Server) handleKafkaMetadata(ctx context.Context, req *kmsg.MetadataRequest) (*kmsg.MetadataResponse, error) {
	resp := kmsg.NewPtrMetadataResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	instanceInfos, err := s.registry.ActiveInstanceInfos(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(instanceInfos, func(i, j int) bool {
		return instanceInfos[i].InstanceID < instanceInfos[j].InstanceID
	})
	brokerIDs := make(map[string]int32, len(instanceInfos))
	for _, info := range instanceInfos {
		if info.KafkaAddress == "" {
			continue
		}
		brokerID := kafkaBrokerID(info.InstanceID)
		brokerIDs[info.InstanceID] = brokerID
		host, port := splitKafkaBrokerAddr(info.KafkaAddress)
		resp.Brokers = append(resp.Brokers, kmsg.MetadataResponseBroker{
			NodeID: brokerID,
			Host:   host,
			Port:   port,
		})
	}

	requested := requestedMetadataTopics(req)
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		if len(requested) > 0 && !requested[topic.Name] {
			continue
		}

		assignments, err := s.assignmentStore.Read(ctx, topic.Name)
		if err != nil {
			continue
		}

		topicResp := kmsg.NewMetadataResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topic.Name)
		for partitionID := 0; partitionID < topic.Partitions; partitionID++ {
			assignment, ok := assignments.Partitions[partitionID]
			if !ok {
				continue
			}

			partResp := kmsg.NewMetadataResponseTopicPartition()
			partResp.Partition = int32(partitionID)
			partResp.Leader = brokerIDs[assignment.Leader]
			for _, replica := range assignment.Replicas {
				if brokerID, ok := brokerIDs[replica]; ok {
					partResp.Replicas = append(partResp.Replicas, brokerID)
				}
			}

			isrState, err := s.isrStore.Read(ctx, topic.Name, partitionID)
			if err == nil {
				for _, isrReplica := range isrState.ISR {
					if brokerID, ok := brokerIDs[isrReplica]; ok {
						partResp.ISR = append(partResp.ISR, brokerID)
					}
				}
			}
			if len(partResp.ISR) == 0 {
				partResp.ISR = append(partResp.ISR, partResp.Replicas...)
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

func (s *Server) handleKafkaFindCoordinator(_ context.Context, req *kmsg.FindCoordinatorRequest) (*kmsg.FindCoordinatorResponse, error) {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if req.GetVersion() >= 4 {
		keys := req.CoordinatorKeys
		if len(keys) == 0 && req.CoordinatorKey != "" {
			keys = []string{req.CoordinatorKey}
		}
		for _, key := range keys {
			brokerID, host, port, err := s.kafkaControllerBroker(context.Background())
			if err != nil {
				return nil, err
			}
			resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
				Key:    key,
				NodeID: brokerID,
				Host:   host,
				Port:   port,
			})
		}
		return resp, nil
	}

	brokerID, host, port, err := s.kafkaControllerBroker(context.Background())
	if err != nil {
		return nil, err
	}
	resp.NodeID = brokerID
	resp.Host = host
	resp.Port = port
	return resp, nil
}

func (s *Server) handleKafkaCreateTopics(ctx context.Context, req *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error) {
	resp := kmsg.NewPtrCreateTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if !s.amLeader() {
		for _, topic := range req.Topics {
			topicResp := kmsg.NewCreateTopicsResponseTopic()
			topicResp.Topic = topic.Topic
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	seen := make(map[string]bool, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewCreateTopicsResponseTopic()
		topicResp.Topic = topic.Topic
		topicResp.NumPartitions = topic.NumPartitions
		topicResp.ReplicationFactor = topic.ReplicationFactor

		reqBody, errCode, errMsg := s.kafkaCreateTopicRequest(topic)
		if errCode == 0 && seen[topic.Topic] {
			errCode = kafkaErrorInvalidRequest
			errMsg = "duplicate topic in request"
		}
		seen[topic.Topic] = true

		if errCode == 0 && !req.ValidateOnly {
			tc, err := s.createTopic(ctx, reqBody)
			switch {
			case err == nil:
				topicResp.NumPartitions = int32(tc.Partitions)
				topicResp.ReplicationFactor = int16(tc.ReplicationFactor)
			case strings.Contains(err.Error(), "already exists"):
				errCode = kafkaErrorTopicAlreadyExists
				errMsg = err.Error()
			case strings.Contains(err.Error(), "replication_factor"):
				errCode = kafkaErrorInvalidReplication
				errMsg = err.Error()
			case strings.Contains(err.Error(), "min_insync_replicas"):
				errCode = kafkaErrorInvalidConfig
				errMsg = err.Error()
			default:
				return nil, err
			}
		}

		if errCode != 0 {
			topicResp.ErrorCode = errCode
			topicResp.ErrorMessage = kmsg.StringPtr(errMsg)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDeleteTopics(ctx context.Context, req *kmsg.DeleteTopicsRequest) (*kmsg.DeleteTopicsResponse, error) {
	resp := kmsg.NewPtrDeleteTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	var topicNames []string
	if req.GetVersion() <= 5 {
		topicNames = append(topicNames, req.TopicNames...)
	} else {
		for _, topic := range req.Topics {
			if topic.Topic != nil {
				topicNames = append(topicNames, *topic.Topic)
				continue
			}
			topicResp := kmsg.NewDeleteTopicsResponseTopic()
			topicResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "topic IDs are not supported"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
		}
	}

	for _, topicName := range topicNames {
		topicResp := kmsg.NewDeleteTopicsResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topicName)
		if !s.amLeader() {
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if err := s.deleteTopic(ctx, topicName); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
			} else {
				return nil, err
			}
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaCreatePartitions(ctx context.Context, req *kmsg.CreatePartitionsRequest) (*kmsg.CreatePartitionsResponse, error) {
	resp := kmsg.NewPtrCreatePartitionsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	seen := make(map[string]bool, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewCreatePartitionsResponseTopic()
		topicResp.Topic = topic.Topic

		if !s.amLeader() {
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if seen[topic.Topic] {
			topicResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "duplicate topic in request"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		seen[topic.Topic] = true

		if len(topic.Assignment) > 0 {
			topicResp.ErrorCode = kafkaErrorInvalidReplicaAssign
			msg := "manual partition assignment is not supported"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, topic.Topic)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Topics = append(resp.Topics, topicResp)
				continue
			}
			return nil, err
		}
		if int(topic.Count) <= tc.Partitions {
			topicResp.ErrorCode = kafkaErrorInvalidPartitions
			msg := "partition count must increase"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if req.ValidateOnly {
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		tc.Partitions = int(topic.Count)
		if err := s.topicStore.Update(ctx, tc); err != nil {
			return nil, err
		}
		s.publishAssignmentsForTopics(ctx, []meta.TopicConfig{tc})
		s.applyAssignmentsForTopic(ctx, tc.Name, tc.Partitions)
		if err := s.partitionManager.AddTopicPartitions(ctx, tc, s.getOwnedEpochs(tc.Name)); err != nil {
			return nil, err
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDescribeConfigs(ctx context.Context, req *kmsg.DescribeConfigsRequest) (*kmsg.DescribeConfigsResponse, error) {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewDescribeConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			tc, err := s.topicStore.Get(ctx, resource.ResourceName)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
					resp.Resources = append(resp.Resources, resourceResp)
					continue
				}
				return nil, err
			}
			configs := kafkaTopicDescribeConfigs(tc)
			resourceResp.Configs = filterDescribeConfigs(configs, resource.ConfigNames)
		default:
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic configs are supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
		}

		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaAlterConfigs(ctx context.Context, req *kmsg.AlterConfigsRequest) (*kmsg.AlterConfigsResponse, error) {
	resp := kmsg.NewPtrAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewAlterConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		if !s.amLeader() {
			resourceResp.ErrorCode = kafkaErrorNotController
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic {
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic config mutation is supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, resource.ResourceName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Resources = append(resp.Resources, resourceResp)
				continue
			}
			return nil, err
		}
		next, err := applyKafkaTopicConfigs(tc, kafkaAlterConfigsToMap(resource.Configs), true)
		if err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if !req.ValidateOnly {
			if err := s.topicStore.Update(ctx, next); err != nil {
				return nil, err
			}
		}
		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaIncrementalAlterConfigs(ctx context.Context, req *kmsg.IncrementalAlterConfigsRequest) (*kmsg.IncrementalAlterConfigsResponse, error) {
	resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewIncrementalAlterConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		if !s.amLeader() {
			resourceResp.ErrorCode = kafkaErrorNotController
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic {
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic config mutation is supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, resource.ResourceName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Resources = append(resp.Resources, resourceResp)
				continue
			}
			return nil, err
		}
		next, err := applyKafkaTopicIncrementalConfigs(tc, resource.Configs)
		if err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if !req.ValidateOnly {
			if err := s.topicStore.Update(ctx, next); err != nil {
				return nil, err
			}
		}
		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDescribeCluster(ctx context.Context, req *kmsg.DescribeClusterRequest) (*kmsg.DescribeClusterResponse, error) {
	resp := kmsg.NewPtrDescribeClusterResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ClusterID = s.kafkaClusterID()
	resp.EndpointType = req.EndpointType

	instanceInfos, err := s.registry.ActiveInstanceInfos(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(instanceInfos, func(i, j int) bool {
		return instanceInfos[i].InstanceID < instanceInfos[j].InstanceID
	})
	for _, info := range instanceInfos {
		if info.KafkaAddress == "" {
			continue
		}
		host, port := splitKafkaBrokerAddr(info.KafkaAddress)
		resp.Brokers = append(resp.Brokers, kmsg.DescribeClusterResponseBroker{
			NodeID: kafkaBrokerID(info.InstanceID),
			Host:   host,
			Port:   port,
		})
	}

	lease, err := s.leaderElection.GetLeader(ctx)
	if err == nil && lease.InstanceID != "" {
		resp.ControllerID = kafkaBrokerID(lease.InstanceID)
	} else {
		resp.ControllerID = kafkaBrokerID(s.instanceID)
	}
	return resp, nil
}

func (s *Server) handleKafkaCreateACLs(ctx context.Context, req *kmsg.CreateACLsRequest) (*kmsg.CreateACLsResponse, error) {
	resp := kmsg.NewPtrCreateACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	records := make([]storage.ACLRecord, 0, len(req.Creations))
	for _, creation := range req.Creations {
		result := kmsg.NewCreateACLsResponseResult()
		record, err := kafkaACLRecordFromCreation(creation)
		if err != nil {
			result.ErrorCode = kafkaErrorInvalidRequest
			msg := err.Error()
			result.ErrorMessage = kmsg.StringPtr(msg)
			resp.Results = append(resp.Results, result)
			continue
		}
		if !s.amLeader() {
			result.ErrorCode = kafkaErrorNotController
			resp.Results = append(resp.Results, result)
			continue
		}
		records = append(records, record)
		resp.Results = append(resp.Results, result)
	}

	if len(records) > 0 {
		if err := s.aclStore.Create(ctx, records); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Server) handleKafkaDescribeACLs(ctx context.Context, req *kmsg.DescribeACLsRequest) (*kmsg.DescribeACLsResponse, error) {
	resp := kmsg.NewPtrDescribeACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	filter, err := kafkaACLFilterFromDescribeRequest(req)
	if err != nil {
		resp.ErrorCode = kafkaErrorInvalidRequest
		msg := err.Error()
		resp.ErrorMessage = kmsg.StringPtr(msg)
		return resp, nil
	}

	acls, err := s.aclStore.List(ctx)
	if err != nil {
		return nil, err
	}

	grouped := make(map[string]*kmsg.DescribeACLsResponseResource)
	keys := make([]string, 0)
	for _, acl := range acls {
		if !filter.Matches(acl) {
			continue
		}
		key := kafkaACLResourceKey(acl)
		resource, ok := grouped[key]
		if !ok {
			resource = &kmsg.DescribeACLsResponseResource{
				ResourceType:        acl.ResourceType,
				ResourceName:        acl.ResourceName,
				ResourcePatternType: acl.ResourcePatternType,
			}
			grouped[key] = resource
			keys = append(keys, key)
		}
		resource.ACLs = append(resource.ACLs, kmsg.DescribeACLsResponseResourceACL{
			Principal:      acl.Principal,
			Host:           acl.Host,
			Operation:      acl.Operation,
			PermissionType: acl.PermissionType,
		})
	}
	sort.Strings(keys)
	for _, key := range keys {
		resp.Resources = append(resp.Resources, *grouped[key])
	}
	return resp, nil
}

func (s *Server) handleKafkaDeleteACLs(ctx context.Context, req *kmsg.DeleteACLsRequest) (*kmsg.DeleteACLsResponse, error) {
	resp := kmsg.NewPtrDeleteACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if !s.amLeader() {
		for range req.Filters {
			result := kmsg.NewDeleteACLsResponseResult()
			result.ErrorCode = kafkaErrorNotController
			resp.Results = append(resp.Results, result)
		}
		return resp, nil
	}

	filters := make([]storage.ACLFilter, 0, len(req.Filters))
	for _, filterReq := range req.Filters {
		filter, err := kafkaACLFilterFromDeleteFilter(filterReq)
		if err != nil {
			result := kmsg.NewDeleteACLsResponseResult()
			result.ErrorCode = kafkaErrorInvalidRequest
			msg := err.Error()
			result.ErrorMessage = kmsg.StringPtr(msg)
			resp.Results = append(resp.Results, result)
			continue
		}
		filters = append(filters, filter)
		resp.Results = append(resp.Results, kmsg.NewDeleteACLsResponseResult())
	}

	if len(filters) == 0 {
		return resp, nil
	}

	matched, err := s.aclStore.DeleteMatching(ctx, filters)
	if err != nil {
		return nil, err
	}

	matchIdx := 0
	for i := range resp.Results {
		if resp.Results[i].ErrorCode != 0 {
			continue
		}
		for _, acl := range matched[matchIdx] {
			resp.Results[i].MatchingACLs = append(resp.Results[i].MatchingACLs, kmsg.DeleteACLsResponseResultMatchingACL{
				ResourceType:        acl.ResourceType,
				ResourceName:        acl.ResourceName,
				ResourcePatternType: acl.ResourcePatternType,
				Principal:           acl.Principal,
				Host:                acl.Host,
				Operation:           acl.Operation,
				PermissionType:      acl.PermissionType,
			})
		}
		matchIdx++
	}
	return resp, nil
}

func (s *Server) kafkaCreateTopicRequest(topic kmsg.CreateTopicsRequestTopic) (createTopicRequest, int16, string) {
	reqBody := createTopicRequest{
		Name:              topic.Topic,
		Partitions:        int(topic.NumPartitions),
		ReplicationFactor: int(topic.ReplicationFactor),
	}
	if reqBody.Name == "" {
		return reqBody, kafkaErrorInvalidRequest, "topic name is required"
	}
	if len(topic.ReplicaAssignment) > 0 {
		return reqBody, kafkaErrorInvalidReplicaAssign, "manual replica assignment is not supported"
	}
	if reqBody.Partitions == -1 {
		reqBody.Partitions = 1
	}
	if reqBody.ReplicationFactor == -1 {
		reqBody.ReplicationFactor = 1
	}
	if reqBody.Partitions < 1 {
		return reqBody, kafkaErrorInvalidPartitions, "partitions must be at least 1"
	}
	if reqBody.ReplicationFactor < 1 {
		return reqBody, kafkaErrorInvalidReplication, "replication factor must be at least 1"
	}

	for _, cfg := range topic.Configs {
		if cfg.Value == nil {
			continue
		}
		switch cfg.Name {
		case "retention.ms":
			ms, err := strconv.ParseInt(*cfg.Value, 10, 64)
			if err != nil || ms < 0 {
				return reqBody, kafkaErrorInvalidConfig, "invalid retention.ms"
			}
			reqBody.Retention = fmt.Sprintf("%dms", ms)
		case "min.insync.replicas":
			minISR, err := strconv.Atoi(*cfg.Value)
			if err != nil || minISR < 1 {
				return reqBody, kafkaErrorInvalidConfig, "invalid min.insync.replicas"
			}
			reqBody.MinInsyncReplicas = minISR
		case "unclean.leader.election.enable":
			v, err := strconv.ParseBool(*cfg.Value)
			if err != nil {
				return reqBody, kafkaErrorInvalidConfig, "invalid unclean.leader.election.enable"
			}
			reqBody.UncleanLeaderElection = v
		case "cleanup.policy":
			if *cfg.Value != "delete" {
				return reqBody, kafkaErrorInvalidConfig, "only cleanup.policy=delete is supported"
			}
		default:
			return reqBody, kafkaErrorInvalidConfig, fmt.Sprintf("unsupported topic config %q", cfg.Name)
		}
	}

	return reqBody, 0, ""
}

func kafkaTopicDescribeConfigs(tc meta.TopicConfig) []kmsg.DescribeConfigsResponseResourceConfig {
	retentionMs := fmt.Sprintf("%d", tc.Retention/time.Millisecond)
	minISR := fmt.Sprintf("%d", tc.MinInsyncReplicas)
	cleanupPolicy := "delete"
	unclean := strconv.FormatBool(tc.UncleanLeaderElection)

	return []kmsg.DescribeConfigsResponseResourceConfig{
		{Name: "cleanup.policy", Value: kmsg.StringPtr(cleanupPolicy), IsDefault: true},
		{Name: "min.insync.replicas", Value: kmsg.StringPtr(minISR), IsDefault: false},
		{Name: "retention.ms", Value: kmsg.StringPtr(retentionMs), IsDefault: false},
		{Name: "unclean.leader.election.enable", Value: kmsg.StringPtr(unclean), IsDefault: !tc.UncleanLeaderElection},
	}
}

func kafkaAlterConfigsToMap(configs []kmsg.AlterConfigsRequestResourceConfig) map[string]*string {
	out := make(map[string]*string, len(configs))
	for _, cfg := range configs {
		out[cfg.Name] = cfg.Value
	}
	return out
}

func applyKafkaTopicIncrementalConfigs(tc meta.TopicConfig, configs []kmsg.IncrementalAlterConfigsRequestResourceConfig) (meta.TopicConfig, error) {
	values := map[string]*string{}
	for _, cfg := range configs {
		switch cfg.Op {
		case kmsg.IncrementalAlterConfigOpSet:
			values[cfg.Name] = cfg.Value
		case kmsg.IncrementalAlterConfigOpDelete:
			values[cfg.Name] = nil
		default:
			return tc, fmt.Errorf("unsupported incremental alter op for %q", cfg.Name)
		}
	}
	return applyKafkaTopicConfigs(tc, values, false)
}

func applyKafkaTopicConfigs(tc meta.TopicConfig, values map[string]*string, resetMissing bool) (meta.TopicConfig, error) {
	next := tc

	if resetMissing {
		next.Retention = 7 * 24 * time.Hour
		next.MinInsyncReplicas = 1
		next.UncleanLeaderElection = false
	}

	for name, value := range values {
		switch name {
		case "cleanup.policy":
			if value == nil {
				continue
			}
			if *value != "delete" {
				return tc, fmt.Errorf("only cleanup.policy=delete is supported")
			}
		case "retention.ms":
			if value == nil {
				next.Retention = 7 * 24 * time.Hour
				continue
			}
			ms, err := strconv.ParseInt(*value, 10, 64)
			if err != nil || ms < 0 {
				return tc, fmt.Errorf("invalid retention.ms")
			}
			next.Retention = time.Duration(ms) * time.Millisecond
		case "min.insync.replicas":
			if value == nil {
				next.MinInsyncReplicas = 1
				continue
			}
			v, err := strconv.Atoi(*value)
			if err != nil || v < 1 || v > next.ReplicationFactor {
				return tc, fmt.Errorf("invalid min.insync.replicas")
			}
			next.MinInsyncReplicas = v
		case "unclean.leader.election.enable":
			if value == nil {
				next.UncleanLeaderElection = false
				continue
			}
			v, err := strconv.ParseBool(*value)
			if err != nil {
				return tc, fmt.Errorf("invalid unclean.leader.election.enable")
			}
			next.UncleanLeaderElection = v
		default:
			return tc, fmt.Errorf("unsupported topic config %q", name)
		}
	}

	return next, nil
}

func filterDescribeConfigs(configs []kmsg.DescribeConfigsResponseResourceConfig, names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	if len(names) == 0 {
		return configs
	}
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	filtered := make([]kmsg.DescribeConfigsResponseResourceConfig, 0, len(configs))
	for _, cfg := range configs {
		if allowed[cfg.Name] {
			filtered = append(filtered, cfg)
		}
	}
	return filtered
}

func (s *Server) kafkaClusterID() string {
	if s.cfg.Storage.Bucket != "" {
		return "camu-" + s.cfg.Storage.Bucket
	}
	return "camu"
}

func kafkaACLRecordFromCreation(creation kmsg.CreateACLsRequestCreation) (storage.ACLRecord, error) {
	record := storage.ACLRecord{
		ResourceType:        creation.ResourceType,
		ResourceName:        creation.ResourceName,
		ResourcePatternType: creation.ResourcePatternType,
		Principal:           creation.Principal,
		Host:                creation.Host,
		Operation:           creation.Operation,
		PermissionType:      creation.PermissionType,
	}
	if creation.ResourceType == kmsg.ACLResourceTypeUnknown || creation.ResourceType == kmsg.ACLResourceTypeAny {
		return record, fmt.Errorf("invalid acl resource type")
	}
	if creation.ResourceName == "" {
		return record, fmt.Errorf("acl resource name is required")
	}
	if creation.ResourcePatternType != kmsg.ACLResourcePatternTypeLiteral && creation.ResourcePatternType != kmsg.ACLResourcePatternTypePrefixed {
		return record, fmt.Errorf("unsupported acl resource pattern type")
	}
	if creation.Principal == "" {
		return record, fmt.Errorf("acl principal is required")
	}
	if creation.Host == "" {
		return record, fmt.Errorf("acl host is required")
	}
	if creation.Operation == kmsg.ACLOperationUnknown || creation.Operation == kmsg.ACLOperationAny {
		return record, fmt.Errorf("invalid acl operation")
	}
	if creation.PermissionType != kmsg.ACLPermissionTypeAllow && creation.PermissionType != kmsg.ACLPermissionTypeDeny {
		return record, fmt.Errorf("invalid acl permission type")
	}
	return record, nil
}

func kafkaACLFilterFromDescribeRequest(req *kmsg.DescribeACLsRequest) (storage.ACLFilter, error) {
	return kafkaACLFilter{
		resourceType:        req.ResourceType,
		resourceName:        req.ResourceName,
		resourcePatternType: req.ResourcePatternType,
		principal:           req.Principal,
		host:                req.Host,
		operation:           req.Operation,
		permissionType:      req.PermissionType,
	}.build()
}

func kafkaACLFilterFromDeleteFilter(req kmsg.DeleteACLsRequestFilter) (storage.ACLFilter, error) {
	return kafkaACLFilter{
		resourceType:        req.ResourceType,
		resourceName:        req.ResourceName,
		resourcePatternType: req.ResourcePatternType,
		principal:           req.Principal,
		host:                req.Host,
		operation:           req.Operation,
		permissionType:      req.PermissionType,
	}.build()
}

type kafkaACLFilter struct {
	resourceType        kmsg.ACLResourceType
	resourceName        *string
	resourcePatternType kmsg.ACLResourcePatternType
	principal           *string
	host                *string
	operation           kmsg.ACLOperation
	permissionType      kmsg.ACLPermissionType
}

func (f kafkaACLFilter) build() (storage.ACLFilter, error) {
	filter := storage.ACLFilter{
		ResourceType:        f.resourceType,
		ResourceName:        f.resourceName,
		ResourcePatternType: f.resourcePatternType,
		Principal:           f.principal,
		Host:                f.host,
		Operation:           f.operation,
		PermissionType:      f.permissionType,
	}
	if f.resourceType == kmsg.ACLResourceTypeUnknown {
		return filter, fmt.Errorf("invalid acl resource type")
	}
	switch f.resourcePatternType {
	case kmsg.ACLResourcePatternTypeAny, kmsg.ACLResourcePatternTypeMatch, kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypePrefixed:
	default:
		return filter, fmt.Errorf("invalid acl resource pattern type")
	}
	if f.operation == kmsg.ACLOperationUnknown {
		return filter, fmt.Errorf("invalid acl operation")
	}
	if f.permissionType == kmsg.ACLPermissionTypeUnknown {
		return filter, fmt.Errorf("invalid acl permission type")
	}
	return filter, nil
}

func kafkaACLResourceKey(acl storage.ACLRecord) string {
	return fmt.Sprintf("%d:%s:%d", acl.ResourceType, acl.ResourceName, acl.ResourcePatternType)
}

func (s *Server) kafkaControllerBroker(ctx context.Context) (int32, string, int32, error) {
	lease, err := s.leaderElection.GetLeader(ctx)
	if err == nil && lease.InstanceID != "" && time.Now().Before(lease.ExpiresAt) {
		info, infoErr := s.registry.GetInstanceInfo(ctx, lease.InstanceID)
		if infoErr == nil && info.KafkaAddress != "" {
			host, port := splitKafkaBrokerAddr(info.KafkaAddress)
			return kafkaBrokerID(info.InstanceID), host, port, nil
		}
	}

	host, port := splitKafkaBrokerAddr(kafkaAdvertiseAddr(s.instanceID, s.Address(), s.cfg.Server.KafkaPort))
	return kafkaBrokerID(s.instanceID), host, port, nil
}

func (s *Server) handleKafkaJoinGroup(_ context.Context, req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, error) {
	if !s.isLocalKafkaCoordinator(context.Background(), req.Group) {
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.ErrorCode = kafkaErrorNotCoordinator
		resp.Generation = -1
		setKafkaResponseVersion(resp, req.GetVersion())
		return resp, nil
	}
	resp, err := s.groupCoord.joinGroup(context.Background(), req)
	if err != nil {
		return nil, err
	}
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (s *Server) handleKafkaDescribeGroups(_ context.Context, req *kmsg.DescribeGroupsRequest) (*kmsg.DescribeGroupsResponse, error) {
	resp := kmsg.NewPtrDescribeGroupsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, groupID := range req.Groups {
		if !s.isLocalKafkaCoordinator(context.Background(), groupID) {
			groupResp := kmsg.NewDescribeGroupsResponseGroup()
			groupResp.Group = groupID
			groupResp.ErrorCode = kafkaErrorNotCoordinator
			resp.Groups = append(resp.Groups, groupResp)
			continue
		}

		groupResp, err := s.groupCoord.describeGroup(context.Background(), groupID)
		if err != nil {
			return nil, err
		}
		resp.Groups = append(resp.Groups, groupResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaListGroups(_ context.Context, req *kmsg.ListGroupsRequest) (*kmsg.ListGroupsResponse, error) {
	resp, err := s.groupCoord.listGroups(context.Background(), req.StatesFilter, req.TypesFilter)
	if err != nil {
		return nil, err
	}
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (s *Server) handleKafkaDeleteGroups(_ context.Context, req *kmsg.DeleteGroupsRequest) (*kmsg.DeleteGroupsResponse, error) {
	resp := kmsg.NewPtrDeleteGroupsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, groupID := range req.Groups {
		groupResp := kmsg.NewDeleteGroupsResponseGroup()
		groupResp.Group = groupID
		if !s.isLocalKafkaCoordinator(context.Background(), groupID) {
			groupResp.ErrorCode = kafkaErrorNotCoordinator
			resp.Groups = append(resp.Groups, groupResp)
			continue
		}

		errorCode, err := s.groupCoord.deleteGroup(context.Background(), groupID)
		if err != nil {
			return nil, err
		}
		if errorCode == 0 {
			if err := s.offsetStore.DeleteGroup(context.Background(), groupID); err != nil {
				return nil, err
			}
		}
		groupResp.ErrorCode = errorCode
		resp.Groups = append(resp.Groups, groupResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaOffsetDelete(ctx context.Context, req *kmsg.OffsetDeleteRequest) (*kmsg.OffsetDeleteResponse, error) {
	resp := kmsg.NewPtrOffsetDeleteResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		resp.ErrorCode = kafkaErrorNotCoordinator
		for _, topic := range req.Topics {
			topicResp := kmsg.NewOffsetDeleteResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetDeleteResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	if _, err := s.s3Client.Get(ctx, kafkaGroupKey(req.Group)); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			resp.ErrorCode = kafkaErrorGroupIDNotFound
			return resp, nil
		}
		return nil, err
	}

	deletes := make(map[string][]int, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetDeleteResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetDeleteResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.ErrorCode = s.kafkaPartitionExists(ctx, topic.Topic, int(partition.Partition))
			if partResp.ErrorCode == 0 {
				deletes[topic.Topic] = append(deletes[topic.Topic], int(partition.Partition))
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	if len(deletes) > 0 {
		if err := s.offsetStore.DeleteGroupTopics(ctx, req.Group, deletes, s.currentControllerEpoch()); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Server) handleKafkaSyncGroup(_ context.Context, req *kmsg.SyncGroupRequest) (*kmsg.SyncGroupResponse, error) {
	if !s.isLocalKafkaCoordinator(context.Background(), req.Group) {
		resp := kmsg.NewPtrSyncGroupResponse()
		resp.ErrorCode = kafkaErrorNotCoordinator
		setKafkaResponseVersion(resp, req.GetVersion())
		return resp, nil
	}
	resp, err := s.groupCoord.syncGroup(context.Background(), req)
	if err != nil {
		return nil, err
	}
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (s *Server) handleKafkaHeartbeat(_ context.Context, req *kmsg.HeartbeatRequest) (*kmsg.HeartbeatResponse, error) {
	if !s.isLocalKafkaCoordinator(context.Background(), req.Group) {
		resp := kmsg.NewPtrHeartbeatResponse()
		resp.ErrorCode = kafkaErrorNotCoordinator
		setKafkaResponseVersion(resp, req.GetVersion())
		return resp, nil
	}
	resp, err := s.groupCoord.heartbeat(context.Background(), req)
	if err != nil {
		return nil, err
	}
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (s *Server) handleKafkaLeaveGroup(_ context.Context, req *kmsg.LeaveGroupRequest) (*kmsg.LeaveGroupResponse, error) {
	if !s.isLocalKafkaCoordinator(context.Background(), req.Group) {
		resp := kmsg.NewPtrLeaveGroupResponse()
		resp.ErrorCode = kafkaErrorNotCoordinator
		setKafkaResponseVersion(resp, req.GetVersion())
		return resp, nil
	}
	resp, err := s.groupCoord.leaveGroup(context.Background(), req)
	if err != nil {
		return nil, err
	}
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (s *Server) isLocalKafkaCoordinator(ctx context.Context, groupKey string) bool {
	brokerID, _, _, err := s.kafkaControllerBroker(ctx)
	if err != nil {
		return true
	}
	return brokerID == kafkaBrokerID(s.instanceID)
}

func (s *Server) currentControllerEpoch() string {
	if s.leaderLease.ETag != "" {
		return s.leaderLease.ETag
	}
	lease, err := s.leaderElection.GetLeader(context.Background())
	if err != nil {
		return ""
	}
	return lease.ETag
}

func (s *Server) handleKafkaListOffsets(ctx context.Context, topic string, partition int, timestamp int64) (KafkaOffsetResponse, error) {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	ps := s.partitionManager.GetPartitionState(topic, partition)
	if ps == nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}
	if topicCfg.ReplicationFactor > 1 && ps.replicaState == nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d", errKafkaLeaderNotAvailable, partition)
	}

	logStartOffset := uint64(0)
	if firstOffset, ok := ps.index.FirstOffset(); ok {
		logStartOffset = firstOffset
	}
	ps.mu.RLock()
	if seg := ps.activeSegment; seg != nil {
		offsetIdx := seg.OffsetIndex()
		if len(offsetIdx) > 0 && (logStartOffset == 0 || uint64(offsetIdx[0].BaseOffset) < logStartOffset) {
			logStartOffset = uint64(offsetIdx[0].BaseOffset)
		}
	}
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()

	switch timestamp {
	case -4, -2:
		return KafkaOffsetResponse{
			Offset:      int64(logStartOffset),
			Timestamp:   -1,
			LeaderEpoch: int32(ps.epoch),
		}, nil
	case -1:
		return KafkaOffsetResponse{
			Offset:      int64(nextOffset),
			Timestamp:   -1,
			LeaderEpoch: int32(ps.epoch),
		}, nil
	default:
		startOffset, ok := ps.index.FirstOffsetForTimestamp(timestamp)
		if !ok {
			startOffset = logStartOffset
		}
		if offset, found, err := s.findKafkaOffsetByTimestamp(ctx, topic, partition, ps, startOffset, timestamp); err != nil {
			return KafkaOffsetResponse{}, err
		} else if found {
			return KafkaOffsetResponse{
				Offset:      int64(offset),
				Timestamp:   timestamp,
				LeaderEpoch: int32(ps.epoch),
			}, nil
		}
		return KafkaOffsetResponse{
			Offset:      -1,
			Timestamp:   -1,
			LeaderEpoch: int32(ps.epoch),
		}, nil
	}
}

func (s *Server) findKafkaOffsetByTimestamp(ctx context.Context, topic string, partition int, ps *partitionState, startOffset uint64, targetTimestamp int64) (uint64, bool, error) {
	index := ps.index
	if index != nil {
		var (
			foundOffset uint64
			found       bool
		)
		_, err := s.fetcher.Walk(ctx, index, topic, partition, startOffset, int(^uint(0)>>1), func(msg log.Message) bool {
			if normalizeTimestampForKafkaMillis(msg.Timestamp) >= targetTimestamp {
				foundOffset = msg.Offset
				found = true
				return false
			}
			return true
		})
		if err != nil {
			return 0, false, err
		}
		if found {
			return foundOffset, true, nil
		}
	}

	ps.mu.RLock()
	activeSeg := ps.activeSegment
	hw, hwOK := readableHighWatermark(ps)
	ps.mu.RUnlock()
	if activeSeg != nil {
		offsetIdx := activeSeg.OffsetIndex()
		for _, entry := range offsetIdx {
			if uint64(entry.LastOffset) < startOffset {
				continue
			}
			if hwOK && uint64(entry.BaseOffset) >= hw {
				break
			}
			if entry.BatchSize <= 0 || entry.Position < 0 {
				continue
			}
			buf := make([]byte, entry.BatchSize)
			n, err := activeSeg.ReadAt(buf, entry.Position)
			if err != nil && n < int(entry.BatchSize) {
				return 0, false, err
			}
			msgs, err := log.DecodeRecordBatch(buf[:n])
			if err != nil {
				return 0, false, err
			}
			for _, msg := range msgs {
				if msg.Offset < startOffset {
					continue
				}
				if hwOK && msg.Offset >= hw {
					return 0, false, nil
				}
				if normalizeTimestampForKafkaMillis(msg.Timestamp) >= targetTimestamp {
					return msg.Offset, true, nil
				}
			}
		}
	}
	return 0, false, nil
}

func normalizeTimestampForKafkaMillis(ts int64) int64 {
	if ts == 0 {
		return 0
	}
	if ts > 1_000_000_000_000_000 {
		return ts / int64(time.Millisecond)
	}
	return ts
}

func (s *Server) handleKafkaOffsetCommit(ctx context.Context, req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error) {
	resp := kmsg.NewPtrOffsetCommitResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		for _, topic := range req.Topics {
			topicResp := kmsg.NewOffsetCommitResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetCommitResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	offsetsByTopic := make(map[string]map[int]uint64, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetCommitResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetCommitResponseTopicPartition()
			partResp.Partition = partition.Partition

			errorCode := s.kafkaPartitionExists(ctx, topic.Topic, int(partition.Partition))
			if errorCode == 0 {
				if offsetsByTopic[topic.Topic] == nil {
					offsetsByTopic[topic.Topic] = make(map[int]uint64)
				}
				offsetsByTopic[topic.Topic][int(partition.Partition)] = uint64(partition.Offset)
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	if len(offsetsByTopic) > 0 {
		if err := s.offsetStore.CommitGroupTopicsWithEpoch(ctx, req.Group, offsetsByTopic, s.currentControllerEpoch()); err != nil {
			for ti := range resp.Topics {
				for pi := range resp.Topics[ti].Partitions {
					if resp.Topics[ti].Partitions[pi].ErrorCode == 0 {
						resp.Topics[ti].Partitions[pi].ErrorCode = kafkaErrorUnknownServer
					}
				}
			}
		}
	}

	return resp, nil
}

func (s *Server) handleKafkaOffsetFetch(ctx context.Context, req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error) {
	resp := kmsg.NewPtrOffsetFetchResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		requestedTopics := req.Topics
		for _, topic := range requestedTopics {
			topicResp := kmsg.NewOffsetFetchResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetFetchResponseTopicPartition()
				partResp.Partition = partition
				partResp.Offset = -1
				partResp.LeaderEpoch = -1
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	topics, err := s.offsetStore.GetGroupTopics(ctx, req.Group)
	if err != nil {
		return nil, err
	}

	requestedTopics := req.Topics
	if len(requestedTopics) == 0 {
		requestedTopics = make([]kmsg.OffsetFetchRequestTopic, 0, len(topics))
		for topic, partitions := range topics {
			topicReq := kmsg.NewOffsetFetchRequestTopic()
			topicReq.Topic = topic
			for partition := range partitions {
				topicReq.Partitions = append(topicReq.Partitions, int32(partition))
			}
			requestedTopics = append(requestedTopics, topicReq)
		}
	}

	for _, topic := range requestedTopics {
		topicResp := kmsg.NewOffsetFetchResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetFetchResponseTopicPartition()
			partResp.Partition = partition
			partResp.Offset = -1
			partResp.LeaderEpoch = -1

			errorCode := s.kafkaPartitionExists(ctx, topic.Topic, int(partition))
			if errorCode == 0 {
				if topicOffsets := topics[topic.Topic]; topicOffsets != nil {
					if offset, ok := topicOffsets[int(partition)]; ok {
						partResp.Offset = int64(offset)
					}
				}
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

func (s *Server) kafkaPartitionError(ctx context.Context, topic string, partition int) int16 {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return kafkaErrorUnknownTopicPartition
	}

	assignments, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	assignment, ok := assignments.Partitions[partition]
	if !ok {
		return kafkaErrorUnknownTopicPartition
	}
	if assignment.Leader != s.instanceID {
		return kafkaErrorNotLeader
	}
	return 0
}

func (s *Server) kafkaPartitionExists(ctx context.Context, topic string, partition int) int16 {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return kafkaErrorUnknownTopicPartition
	}
	return 0
}

// Shutdown gracefully shuts down the HTTP server and partition manager.
// Ordering:
//  1. Set shuttingDown so produce handlers reject new writes immediately.
//  2. Stop the HTTP server (drains in-flight requests).
//  3. Stop the batcher — flushes remaining local segment data to S3.
//  4. Stop lease renewal and release all owned leases.
func (s *Server) Shutdown(ctx context.Context) error {
	// 1. Stop accepting new writes.
	s.shuttingDown.Store(true)

	// 2. Shut down HTTP servers (waits for in-flight requests to finish).
	httpErr := s.httpServer.Shutdown(ctx)
	if err := s.internalServer.Shutdown(ctx); err != nil && httpErr == nil {
		httpErr = err
	}
	if s.kafkaServer != nil {
		if err := s.kafkaServer.Close(); err != nil && httpErr == nil {
			httpErr = err
		}
	}

	// 3. Cancel all follower fetch loops.
	s.partitionManager.CancelAllFetchLoops()

	// 4. Flush batcher / close local segments.
	pmErr := s.partitionManager.Shutdown(ctx)

	// 5. Stop coordination goroutine.
	close(s.leaseStop)
	s.leaseWg.Wait()

	// 6. Deregister from cluster.
	s.registry.Deregister(ctx)

	if httpErr != nil {
		return httpErr
	}
	if pmErr != nil {
		return pmErr
	}
	_ = s.registry.Deregister(ctx)
	return nil
}

// Address returns the actual listening address (host:port).
func (s *Server) Address() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Server.Address
}

func (s *Server) InternalAddress() string {
	if s.internalListener != nil {
		return s.internalListener.Addr().String()
	}
	return s.cfg.Server.InternalAddress
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

func kafkaAdvertiseAddr(instanceID, rawAddr string, kafkaPort int) string {
	host, _, err := net.SplitHostPort(rawAddr)
	if err != nil || host == "" {
		host = instanceID
	}
	if host == "0.0.0.0" || host == "::" {
		host = instanceID
	}
	return net.JoinHostPort(host, strconv.Itoa(kafkaPort))
}

func kafkaBrokerID(instanceID string) int32 {
	id := crc32.ChecksumIEEE([]byte(instanceID)) & 0x7fffffff
	if id == 0 {
		return 1
	}
	return int32(id)
}

func routablePeerAddress(instanceID, rawAddr string) string {
	host, port, err := net.SplitHostPort(rawAddr)
	if err != nil {
		if rawAddr == "" {
			return net.JoinHostPort(instanceID, "8081")
		}
		return rawAddr
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = instanceID
	}
	if port == "" {
		port = "8081"
	}
	return net.JoinHostPort(host, port)
}

// InstanceID returns the server's unique instance ID.
func (s *Server) InstanceID() string {
	return s.instanceID
}

type ProducerStateSnapshot struct {
	NextSeq    uint64
	LastOffset uint64
}

type PartitionStateSnapshot struct {
	NextOffset            uint64
	FlushedOffset         uint64
	IndexHighWatermark    uint64
	ReadableHighWatermark uint64
}

func (s *Server) ProducerStateSnapshot(topic string, partitionID int, producerID uint64) (ProducerStateSnapshot, bool) {
	ps := s.partitionManager.GetPartitionState(topic, partitionID)
	if ps == nil {
		return ProducerStateSnapshot{}, false
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	state, ok := ps.producerSeqs[producerID]
	if !ok {
		return ProducerStateSnapshot{}, false
	}
	return ProducerStateSnapshot{
		NextSeq:    state.NextSeq,
		LastOffset: state.LastOffset,
	}, true
}

func (s *Server) PartitionStateSnapshot(topic string, partitionID int) (PartitionStateSnapshot, bool) {
	ps := s.partitionManager.GetPartitionState(topic, partitionID)
	if ps == nil {
		return PartitionStateSnapshot{}, false
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	snapshot := PartitionStateSnapshot{
		NextOffset:         ps.nextOffset,
		FlushedOffset:      ps.flushedOffset,
		IndexHighWatermark: ps.index.HighWatermark(),
	}
	if hw, ok := readableHighWatermark(ps); ok {
		snapshot.ReadableHighWatermark = hw
	} else {
		snapshot.ReadableHighWatermark = snapshot.IndexHighWatermark
	}
	return snapshot, true
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

	// Start controller if we became leader during initial coordination.
	if s.amLeader() && s.controllerState.Load() == nil {
		s.startController(ctx)
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
			// Ignore version and leader epoch churn, but persist any leader or replica-set change.
			if !partitionAssignmentsChanged(existing.Partitions, newPartitions) {
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

func partitionAssignmentsChanged(existing, next map[int]coordination.PartitionAssignment) bool {
	if len(existing) != len(next) {
		return true
	}
	for pid, nextPartition := range next {
		currentPartition, ok := existing[pid]
		if !ok {
			return true
		}
		if currentPartition.Leader != nextPartition.Leader {
			return true
		}
		if !slices.Equal(currentPartition.Replicas, nextPartition.Replicas) {
			return true
		}
	}
	return false
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
		s.fallbackToSelfAssignmentOnError(err, topic, numPartitions)
		return
	}

	owned := make(map[int]localPartitionAssignment)
	for pid, pa := range assigned.Partitions {
		isLeader := pa.Leader == s.instanceID
		isReplica := false

		if slices.Contains(pa.Replicas, s.InstanceID()) {
			isReplica = true
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

// No assignments yet — single-instance fallback: own all partitions.
func (s *Server) fallbackToSelfAssignmentOnError(err error, topic string, numPartitions int) {
	if errors.Is(err, storage.ErrNotFound) {
		s.assignmentsMu.Lock()
		s.myPartitions[topic] = make(map[int]localPartitionAssignment)

		for i := range numPartitions {
			s.myPartitions[topic][i] = localPartitionAssignment{Owned: true}
		}

		s.assignmentsMu.Unlock()
		return
	}

	slog.Error("applyAssignments: read assignments", "topic", topic, "error", err)
	s.revokeTopic(topic)
}

// initPartitionAsLeader sets up replication state for a partition this instance
// leads: loads/appends epoch history, recovers the high watermark, creates
// ReplicaState, and writes the initial ISR to S3.
func (s *Server) initPartitionAsLeader(ctx context.Context, topic string, pid int, pa coordination.PartitionAssignment) {
	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return // not yet initialized, will be set up later
	}
	ps.mu.RLock()
	if ps.isLeader {
		ps.mu.RUnlock()
		return // already initialized as leader
	}
	ps.mu.RUnlock()

	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		slog.Error("initPartitionAsLeader: get topic", "topic", topic, "error", err)
		return
	}

	// If this partition was a follower, stop the fetch loop before recovery so
	// we don't race an old-leader append against local leader promotion.
	ps.mu.Lock()
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchCancel = nil
	ps.fetchDone = nil
	ps.mu.Unlock()
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	// Refresh the local index from S3 before recovering as leader. Assignment-
	// driven promotions do not go through the follower failover path, and the
	// in-memory index can otherwise miss flushed prefix segments from the old
	// leader, which causes the promoted leader to rebuild only its local tail.
	s.partitionManager.RefreshIndex(ctx, topic, pid)
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("initPartitionAsLeader: ensure active segment before recovery", "topic", topic, "partition", pid, "error", err)
	}

	// Recover true local log end from native storage.
	logEnd := s.partitionManager.recoverLocalLogEnd(topic, pid)

	// Load epoch history from S3 (authoritative), fall back to local file,
	// or use existing epochHistory if already set.
	ps.mu.RLock()
	eh := ps.epochHistory
	ps.mu.RUnlock()
	if eh == nil {
		ehPath := s.partitionManager.EpochHistoryPath(topic, pid)
		eh, _ = s.isrStore.ReadEpochHistory(ctx, topic, pid)
		if eh == nil || len(eh.Entries) == 0 {
			eh, _ = replication.LoadEpochHistory(ehPath)
			if eh == nil {
				eh = &replication.EpochHistory{}
			}
		}
	}
	eh.Append(replication.EpochEntry{Epoch: pa.LeaderEpoch, StartOffset: logEnd})
	ps.mu.Lock()
	// TOCTOU re-check: another goroutine may have promoted this partition
	// while we were doing heavy work (local recovery, index refresh).
	if ps.isLeader && ps.epoch >= pa.LeaderEpoch {
		ps.mu.Unlock()
		return // someone else promoted during our init
	}
	prevEpochHistory := ps.epochHistory
	ps.isLeader = true
	ps.leaderID = ""
	ps.epoch = pa.LeaderEpoch
	ps.nextOffset = logEnd
	ps.epochHistory = eh
	ps.mu.Unlock()
	if eh != prevEpochHistory {
		if err := eh.SaveToFile(s.partitionManager.EpochHistoryPath(topic, pid)); err != nil {
			slog.Warn("initPartitionAsLeader: save epoch history locally", "topic", topic, "partition", pid, "error", err)
		}
		if err := s.isrStore.WriteEpochHistory(ctx, topic, pid, eh); err != nil {
			slog.Warn("initPartitionAsLeader: save epoch history to S3", "topic", topic, "partition", pid, "error", err)
		}
	}

	// HW recovery:
	// rf=1: everything in the local log is committed, so HW = log end.
	// rf>1: recover from the most advanced local/persisted view, capped at log end.
	// Persisted HW metadata can lag a follower's local tail on reassignment; if we drop
	// back to the stale persisted value, the next flush truncates a safe prefix.
	recoveredHW := logEnd
	if topicCfg.ReplicationFactor > 1 {
		ps.mu.RLock()
		recoveredHW = ps.index.HighWatermark()
		ps.mu.RUnlock()
		isrState, err := s.isrStore.Read(ctx, topic, pid)
		if err == nil && isrState.HighWatermark > recoveredHW {
			recoveredHW = isrState.HighWatermark
		}
		if logEnd > recoveredHW {
			recoveredHW = logEnd
		}
	}
	ps.mu.RLock()
	if recoveredHW > ps.nextOffset {
		recoveredHW = ps.nextOffset
	}
	indexHW := ps.index.HighWatermark()
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()

	slog.Info("leader_recovery_state",
		"topic", topic,
		"partition", pid,
		"epoch", pa.LeaderEpoch,
		"log_end", logEnd,
		"next_offset", nextOffset,
		"index_hw", indexHW,
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

	// Update the in-memory index with the recovered HW so consumers see the correct value.
	ps.mu.Lock()
	ps.index.SetHighWatermark(recoveredHW)
	if topicCfg.ReplicationFactor > 1 {
		ps.replicaState = replication.NewReplicaState(s.instanceID, recoveredHW, topicCfg.MinInsyncReplicas, s.cfg.Coordination.ISRExpansionThresholdValue())
		ps.replicaState.SetEpochHistory(ps.epochHistory)
		for _, r := range pa.Replicas {
			if r != s.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}
	}
	ps.mu.Unlock()
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("initPartitionAsLeader: ensure active segment", "topic", topic, "partition", pid, "error", err)
	}

	if topicCfg.ReplicationFactor > 1 {
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

	// Recover producer idempotency state from S3 checkpoint + local tail.
	// This MUST happen before any flush — onFlush uploads the checkpoint from
	// ps.producerSeqs, so loading it first prevents overwriting a good checkpoint
	// with an empty one.
	checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, pid)
	if data, err := s.s3Client.Get(ctx, checkpointKey); err == nil && len(data) > 0 {
		ps.mu.Lock()
		ps.loadProducerCheckpoint(data)
		ps.mu.Unlock()
		slog.Info("idempotency_checkpoint_loaded", "topic", topic, "partition", pid, "size", len(data))
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		slog.Warn("idempotency_checkpoint_load_failed", "topic", topic, "partition", pid, "error", err)
	}

	if source, n := s.partitionManager.RebuildProducerStateFromLocalTail(topic, pid); n > 0 {
		slog.Info("idempotency_local_tail_recovery", "topic", topic, "partition", pid, "source", source, "batches", n)
	}

	// If this replica was promoted with a durable tail only in local native storage,
	// persist that recovered prefix immediately so leader reads can serve it
	// through the normal index/segment path.
	ps.mu.RLock()
	indexNextOffset := ps.index.NextOffset()
	logNextOffset := ps.nextOffset
	ps.mu.RUnlock()
	if recoveredHW > indexNextOffset {
		if err := s.partitionManager.flushRecoveredTail(topic, pid); err != nil {
			slog.Warn("initPartitionAsLeader: flush recovered tail",
				"topic", topic,
				"partition", pid,
				"epoch", pa.LeaderEpoch,
				"recovered_hw", recoveredHW,
				"index_next_offset", indexNextOffset,
				"error", err,
			)
		}
	}

	slog.Info("partition_leader_init", "topic", topic, "partition", pid,
		"epoch", pa.LeaderEpoch, "hw", recoveredHW, "next_offset", logNextOffset, "replicas", len(pa.Replicas))
}

// initPartitionAsFollower sets up a fetch loop for a partition this instance
// replicates as a follower. Resolves the leader address from the registry and
// starts a background FollowerFetcher goroutine.
func (s *Server) initPartitionAsFollower(ctx context.Context, topic string, pid int, pa coordination.PartitionAssignment) {
	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return // not yet initialized
	}

	ps.mu.RLock()
	if !ps.isLeader && ps.fetchCancel != nil && ps.leaderID == pa.Leader {
		ps.mu.RUnlock()
		return
	}
	localNextOffset := ps.nextOffset
	localEpoch := ps.epoch
	flushedOffset := ps.flushedOffset
	indexHW := ps.index.HighWatermark()
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.mu.RUnlock()

	// If fetchCancel is nil but we're supposed to be a follower, the previous
	// fetch loop may have exited (leader-down or error). Re-init.

	slog.Info("follower_transition",
		"topic", topic,
		"partition", pid,
		"leader", pa.Leader,
		"leader_epoch", pa.LeaderEpoch,
		"local_next_offset", localNextOffset,
		"local_epoch", localEpoch,
		"flushed_offset", flushedOffset,
		"index_hw", indexHW,
	)

	// Resolve leader address. Use the internal address (h2c) for replication
	// traffic. The registry stores the listener bind address (e.g. "[::]:8081")
	// which is useless for inter-node comms — extract port and combine with
	// the leader's instanceID (hostname).
	leaderInfo, err := s.registry.GetInstanceInfo(ctx, pa.Leader)
	if err != nil {
		slog.Warn("initPartitionAsFollower: resolve leader", "leader", pa.Leader, "error", err)
		return
	}
	addr := leaderInfo.InternalAddress
	if addr == "" {
		addr = leaderInfo.Address // fallback for rolling upgrades
	}
	leaderAddr := routablePeerAddress(pa.Leader, addr)

	// Cancel existing fetch loop and wait for it to finish.
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	// Start follower fetch loop.
	ps.mu.Lock()
	ps.isLeader = false
	ps.leaderID = pa.Leader
	ps.replicaState = nil
	localOffset := ps.nextOffset
	fetchEpoch := localEpoch
	fetchDone := make(chan struct{})
	fetchCtx, cancel := context.WithCancel(context.Background())
	ps.fetchCancel = cancel
	ps.fetchDone = fetchDone
	ps.mu.Unlock()
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("initPartitionAsFollower: ensure active segment", "topic", topic, "partition", pid, "error", err)
	}
	slog.Info("partition_follower_init",
		"topic", topic, "partition", pid,
		"leader", pa.Leader, "leader_addr", leaderAddr,
		"local_offset", localOffset, "epoch", fetchEpoch)
	go func() {
		defer close(fetchDone)
		s.followerFetcher.Run(fetchCtx, topic, pid, leaderAddr, localOffset, fetchEpoch, s.instanceID, s.partitionManager)
	}()
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
			if s.controllerState.Load() != nil {
				s.stopController()
			}
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

	// Start controller if we hold the lease and it isn't running yet.
	if s.amLeader() && s.controllerState.Load() == nil {
		s.startController(ctx)
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

	// Evict stale idempotent producers every 10th tick.
	if s.coordinationGCTick%10 == 0 {
		if evicted := s.partitionManager.EvictStaleProducers(30 * time.Minute); evicted > 0 {
			slog.Info("idempotency_evicted_stale_producers", "count", evicted)
		}
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
//
// It also checks ownership, so callers can skip a separate isOwnedPartition
// call on the produce hot path.
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

	// Collect unique instance IDs and resolve their addresses from the registry.
	addressCache := make(map[string]string)
	for _, pa := range assigned.Partitions {
		for _, instanceID := range pa.Replicas {
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
	}

	for pid, pa := range assigned.Partitions {
		instanceID := pa.Leader
		addr, ok := addressCache[instanceID]
		if !ok {
			continue
		}
		key := fmt.Sprintf("%d", pid)
		replicas := make([]routingReplicaInfo, 0, len(pa.Replicas))
		for _, replicaID := range pa.Replicas {
			replicaAddr, ok := addressCache[replicaID]
			if !ok {
				continue
			}
			replicas = append(replicas, routingReplicaInfo{
				InstanceID: replicaID,
				Address:    replicaAddr,
			})
		}
		resp.Partitions[key] = routingPartitionInfo{
			InstanceID: instanceID,
			Address:    addr,
			Replicas:   replicas,
		}
	}

	return resp
}

// leaderInternalAddr resolves the internal (h2c) address for the leader of the
// given topic/partition. Returns "" if the leader cannot be determined.
func (s *Server) leaderInternalAddr(topic string, pid int) string {
	ctx := context.Background()
	assigned, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		return ""
	}
	pa, ok := assigned.Partitions[pid]
	if !ok {
		return ""
	}
	leaderID := pa.Leader
	if leaderID == "" || leaderID == s.instanceID {
		return ""
	}
	info, err := s.registry.GetInstanceInfo(ctx, leaderID)
	if err != nil {
		return ""
	}
	addr := info.InternalAddress
	if addr == "" {
		addr = info.Address
	}
	return addr
}

// proxyToLeader forwards the request to the leader node over the h2c internal
// transport. The leader's public-facing produce handler processes the request
// and the response is streamed back to the original client.
func (s *Server) proxyToLeader(w http.ResponseWriter, r *http.Request, leaderAddr string) {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = leaderAddr
			req.Host = leaderAddr
			req.Header.Set(headerForwardedBy, s.instanceID)
		},
		ModifyResponse: func(resp *http.Response) error {
			// Propagate the leader's instance ID so clients and checkers
			// see the true leader, not the proxy node.
			if id := resp.Header.Get("X-Camu-Instance-ID"); id != "" {
				resp.Header.Set("X-Camu-Instance-ID", id)
			}
			return nil
		},
		Transport: s.internalClient.Transport,
	}
	// Clear headers set by middleware — the proxy response replaces them.
	w.Header().Del("X-Camu-Instance-ID")
	w.Header().Del("Content-Type")
	proxy.ServeHTTP(w, r)
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

	// 4a. Cancel fetch loop and wait for it to finish so any in-flight
	// append completes before local recovery proceeds.
	ps.mu.Lock()
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchCancel = nil
	ps.fetchDone = nil
	ps.mu.Unlock()
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	// 4b. Refresh index from S3 so we see segments flushed by the old leader.
	// The follower's in-memory index may be stale.
	s.partitionManager.RefreshIndex(ctx, topic, pid)
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("attemptPartitionLeadership: ensure active segment before recovery", "topic", topic, "pid", pid, "error", err)
	}

	// 4c. Recover true local log end from native segment state.
	ps.mu.RLock()
	prevEpoch := ps.epoch
	prevNextOffset := ps.nextOffset
	indexHW := ps.index.HighWatermark()
	flushedOffset := ps.flushedOffset
	ps.mu.RUnlock()
	logEnd := s.partitionManager.recoverLocalLogEnd(topic, pid)

	slog.Info("failover_recovery_state",
		"topic", topic,
		"partition", pid,
		"new_epoch", newEpoch,
		"previous_epoch", prevEpoch,
		"previous_next_offset", prevNextOffset,
		"log_end", logEnd,
		"index_hw", indexHW,
		"flushed_offset", flushedOffset,
		"isr_hw", func() uint64 {
			if isrErr != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)

	// 4c. Set HW to max of local log end, S3 index state, and persisted ISR HW.
	// The new leader was an ISR member — everything in its local tail and in S3
	// segments (visible via the refreshed index) is safe to serve.
	// logEnd alone can undercount committed data that is already in S3 segments.
	recoveredHW := logEnd
	ps.mu.RLock()
	indexNext := ps.index.NextOffset()
	currentIndexHW := ps.index.HighWatermark()
	ps.mu.RUnlock()
	if indexNext > recoveredHW {
		recoveredHW = indexNext
	}
	if isrErr == nil && isrState.HighWatermark > recoveredHW {
		recoveredHW = isrState.HighWatermark
	}
	slog.Info("failover: recovered HW",
		"topic", topic, "pid", pid,
		"hw", recoveredHW, "log_end", logEnd,
		"index_next", indexNext, "index_hw", currentIndexHW)

	// 4d. Epoch history — load from S3 (authoritative), fall back to local.
	ehPath := s.partitionManager.EpochHistoryPath(topic, pid)
	ps.mu.RLock()
	eh := ps.epochHistory
	ps.mu.RUnlock()
	if eh == nil {
		eh, _ = s.isrStore.ReadEpochHistory(ctx, topic, pid)
		if eh == nil || len(eh.Entries) == 0 {
			eh, _ = replication.LoadEpochHistory(ehPath)
			if eh == nil {
				eh = &replication.EpochHistory{}
			}
		}
	}
	eh.Append(replication.EpochEntry{Epoch: newEpoch, StartOffset: logEnd})
	ps.mu.Lock()
	ps.epochHistory = eh
	ps.isLeader = true
	ps.leaderID = ""
	ps.epoch = newEpoch
	ps.index.SetHighWatermark(recoveredHW)
	ps.mu.Unlock()
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("attemptPartitionLeadership: ensure active segment", "topic", topic, "pid", pid, "error", err)
	}
	if err := eh.SaveToFile(ehPath); err != nil {
		slog.Warn("attemptPartitionLeadership: save epoch history locally", "topic", topic, "pid", pid, "error", err)
	}
	if err := s.isrStore.WriteEpochHistory(ctx, topic, pid, eh); err != nil {
		slog.Warn("attemptPartitionLeadership: save epoch history to S3", "topic", topic, "pid", pid, "error", err)
	}

	// 4d. Initialize as leader.
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		slog.Error("attemptPartitionLeadership: get topic config", "topic", topic, "pid", pid, "error", err)
		return err
	}
	if topicCfg.ReplicationFactor > 1 {
		ps.mu.Lock()
		ps.replicaState = replication.NewReplicaState(s.instanceID, recoveredHW, topicCfg.MinInsyncReplicas, s.cfg.Coordination.ISRExpansionThresholdValue())
		ps.replicaState.SetEpochHistory(ps.epochHistory)
		for _, r := range pa.Replicas {
			if r != s.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}
		ps.mu.Unlock()
	}

	// 4e. Write ISR = [self] to S3.
	if err := s.isrStore.Write(ctx, topic, replication.ISRState{
		Partition:     pid,
		ISR:           []string{s.instanceID},
		Leader:        s.instanceID,
		LeaderEpoch:   newEpoch,
		HighWatermark: recoveredHW,
	}, ""); err != nil {
		slog.Warn("attemptPartitionLeadership: write ISR", "topic", topic, "pid", pid, "error", err)
	}

	// Recover producer idempotency state from S3 checkpoint + local committed tail.
	checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, pid)
	if data, err := s.s3Client.Get(ctx, checkpointKey); err == nil && len(data) > 0 {
		ps.mu.Lock()
		ps.loadProducerCheckpoint(data)
		ps.mu.Unlock()
		slog.Info("idempotency_checkpoint_loaded", "topic", topic, "partition", pid, "size", len(data))
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		slog.Warn("idempotency_checkpoint_load_failed", "topic", topic, "partition", pid, "error", err)
	}

	if source, n := s.partitionManager.RebuildProducerStateFromLocalTail(topic, pid); n > 0 {
		slog.Info("idempotency_local_tail_recovery", "topic", topic, "partition", pid, "source", source, "batches", n)
	}

	// If promotion recovered committed data from local native storage beyond the current
	// flushed/indexed prefix, persist it immediately so S3 and the local index
	// reflect the promoted leader's committed state.
	ps.mu.RLock()
	indexNextOffset := ps.index.NextOffset()
	ps.mu.RUnlock()
	if recoveredHW > indexNextOffset {
		if err := s.partitionManager.flushRecoveredTail(topic, pid); err != nil {
			slog.Warn("attemptPartitionLeadership: flush recovered tail",
				"topic", topic,
				"partition", pid,
				"epoch", newEpoch,
				"recovered_hw", recoveredHW,
				"index_next_offset", indexNextOffset,
				"error", err,
			)
		}
	}

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
			ps.mu.RLock()
			isLeader := ps.isLeader
			rs := ps.replicaState
			epoch := ps.epoch
			ps.mu.RUnlock()
			if isLeader && rs != nil {
				changed := rs.CheckISRLag(30 * time.Second)
				if changed || rs.ISRChanged() {
					rs.ClearISRChanged()
					isr := rs.GetISRMembers()
					if err := s.isrStore.Write(ctx, topic, replication.ISRState{
						Partition:     pid,
						ISR:           isr,
						Leader:        s.instanceID,
						LeaderEpoch:   epoch,
						HighWatermark: rs.HighWatermark(),
					}, ""); err != nil {
						slog.Warn("checkISRExpansion: write ISR", "topic", topic, "pid", pid, "error", err)
					}
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
