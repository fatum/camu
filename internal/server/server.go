package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/singleflight"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/metrics"
	"github.com/maksim/camu/internal/parquet"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

const headerForwardedBy = "X-Forwarded-By"

// Server is the HTTP server for camu.
type Server struct {
	cfg                 *config.Config
	httpServer          *http.Server
	internalServer      *http.Server
	internalListener    net.Listener
	replicationServer   *replication.ReplicationServer
	replicationListener net.Listener
	s3Client            *storage.S3Client
	topicStore          *meta.TopicStore
	partitionManager    *PartitionManager
	fetcher             *consumer.Fetcher
	registry            *coordination.Registry
	offsetStore         *storage.OffsetStore
	aclStore            *storage.ACLStore
	instanceID          string
	metrics             *metrics.Registry
	listener            net.Listener

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

	disklessEngine *diskless.Engine
	disklessMeta   diskless.MetaStore

	// assignmentsMu protects myPartitions and disklessTopics.
	assignmentsMu  sync.RWMutex
	myPartitions   map[string]map[int]localPartitionAssignment // topic -> partitionID -> local assignment view
	disklessTopics map[string]bool                             // cached: topic is diskless

	// leaseStop signals the background coordination goroutine to stop.
	leaseStop chan struct{}
	leaseWg   sync.WaitGroup

	leaseTTL             time.Duration
	leaseRenewalInterval time.Duration
	replicationTimeout   time.Duration
	fenceInterval        time.Duration

	// fenceMu guards fenceVerified, the per-partition last-verified timestamps
	// for rf=1 ack-time ownership checks.
	fenceMu        sync.Mutex
	fenceVerified  map[string]time.Time
	fenceCheckFunc func(ctx context.Context, topic string, partitionID int, epoch uint64) bool

	// shuttingDown is set to 1 during shutdown; produce handlers check this
	// and reject new writes with 503 before batcher/local state are torn down.
	shuttingDown atomic.Bool

	// ready is set after initial coordination completes (S3 synced,
	// assignments applied, partitions initialized).
	ready atomic.Bool

	// coordinationGCTick counts renewal ticks; GC runs every 10th tick.
	coordinationGCTick uint64

	sqlLimiter   chan struct{}
	sqlDBMu      sync.Mutex
	sqlDB        *sql.DB
	sqlCtx       context.Context
	sqlCtxCancel context.CancelFunc

	// parquetFetchGroup coalesces concurrent cache fetches per cache
	// path. Kept per-Server (not package-scoped) so two server instances
	// in the same process (tests) do not share the group.
	parquetFetchGroup singleflight.Group
	parquetCacheMu    sync.Mutex
	parquetCachePins  map[string]int
	// parquetStoreFactory is an internal test seam for faulting manifest
	// publication independently from immutable Parquet object uploads.
	parquetStoreFactory func() *parquet.Store

	// One local consumer runs for each partition led by this instance. Its
	// ownership is fenced by Camu's partition epoch, not a Kafka group.
	parquetConsumersMu sync.Mutex
	parquetConsumers   map[string]parquetConsumer
}

type parquetConsumer struct {
	topicConfig meta.TopicConfig
	epoch       uint64
	cancel      context.CancelFunc
	done        chan struct{}
	stopping    bool
}

// DisklessMeta returns the diskless MetaStore used by the server.
func (s *Server) DisklessMeta() diskless.MetaStore {
	return s.disklessMeta
}

func (s *Server) isTopicDiskless(_ context.Context, topic string) bool {
	s.assignmentsMu.RLock()
	defer s.assignmentsMu.RUnlock()
	return s.disklessTopics[topic]
}

func (s *Server) markTopicDiskless(topic string) {
	s.assignmentsMu.Lock()
	defer s.assignmentsMu.Unlock()
	if s.disklessTopics == nil {
		s.disklessTopics = make(map[string]bool)
	}
	s.disklessTopics[topic] = true
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
	if err := cleanupSQLFilesystem(cfg.SQL.CacheDirectoryValue(), cfg.SQL.TempDirectoryValue()); err != nil {
		return nil, fmt.Errorf("clean local SQL filesystem: %w", err)
	}

	s := &Server{
		cfg:              cfg,
		s3Client:         s3Client,
		topicStore:       meta.NewTopicStore(s3Client),
		instanceID:       instanceID,
		metrics:          metrics.NewRegistry(),
		sqlLimiter:       make(chan struct{}, cfg.SQL.MaxConcurrencyValue()),
		parquetConsumers: make(map[string]parquetConsumer),
	}
	s.sqlCtx, s.sqlCtxCancel = context.WithCancel(context.Background())
	s3Client.SetMetrics(s.metrics)
	s.httpServer = &http.Server{Handler: s.publicRoutes()}

	// Query nodes only need the read-only topic and Parquet manifest stores used
	// by SQL scope resolution. Do not construct streaming services here: doing so
	// can allocate local log state or require coordination configuration despite
	// query nodes never joining the stream cluster.
	if s.isQueryMode() {
		return s, nil
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
	fenceInterval, err := cfg.Coordination.FenceIntervalDuration()
	if err != nil {
		return nil, fmt.Errorf("parsing coordination.fence_interval: %w", err)
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
	if fenceInterval <= 0 {
		return nil, fmt.Errorf("coordination.fence_interval must be > 0")
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		return nil, fmt.Errorf("creating partition manager: %w", err)
	}

	idempotencyMgr := idempotency.NewManager(s3Client)

	s.partitionManager = pm
	s.fetcher = consumer.NewFetcher(s3Client, pm.GetDiskCache())
	s.fetcher.SetMetrics(s.metrics)
	s.offsetStore = storage.NewOffsetStore(s3Client)
	s.aclStore = storage.NewACLStore(s3Client)
	s.groupCoord = newKafkaGroupCoordinator(s3Client, instanceID)
	s.leaderElection = coordination.NewLeaderElection(s3Client, instanceID, leaseTTL)
	s.assignmentStore = coordination.NewAssignmentStore(s3Client)
	s.isrStore = replication.NewISRStore(s3Client)
	s.idempotencyManager = idempotencyMgr
	s.myPartitions = make(map[string]map[int]localPartitionAssignment)
	s.leaseStop = make(chan struct{})
	s.leaseTTL = leaseTTL
	s.leaseRenewalInterval = leaseRenewalInterval
	s.replicationTimeout = replicationTimeout
	s.fenceInterval = fenceInterval
	s.fenceVerified = make(map[string]time.Time)
	s.readAssignments = s.assignmentStore.Read
	s.groupCoord.controllerEpoch = s.currentControllerEpoch

	s.internalClient = replication.NewH2CClient(replicationTimeout)
	s.assignmentPusher = NewAssignmentPusher(s.internalClient)
	s.followerFetcher = replication.NewFollowerFetcher(s.partitionFollower().handleLeaderDown, replicationTimeout)

	// Wire ownership check into partition manager — verifies from assignment store at flush time.
	// If ownership lost, revokes the partition so future writes are rejected locally.
	pm.SetLeaseChecker(s.verifyOwnershipFromS3)

	h2s := &http2.Server{}
	s.internalServer = &http.Server{
		Handler: h2c.NewHandler(s.internalRoutes(), h2s),
	}

	return s, nil
}

func (s *Server) isQueryMode() bool {
	return s.cfg != nil && s.cfg.Server.IsQueryMode()
}

func (s *Server) isStreamMode() bool {
	return !s.isQueryMode()
}

// Start starts the HTTP server on the configured address.
func (s *Server) Start() error {
	if err := s.validateParquetExportExistingTopics(context.Background()); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", s.cfg.Server.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Server.Address, err)
	}
	return s.startWithListener(ln)
}

// StartOnPort starts the HTTP server on a specific port.
func (s *Server) StartOnPort(port int) error {
	if err := s.validateParquetExportExistingTopics(context.Background()); err != nil {
		return err
	}
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	return s.startWithListener(ln)
}

// startWithListener completes server startup once a listener is available.
func (s *Server) startWithListener(ln net.Listener) error {
	// startWithListener is used directly by tests and embedding code. Keep the
	// check here too so no stream startup path can register or serve an
	// incompatible persisted topic.
	if err := s.validateParquetExportExistingTopics(context.Background()); err != nil {
		_ = ln.Close()
		return err
	}
	s.listener = ln
	if s.isQueryMode() {
		s.ready.Store(true)
		go func() { _ = s.httpServer.Serve(ln) }()
		return nil
	}
	internalLn, err := net.Listen("tcp", s.cfg.Server.InternalAddress)
	if err != nil {
		return fmt.Errorf("listen internal on %s: %w", s.cfg.Server.InternalAddress, err)
	}
	s.internalListener = internalLn

	replicationLn, err := net.Listen("tcp", s.cfg.Server.ReplicationAddress)
	if err != nil {
		return fmt.Errorf("listen replication on %s: %w", s.cfg.Server.ReplicationAddress, err)
	}
	s.replicationListener = replicationLn
	s.replicationServer = replication.NewReplicationServer(s.handleReplicaFetchTCP, slog.Default())
	go func() { _ = s.replicationServer.Serve(replicationLn) }()
	slog.Info("replication_server_started", "address", replicationLn.Addr().String(), "protocol", "tcp")

	instanceTTL, err := s.cfg.Coordination.InstanceTTLDuration()
	if err != nil {
		return fmt.Errorf("parsing coordination.instance_ttl: %w", err)
	}
	kafkaAddr := ""
	if s.cfg.Server.KafkaPort > 0 {
		kafkaAddr = kafkaAdvertiseAddr(s.instanceID, s.Address(), s.cfg.Server.KafkaPort, s.cfg.Server.KafkaAdvertiseAddress)
	}
	s.registry = coordination.NewRegistry(s.s3Client, s.instanceID, s.Address(), s.InternalAddress(), s.ReplicationAddress(), kafkaAddr, instanceTTL)
	if err := s.registry.Register(context.Background()); err != nil {
		return fmt.Errorf("register registry: %w", err)
	}
	if err := s.initExistingTopics(); err != nil {
		return fmt.Errorf("init existing topics: %w", err)
	}
	switch s.cfg.Diskless.MetaStore {
	case "dynamodb":
		dms, err := diskless.NewDynamoMetaStore(context.Background(), diskless.DynamoMetaStoreConfig{
			TablePrefix: s.cfg.Diskless.DynamoDB.TablePrefix,
			Region:      s.cfg.Diskless.DynamoDB.Region,
			Endpoint:    s.cfg.Diskless.DynamoDB.Endpoint,
		})
		if err != nil {
			return fmt.Errorf("create dynamodb metastore: %w", err)
		}
		if err := dms.EnsureTables(context.Background()); err != nil {
			return fmt.Errorf("ensure dynamodb tables: %w", err)
		}
		s.disklessMeta = dms
	default:
		s.disklessMeta = diskless.NewMemoryMetaStore()
	}
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{
		LingerMs:      s.cfg.Diskless.LingerMs,
		MaxBatchBytes: s.cfg.Diskless.MaxBatchBytesValue(),
	})
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
	brokerAddr := kafkaAdvertiseAddr(s.instanceID, s.Address(), port, s.cfg.Server.KafkaAdvertiseAddress)
	brokerID := kafkaBrokerID(s.instanceID)

	// Create partition getter that wraps partition manager
	pg := &kafkaPartitionGetter{pm: s.partitionManager, brokerID: brokerID}

	// Create topic lister that wraps topic store
	tl := &kafkaTopicLister{ts: s.topicStore}

	s.kafkaServer = NewKafkaServer(&KafkaServerCfg{
		Metrics:                     s.metrics,
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
		WaitForReplicatedFunc:       s.waitForKafkaReplicated,
		FetchRawBatchesFunc:         s.handleKafkaFetchRawBatches,
		FetchFunc:                   s.handleKafkaFetch,
		BrokerID:                    brokerID,
		BrokerAddr:                  brokerAddr,
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("kafka_server_start", "address", addr, "error", err)
		return
	}
	go func() {
		if err := s.kafkaServer.serveListener(ln); err != nil {
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

	if s.isTopicDiskless(context.Background(), topic) {
		return nil, fmt.Errorf("%w: use raw batch path for diskless topics", errKafkaNotLeader)
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

	if s.isTopicDiskless(context.Background(), topic) {
		return nil, fmt.Errorf("%w: use raw batch path for diskless topics", errKafkaNotLeader)
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

	if s.isTopicDiskless(ctx, topic) {
		result, err := s.disklessEngine.Produce(ctx, topic, partition, batch)
		if err != nil {
			return 0, err
		}
		return result.BaseOffset, nil
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

// waitForKafkaReplicated blocks until the given offset is readable at the high
// watermark (ISR quorum confirmed) or the replication timeout expires. It is
// the Kafka produce counterpart of waitForReplicatedOffset used by the HTTP
// path, so acks=all clients get the same durability contract.
func (s *Server) waitForKafkaReplicated(ctx context.Context, topic string, partition int, offset uint64) error {
	ps := s.partitionManager.GetPartitionState(topic, partition)
	if ps == nil {
		return nil
	}
	return waitForReplicatedOffset(ctx, ps, offset, s.replicationTimeout)
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
	if s.isTopicDiskless(context.Background(), topic) {
		data, hw, err := s.disklessEngine.Fetch(context.Background(), topic, partition, int64(startOffset), int(maxBytes))
		if err != nil {
			return KafkaFetchResult{}, err
		}
		return KafkaFetchResult{
			RecordBatches:    data,
			HighWatermark:    hw,
			LastStableOffset: hw,
		}, nil
	}

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
	if s.isTopicDiskless(ctx, topic) {
		return s.disklessEngine.Fetch(ctx, topic, partition, startOffset, maxBytes)
	}
	return s.partitionManager.ReadRawBatches(ctx, topic, partition, startOffset, maxBytes)
}

func kafkaFetchHighWatermark(highWatermark uint64, ok bool, nextOffset uint64) int64 {
	if ok {
		return int64(highWatermark)
	}
	return int64(nextOffset)
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
	s.stopAllParquetConsumers()

	// 2. Shut down HTTP servers (waits for in-flight requests to finish).
	httpErr := s.httpServer.Shutdown(ctx)
	if s.internalServer != nil {
		if err := s.internalServer.Shutdown(ctx); err != nil && httpErr == nil {
			httpErr = err
		}
	}
	if s.replicationServer != nil {
		if err := s.replicationServer.Close(); err != nil && httpErr == nil {
			httpErr = err
		}
	}
	if s.kafkaServer != nil {
		if err := s.kafkaServer.Close(); err != nil && httpErr == nil {
			httpErr = err
		}
	}
	// Cancel all in-flight SQL queries before closing the DuckDB handle.
	// Without this, sql.DB.Close waits for connections to return, and a
	// long-running user query would block server shutdown indefinitely.
	if s.sqlCtxCancel != nil {
		s.sqlCtxCancel()
	}
	if s.sqlDB != nil {
		if err := s.sqlDB.Close(); err != nil && httpErr == nil {
			httpErr = err
		}
	}

	// 3. Close diskless engine (flush pending writes).
	if s.disklessEngine != nil {
		s.disklessEngine.Close()
	}

	// 4. Cancel all follower fetch loops.
	var pmErr error
	if s.partitionManager != nil {
		s.partitionManager.CancelAllFetchLoops()

		// 4. Flush batcher / close local segments.
		pmErr = s.partitionManager.Shutdown(ctx)
	}

	// 5. Stop coordination goroutine.
	if s.leaseStop != nil {
		close(s.leaseStop)
		s.leaseWg.Wait()
	}

	// 6. Deregister from cluster.
	if s.registry != nil {
		s.registry.Deregister(ctx)
	}

	if httpErr != nil {
		return httpErr
	}
	if pmErr != nil {
		return pmErr
	}
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

func (s *Server) ReplicationAddress() string {
	if s.replicationListener != nil {
		return s.replicationListener.Addr().String()
	}
	return s.cfg.Server.ReplicationAddress
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

func kafkaAdvertiseAddr(instanceID, rawAddr string, kafkaPort int, override string) string {
	if override != "" {
		if _, _, err := net.SplitHostPort(override); err == nil {
			return override
		}
		return net.JoinHostPort(override, strconv.Itoa(kafkaPort))
	}
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
	return routableAddress(instanceID, rawAddr, "8081")
}

func routableReplicationAddress(instanceID, rawAddr string) string {
	return routableAddress(instanceID, rawAddr, "8082")
}

func routableAddress(instanceID, rawAddr, defaultPort string) string {
	host, port, err := net.SplitHostPort(rawAddr)
	if err != nil {
		if rawAddr == "" {
			return net.JoinHostPort(instanceID, defaultPort)
		}
		return rawAddr
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = instanceID
	}
	if port == "" {
		port = defaultPort
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
		if tc.StorageMode == meta.StorageModeDiskless {
			s.markTopicDiskless(tc.Name)
			continue
		}
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
	s.runPartitionMaintenance(ctx, topics)
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
		if tc.StorageMode == meta.StorageModeDiskless {
			// Diskless topics skip partition manager init but still need
			// assignment-based ownership so routing works.
			s.applyDisklessAssignments(ctx, tc)
			continue
		}
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

// applyDisklessAssignments reads assignments for a diskless topic and updates
// the local ownership cache without initializing partition managers.
func (s *Server) applyDisklessAssignments(ctx context.Context, tc meta.TopicConfig) {
	assigned, err := s.readAssignments(ctx, tc.Name)
	if err != nil {
		// Fallback: assume we own all partitions (single-node case).
		s.assignmentsMu.Lock()
		if s.myPartitions[tc.Name] == nil {
			s.myPartitions[tc.Name] = make(map[int]localPartitionAssignment)
		}
		for pid := 0; pid < tc.Partitions; pid++ {
			if _, exists := s.myPartitions[tc.Name][pid]; !exists {
				s.myPartitions[tc.Name][pid] = localPartitionAssignment{
					Owned:       true,
					LeaderEpoch: 1,
				}
			}
		}
		s.assignmentsMu.Unlock()
		return
	}

	owned := make(map[int]localPartitionAssignment)
	for pid, pa := range assigned.Partitions {
		if pa.Leader == s.instanceID {
			owned[pid] = localPartitionAssignment{
				Owned:       true,
				LeaderEpoch: pa.LeaderEpoch,
			}
		}
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = owned
	s.assignmentsMu.Unlock()
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
	ps.fetchAssignmentEpoch = 0
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
	// or use existing epochHistory if S3 is unavailable.
	ehPath := s.partitionManager.EpochHistoryPath(topic, pid)
	s3eh, _ := s.isrStore.ReadEpochHistory(ctx, topic, pid)
	ps.mu.RLock()
	eh := ps.epochHistory
	ps.mu.RUnlock()
	if s3eh != nil && len(s3eh.Entries) > 0 {
		eh = s3eh
	} else if eh == nil {
		eh, _ = replication.LoadEpochHistory(ehPath)
		if eh == nil {
			eh = &replication.EpochHistory{}
		}
	}
	hasCurrentEpoch := false
	for _, entry := range eh.Entries {
		if entry.Epoch == pa.LeaderEpoch {
			hasCurrentEpoch = true
			break
		}
	}
	ehChanged := !hasCurrentEpoch
	if !hasCurrentEpoch {
		if err := eh.Ensure(replication.EpochEntry{Epoch: pa.LeaderEpoch, StartOffset: logEnd}); err != nil {
			slog.Error("initPartitionAsLeader: invalid epoch history", "topic", topic, "partition", pid, "error", err)
			return
		}
	}
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
	// The leader can produce and read beyond its local active tail: sealed
	// segments in the refreshed index are served from object storage. A
	// promoted replica whose local tail is empty (or short) must continue at
	// the index's next offset, or the recovery HW below gets capped to the
	// stale local log end and committed S3 records become unreadable.
	ps.nextOffset = logEnd
	if indexNext := ps.index.NextOffset(); indexNext > ps.nextOffset {
		ps.nextOffset = indexNext
	}
	ps.epochHistory = eh
	ps.mu.Unlock()
	if ehChanged || eh != prevEpochHistory {
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
		// The guarded update refuses to clobber a higher-epoch leader's state.
		if err := s.isrStore.Update(ctx, topic, pid, pa.LeaderEpoch, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{
				ISR:           []string{s.instanceID},
				Leader:        s.instanceID,
				HighWatermark: recoveredHW,
			}, nil
		}); err != nil {
			s.onISRWriteError(topic, pid, err)
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

	ps.mu.Lock()
	if followerFetchMatchesAssignment(ps, pa.Leader, pa.LeaderEpoch) {
		ps.mu.Unlock()
		return
	}
	localNextOffset := ps.nextOffset
	localEpoch := ps.epoch
	flushedOffset := ps.flushedOffset
	indexHW := ps.index.HighWatermark()
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchGeneration++
	generation := ps.fetchGeneration
	ps.fetchCancel = nil
	ps.fetchDone = nil
	// Demote before resolving the new leader. The later isLeader check fences a
	// concurrent promotion; leaving this true here made former leaders return
	// without ever starting their follower fetcher.
	ps.isLeader = false
	ps.replicaState = nil
	ps.mu.Unlock()

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

	// Resolve leader replication address. The registry stores the listener
	// bind address (e.g. "[::]:8082") which is useless for inter-node comms —
	// extract port and combine with the leader's instanceID (hostname).
	leaderInfo, err := s.registry.GetInstanceInfo(ctx, pa.Leader)
	if err != nil {
		slog.Warn("initPartitionAsFollower: resolve leader", "leader", pa.Leader, "error", err)
		return
	}
	leaderAddr := routableReplicationAddress(pa.Leader, leaderInfo.ReplicationAddress)

	// Cancel existing fetch loop and wait for it to finish.
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	// Start follower fetch loop. Read the offset only after the old loop has
	// stopped: it may have just applied a truncation from the new leader.
	ps.mu.Lock()
	if ps.fetchGeneration != generation || ps.isLeader {
		ps.mu.Unlock()
		return
	}
	ps.leaderID = pa.Leader
	ps.fetchAssignmentEpoch = pa.LeaderEpoch
	localOffset := ps.nextOffset
	fetchEpoch := ps.epoch
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
		defer func() {
			close(fetchDone)
			ps.mu.Lock()
			if ps.fetchGeneration == generation {
				ps.fetchCancel = nil
				ps.fetchDone = nil
				ps.fetchAssignmentEpoch = 0
			}
			ps.mu.Unlock()
		}()
		s.followerFetcher.Run(fetchCtx, topic, pid, leaderAddr, localOffset, fetchEpoch, s.instanceID, s.partitionManager)
	}()
}

// followerFetchMatchesAssignment reports whether an active fetcher already
// follows the supplied assignment. Callers must hold ps.mu.
func followerFetchMatchesAssignment(ps *partitionState, leader string, epoch uint64) bool {
	return !ps.isLeader && ps.fetchCancel != nil && ps.leaderID == leader && ps.fetchAssignmentEpoch >= epoch
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

	// Partition leaders run partition-scoped maintenance on a slow cadence.
	if s.coordinationGCTick%10 == 0 {
		s.runPartitionMaintenance(ctx, topics)
	}

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

// verifyPartitionFence re-checks rf=1 partition ownership against the
// authoritative assignment store, amortized by fenceInterval. It closes the
// window in which a fenced leader keeps acknowledging writes: the first produce
// after fenceInterval performs a fresh ownership read, and lost leadership
// immediately revokes the local partition so all future produces fail closed.
func (s *Server) verifyPartitionFence(ctx context.Context, topic string, partitionID int, epoch uint64) bool {
	if s.fenceInterval <= 0 {
		return true
	}
	fenceKey := topic + "/" + strconv.Itoa(partitionID)
	s.fenceMu.Lock()
	if last, ok := s.fenceVerified[fenceKey]; ok && time.Since(last) < s.fenceInterval {
		s.fenceMu.Unlock()
		return true
	}
	s.fenceMu.Unlock()

	assigned, err := s.readAssignments(ctx, topic)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No authoritative assignment published yet — fall back to the local
			// ownership cache (same as verifyOwnershipFromS3).
			return s.isOwnedPartition(topic, partitionID)
		}
		// Cannot confirm ownership against the authoritative store — fail closed.
		slog.Warn("verifyPartitionFence: read failed", "topic", topic, "partition", partitionID, "error", err)
		return false
	}
	pa, ok := assigned.Partitions[partitionID]
	if !ok || pa.Leader != s.instanceID || pa.LeaderEpoch != epoch {
		slog.Warn("verifyPartitionFence: lost",
			"topic", topic, "partition", partitionID,
			"owner", pa.Leader, "assignment_epoch", pa.LeaderEpoch,
			"self", s.instanceID, "local_epoch", epoch)
		s.revokePartition(topic, partitionID)
		return false
	}

	s.fenceMu.Lock()
	s.fenceVerified[fenceKey] = time.Now()
	s.fenceMu.Unlock()
	return true
}

// onISRWriteError handles an ISR state write failure. A stale-epoch rejection
// means this node is no longer the partition leader: ownership is revoked
// immediately so produce stops acknowledging and the node re-converges on the
// next assignment apply cycle.
func (s *Server) onISRWriteError(topic string, pid int, err error) {
	if errors.Is(err, replication.ErrISRStaleEpoch) {
		slog.Warn("isr_write_fenced", "topic", topic, "partition", pid, "error", err)
		s.revokePartition(topic, pid)
		return
	}
	slog.Warn("isr_write_failed", "topic", topic, "partition", pid, "error", err)
}

// verifyProduceLeadership fences stale leaders on the write path using the
// locally applied assignment epoch. This avoids an assignment-store read on
// every produce while still rejecting any producer request that raced a
// reassignment before the partition state was updated.
//
// It also checks ownership, so callers can skip a separate isOwnedPartition
// call on the produce hot path.
func (s *Server) verifyProduceLeadership(topic string, partitionID int, localEpoch uint64) bool {	s.assignmentsMu.RLock()
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
	return s.partitionFollower().leaderInternalAddr(topic, pid)
}

// proxyToLeader forwards the request to the leader node over the h2c internal
// transport. The leader's public-facing produce handler processes the request
// and the response is streamed back to the original client.
func (s *Server) proxyToLeader(w http.ResponseWriter, r *http.Request, leaderAddr string) {
	s.partitionFollower().proxyToLeader(w, r, leaderAddr)
}

// attemptPartitionLeadership is called when a follower detects the leader is
// down. It tries to become the new leader via a CAS write to the assignment
// store and, on success, transitions the local partition state from follower
// to leader.
func (s *Server) attemptPartitionLeadership(topic string, pid int) error {
	return s.partitionFollower().attemptPartitionLeadership(topic, pid)
}

// checkISRLag iterates over all leader partitions and removes followers from
// the ISR set if they have not contacted the leader within the lag timeout.
// When the ISR changes, the updated set is written to S3.
func (s *Server) checkISRLag(ctx context.Context) {
	s.partitionManager.mu.RLock()
	defer s.partitionManager.mu.RUnlock()
	for topic, parts := range s.partitionManager.partitions {
		for pid, ps := range parts {
			ps.mu.Lock()
			isLeader := ps.isLeader
			rs := ps.replicaState
			epoch := ps.epoch
			if !isLeader || rs == nil {
				ps.mu.Unlock()
				continue
			}
			// CheckISRLag mutates the ISR set, so it must run under the same
			// partition lock as the replica fetch path (UpdateFollower).
			changed := rs.CheckISRLag(30 * time.Second) || rs.ISRChanged()
			if !changed {
				ps.mu.Unlock()
				continue
			}
			rs.ClearISRChanged()
			isr := rs.GetISRMembers()
			hw := rs.HighWatermark()
			ps.mu.Unlock()

			if err := s.isrStore.Update(ctx, topic, pid, epoch, func(_ replication.ISRState) (replication.ISRState, error) {
				return replication.ISRState{
					ISR:           isr,
					Leader:        s.instanceID,
					HighWatermark: hw,
				}, nil
			}); err != nil {
				s.onISRWriteError(topic, pid, err)
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
