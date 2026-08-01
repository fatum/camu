package server

import (
	"context"
	"errors"
	"hash/crc32"
	"log/slog"
	"net"
	"sync"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/metrics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const maxKafkaRequestSize = 16 << 20

var kafkaCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

var (
	errKafkaUnknownTopicPartition = errors.New("kafka unknown topic or partition")
	errKafkaNotLeader             = errors.New("kafka not leader")
	errKafkaLeaderNotAvailable    = errors.New("kafka leader not available")
	errKafkaInvalidRequest        = errors.New("kafka invalid request")
)

const (
	kafkaErrorUnknownServer         int16 = 1
	kafkaErrorUnknownTopicPartition int16 = 3
	kafkaErrorLeaderNotAvailable    int16 = 5
	kafkaErrorNotLeader             int16 = 6
	kafkaErrorTopicAlreadyExists    int16 = 36
	kafkaErrorInvalidPartitions     int16 = 37
	kafkaErrorInvalidReplication    int16 = 38
	kafkaErrorInvalidReplicaAssign  int16 = 39
	kafkaErrorInvalidConfig         int16 = 40
	kafkaErrorNotController         int16 = 41
	kafkaErrorInvalidRequest        int16 = 42
	kafkaErrorOutOfOrderSequence    int16 = 45
	kafkaErrorUnknownProducerID     int16 = 59
)

type KafkaServer struct {
	cfg        *KafkaServerCfg
	log        *slog.Logger
	listenerMu sync.Mutex
	listener   net.Listener
	connsMu    sync.Mutex
	conns      map[net.Conn]struct{}
}

type KafkaServerCfg struct {
	Metrics                     *metrics.Registry
	PartitionGetter             PartitionGetter
	TopicLister                 TopicLister
	MetadataFunc                func(ctx context.Context, req *kmsg.MetadataRequest) (*kmsg.MetadataResponse, error)
	CreateTopicsFunc            func(ctx context.Context, req *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error)
	DeleteTopicsFunc            func(ctx context.Context, req *kmsg.DeleteTopicsRequest) (*kmsg.DeleteTopicsResponse, error)
	CreatePartitionsFunc        func(ctx context.Context, req *kmsg.CreatePartitionsRequest) (*kmsg.CreatePartitionsResponse, error)
	DescribeConfigsFunc         func(ctx context.Context, req *kmsg.DescribeConfigsRequest) (*kmsg.DescribeConfigsResponse, error)
	AlterConfigsFunc            func(ctx context.Context, req *kmsg.AlterConfigsRequest) (*kmsg.AlterConfigsResponse, error)
	IncrementalAlterConfigsFunc func(ctx context.Context, req *kmsg.IncrementalAlterConfigsRequest) (*kmsg.IncrementalAlterConfigsResponse, error)
	DescribeClusterFunc         func(ctx context.Context, req *kmsg.DescribeClusterRequest) (*kmsg.DescribeClusterResponse, error)
	CreateACLsFunc              func(ctx context.Context, req *kmsg.CreateACLsRequest) (*kmsg.CreateACLsResponse, error)
	DescribeACLsFunc            func(ctx context.Context, req *kmsg.DescribeACLsRequest) (*kmsg.DescribeACLsResponse, error)
	DeleteACLsFunc              func(ctx context.Context, req *kmsg.DeleteACLsRequest) (*kmsg.DeleteACLsResponse, error)
	FindCoordinatorFunc         func(ctx context.Context, req *kmsg.FindCoordinatorRequest) (*kmsg.FindCoordinatorResponse, error)
	InitProducerIDFunc          func(ctx context.Context, req *kmsg.InitProducerIDRequest) (*kmsg.InitProducerIDResponse, error)
	DescribeGroupsFunc          func(ctx context.Context, req *kmsg.DescribeGroupsRequest) (*kmsg.DescribeGroupsResponse, error)
	ListGroupsFunc              func(ctx context.Context, req *kmsg.ListGroupsRequest) (*kmsg.ListGroupsResponse, error)
	DeleteGroupsFunc            func(ctx context.Context, req *kmsg.DeleteGroupsRequest) (*kmsg.DeleteGroupsResponse, error)
	OffsetDeleteFunc            func(ctx context.Context, req *kmsg.OffsetDeleteRequest) (*kmsg.OffsetDeleteResponse, error)
	JoinGroupFunc               func(ctx context.Context, req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, error)
	SyncGroupFunc               func(ctx context.Context, req *kmsg.SyncGroupRequest) (*kmsg.SyncGroupResponse, error)
	HeartbeatFunc               func(ctx context.Context, req *kmsg.HeartbeatRequest) (*kmsg.HeartbeatResponse, error)
	LeaveGroupFunc              func(ctx context.Context, req *kmsg.LeaveGroupRequest) (*kmsg.LeaveGroupResponse, error)
	ListOffsetsFunc             func(ctx context.Context, topic string, partition int, timestamp int64) (KafkaOffsetResponse, error)
	OffsetCommitFunc            func(ctx context.Context, req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error)
	OffsetFetchFunc             func(ctx context.Context, req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error)
	PartitionErrorFunc          func(ctx context.Context, topic string, partition int) int16
	AppendRawBatchFunc          func(ctx context.Context, topic string, partition int, batch []byte) (int64, error)
	AppendBatchFunc             func(topic string, partition int, batch log.Batch) ([]uint64, error)
	AppendFunc                  func(topic string, partition int, msgs []log.Message) ([]uint64, error)
	FetchRawBatchesFunc         func(ctx context.Context, topic string, partition int, startOffset int64, maxBytes int) ([]byte, int64, error)
	FetchFunc                   func(topic string, partition int, startOffset uint64, maxBytes int32) (KafkaFetchResult, error)
	RequestHandler              func(req kmsg.Request) (kmsg.Response, error)
	BrokerID                    int32
	BrokerAddr                  string
}

type KafkaOffsetResponse struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch int32
}

type KafkaFetchResult struct {
	RecordBatches    []byte
	HighWatermark    int64
	LastStableOffset int64
}

type PartitionGetter interface {
	GetPartitionInfo(topic string, partition int) (*PartitionInfo, bool)
}

type TopicLister interface {
	ListTopics() ([]*TopicConfig, error)
}

type TopicConfig struct {
	Name       string
	Partitions int
}

type PartitionInfo struct {
	Leader   int32
	Replicas []int32
	ISR      []int32
}

func NewKafkaServer(cfg *KafkaServerCfg) *KafkaServer {
	return &KafkaServer{cfg: cfg, log: slog.Default()}
}
