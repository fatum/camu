package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/storage"
)

func TestKafkaGroupCoordinatorCycle(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)
	gc := newKafkaGroupCoordinator(s3Client, "n1")

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-1"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	metadata := kmsg.NewConsumerMemberMetadata()
	metadata.Version = 0
	metadata.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: metadata.AppendTo(nil),
	}}

	joinResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), joinResp.ErrorCode)
	require.NotEmpty(t, joinResp.MemberID)
	require.Equal(t, joinResp.MemberID, joinResp.LeaderID)
	require.Len(t, joinResp.Members, 1)

	assignment := kmsg.NewConsumerMemberAssignment()
	assignment.Version = 0
	assignment.Topics = []kmsg.ConsumerMemberAssignmentTopic{{
		Topic:      "test-topic",
		Partitions: []int32{0},
	}}
	assignmentBytes := assignment.AppendTo(nil)
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = "group-1"
	syncReq.Generation = joinResp.Generation
	syncReq.MemberID = joinResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{
		MemberID:         joinResp.MemberID,
		MemberAssignment: assignmentBytes,
	}}

	syncResp, err := gc.syncGroup(context.Background(), syncReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), syncResp.ErrorCode)
	require.Equal(t, assignmentBytes, syncResp.MemberAssignment)

	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = "group-1"
	hbReq.Generation = joinResp.Generation
	hbReq.MemberID = joinResp.MemberID
	hbResp, err := gc.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = "group-1"
	leaveReq.MemberID = joinResp.MemberID
	leaveResp, err := gc.leaveGroup(context.Background(), leaveReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), leaveResp.ErrorCode)
}

func TestKafkaGroupCoordinatorPersistsState(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc1 := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-persist"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinResp, err := gc1.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	gc2 := newKafkaGroupCoordinator(s3Client, "n1")
	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = "group-persist"
	hbReq.Generation = joinResp.Generation
	hbReq.MemberID = joinResp.MemberID

	hbResp, err := gc2.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), hbResp.ErrorCode)
}

func TestKafkaGroupCoordinatorSecondInstanceCanContinuePersistedState(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc1 := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-lease"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinResp, err := gc1.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), joinResp.ErrorCode)

	gc2 := newKafkaGroupCoordinator(s3Client, "n2")
	secondResp, err := gc2.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), secondResp.ErrorCode)
	require.Equal(t, int32(2), secondResp.Generation)
	require.NotEqual(t, joinResp.MemberID, secondResp.MemberID)
}

func TestKafkaGroupCoordinatorHeartbeatUsesCachedState(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-cache"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), joinResp.ErrorCode)

	_, etagBefore, err := s3Client.GetWithETag(context.Background(), kafkaGroupKey("group-cache"))
	require.NoError(t, err)

	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = "group-cache"
	hbReq.Generation = joinResp.Generation
	hbReq.MemberID = joinResp.MemberID

	hbResp, err := gc.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	_, etagAfterFirstHeartbeat, err := s3Client.GetWithETag(context.Background(), kafkaGroupKey("group-cache"))
	require.NoError(t, err)
	require.Equal(t, etagBefore, etagAfterFirstHeartbeat)

	gc.mu.Lock()
	gc.cache["group-cache"].lastHeartbeatPersist = time.Now().Add(-2 * kafkaGroupHeartbeatPersistInterval)
	gc.mu.Unlock()

	hbResp, err = gc.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	_, etagAfterSecondHeartbeat, err := s3Client.GetWithETag(context.Background(), kafkaGroupKey("group-cache"))
	require.NoError(t, err)
	require.NotEqual(t, etagBefore, etagAfterSecondHeartbeat)
}

func TestKafkaGroupCoordinatorJoinBumpsGenerationOnMembershipChange(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	base := time.Unix(100, 0)
	gc.now = func() time.Time { return base }

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-generation"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	firstResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int32(1), firstResp.Generation)

	base = base.Add(time.Second)
	secondResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), secondResp.Generation)
	require.NotEqual(t, firstResp.MemberID, secondResp.MemberID)
}

func TestSelectJoinProtocol_RejectsWhenCurrentProtocolMissing(t *testing.T) {
	selected, ok := selectJoinProtocol("range", []kmsg.JoinGroupRequestProtocol{
		{Name: "roundrobin"},
	})
	require.False(t, ok)
	require.Equal(t, kmsg.JoinGroupRequestProtocol{}, selected)
}

func TestKafkaGroupCoordinatorHeartbeatExpiresStaleMembers(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	base := time.Unix(100, 0)
	gc.now = func() time.Time { return base }

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-expiry"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 1000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	firstResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	base = base.Add(100 * time.Millisecond)
	secondResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), secondResp.Generation)

	base = base.Add(400 * time.Millisecond)
	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = "group-expiry"
	hbReq.Generation = secondResp.Generation
	hbReq.MemberID = firstResp.MemberID
	hbResp, err := gc.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	base = base.Add(700 * time.Millisecond)
	hbResp, err = gc.heartbeat(context.Background(), hbReq)
	require.NoError(t, err)
	require.Equal(t, kafkaErrorIllegalGeneration, hbResp.ErrorCode)

	group, err := gc.readGroup(context.Background(), "group-expiry")
	require.NoError(t, err)
	require.Equal(t, int32(3), group.Generation)
	require.Len(t, group.Members, 1)
	_, ok := group.Members[firstResp.MemberID]
	require.True(t, ok)
	_, ok = group.Members[secondResp.MemberID]
	require.False(t, ok)
	require.Equal(t, firstResp.MemberID, group.LeaderID)
}
