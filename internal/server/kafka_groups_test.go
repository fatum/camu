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

func TestKafkaGroupCoordinatorSyncGroupRejectsFollowerAssignments(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := newTestKafkaJoinGroupRequest("group-sync-follower")

	leaderResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	secondResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	leaderAssignment := newTestKafkaMemberAssignment(0)
	followerAssignment := newTestKafkaMemberAssignment(1)

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = "group-sync-follower"
	syncReq.Generation = secondResp.Generation
	syncReq.MemberID = secondResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leaderResp.MemberID, MemberAssignment: leaderAssignment},
		{MemberID: secondResp.MemberID, MemberAssignment: followerAssignment},
	}

	syncResp, err := gc.syncGroup(context.Background(), syncReq)
	require.NoError(t, err)
	require.Equal(t, kafkaErrorRebalanceInProgress, syncResp.ErrorCode)

	group, err := gc.readGroup(context.Background(), "group-sync-follower")
	require.NoError(t, err)
	require.Empty(t, group.Assignments)
}

func TestKafkaGroupCoordinatorSyncGroupRejectsIncompleteAssignments(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := newTestKafkaJoinGroupRequest("group-sync-incomplete")

	leaderResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	secondResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = "group-sync-incomplete"
	syncReq.Generation = secondResp.Generation
	syncReq.MemberID = leaderResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leaderResp.MemberID, MemberAssignment: newTestKafkaMemberAssignment(0)},
	}

	syncResp, err := gc.syncGroup(context.Background(), syncReq)
	require.NoError(t, err)
	require.Equal(t, kafkaErrorRebalanceInProgress, syncResp.ErrorCode)

	group, err := gc.readGroup(context.Background(), "group-sync-incomplete")
	require.NoError(t, err)
	require.Empty(t, group.Assignments)
}

func TestKafkaGroupCoordinatorLeaveGroupRebalancesOnMemberRemoval(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := newTestKafkaJoinGroupRequest("group-leave-rebalance")

	leaderResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int32(1), leaderResp.Generation)

	secondResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), secondResp.Generation)

	assignment := newTestKafkaMemberAssignment(0)
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = "group-leave-rebalance"
	syncReq.Generation = secondResp.Generation
	syncReq.MemberID = leaderResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leaderResp.MemberID, MemberAssignment: assignment},
		{MemberID: secondResp.MemberID, MemberAssignment: assignment},
	}

	syncResp, err := gc.syncGroup(context.Background(), syncReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = "group-leave-rebalance"
	leaveReq.MemberID = secondResp.MemberID

	leaveResp, err := gc.leaveGroup(context.Background(), leaveReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), leaveResp.ErrorCode)

	group, err := gc.readGroup(context.Background(), "group-leave-rebalance")
	require.NoError(t, err)
	require.Equal(t, int32(3), group.Generation)
	require.Len(t, group.Members, 1)
	require.Empty(t, group.Assignments)
	require.Equal(t, leaderResp.MemberID, group.LeaderID)
}

func TestKafkaGroupCoordinatorJoinRejectsUnknownMemberID(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	require.NoError(t, err)

	gc := newKafkaGroupCoordinator(s3Client, "n1")
	joinReq := newTestKafkaJoinGroupRequest("group-unknown-member")

	firstResp, err := gc.joinGroup(context.Background(), joinReq)
	require.NoError(t, err)
	require.Equal(t, int16(0), firstResp.ErrorCode)

	rejoinReq := newTestKafkaJoinGroupRequest("group-unknown-member")
	rejoinReq.MemberID = "stale-member-id"

	rejoinResp, err := gc.joinGroup(context.Background(), rejoinReq)
	require.NoError(t, err)
	require.Equal(t, kafkaErrorUnknownMemberID, rejoinResp.ErrorCode)
	require.Equal(t, int32(-1), rejoinResp.Generation)

	group, err := gc.readGroup(context.Background(), "group-unknown-member")
	require.NoError(t, err)
	require.Len(t, group.Members, 1)
	_, ok := group.Members["stale-member-id"]
	require.False(t, ok)
}

func newTestKafkaJoinGroupRequest(group string) *kmsg.JoinGroupRequest {
	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = group
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"test-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	return joinReq
}

func newTestKafkaMemberAssignment(partition int32) []byte {
	assignment := kmsg.NewConsumerMemberAssignment()
	assignment.Version = 0
	assignment.Topics = []kmsg.ConsumerMemberAssignmentTopic{{
		Topic:      "test-topic",
		Partitions: []int32{partition},
	}}
	return assignment.AppendTo(nil)
}
