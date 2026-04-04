//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

func TestKafkaOffsetCommitFetchWithFranzGoRequests(t *testing.T) {
	env, httpClient, client := newKafkaTestEnv(t, "kafka-offsets")
	defer env.Cleanup()
	defer client.Close()

	if err := httpClient.CreateTopic("kafka-offsets-b", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = "group-1"
	findRespAny, err := client.Request(ctx, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator Request() error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	if findResp.NodeID == 0 {
		t.Fatalf("FindCoordinator node id = %d, want non-zero", findResp.NodeID)
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = "group-1"
	commitReq.Topics = []kmsg.OffsetCommitRequestTopic{
		{
			Topic: "kafka-offsets",
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    7,
			}},
		},
		{
			Topic: "kafka-offsets-b",
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    11,
			}},
		},
	}
	commitRespAny, err := client.Request(ctx, commitReq)
	if err != nil {
		t.Fatalf("OffsetCommit Request() error: %v", err)
	}
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	if len(commitResp.Topics) != 2 {
		t.Fatalf("OffsetCommit topics = %d, want 2", len(commitResp.Topics))
	}
	for _, topic := range commitResp.Topics {
		if len(topic.Partitions) != 1 || topic.Partitions[0].ErrorCode != 0 {
			t.Fatalf("OffsetCommit response for %s = %+v", topic.Topic, topic.Partitions)
		}
	}

	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = "group-1"
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{
		{Topic: "kafka-offsets", Partitions: []int32{0}},
		{Topic: "kafka-offsets-b", Partitions: []int32{0}},
	}
	fetchRespAny, err := client.Request(ctx, fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch Request() error: %v", err)
	}
	fetchResp := fetchRespAny.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Topics) != 2 {
		t.Fatalf("OffsetFetch topics = %d, want 2", len(fetchResp.Topics))
	}

	got := map[string]int64{}
	for _, topic := range fetchResp.Topics {
		if len(topic.Partitions) != 1 {
			t.Fatalf("OffsetFetch partitions for %s = %d, want 1", topic.Topic, len(topic.Partitions))
		}
		if topic.Partitions[0].ErrorCode != 0 {
			t.Fatalf("OffsetFetch error for %s = %d", topic.Topic, topic.Partitions[0].ErrorCode)
		}
		got[topic.Topic] = topic.Partitions[0].Offset
	}
	if got["kafka-offsets"] != 7 || got["kafka-offsets-b"] != 11 {
		t.Fatalf("OffsetFetch offsets = %v, want kafka-offsets=7 kafka-offsets-b=11", got)
	}
}

func TestKafkaFindCoordinatorRoutesToLeaseOwner(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	groupID := "group-coordinator-lease"
	joinAddr := fmt.Sprintf("127.0.0.1:%d", port1)
	queryAddr := fmt.Sprintf("127.0.0.1:%d", port2)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"coordinator-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinRespAny, err := sendKafkaRequest(joinAddr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup via broker1 error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(queryAddr, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator via broker2 error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	wantNodeID := kafkaBrokerIDForTest("127.0.0.1")
	if findResp.NodeID != wantNodeID {
		t.Fatalf("FindCoordinator node id = %d, want %d", findResp.NodeID, wantNodeID)
	}
	if findResp.Host != "127.0.0.1" || findResp.Port != int32(port1) {
		t.Fatalf("FindCoordinator address = %s:%d, want 127.0.0.1:%d", findResp.Host, findResp.Port, port1)
	}
}

func TestKafkaJoinGroupOnNonCoordinatorReturnsNotCoordinator(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-non-coordinator"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)

	joinAddr := addr1
	wantNodeID := kafkaBrokerIDForTest("127.0.0.1")
	if findResp.NodeID == wantNodeID {
		joinAddr = addr2
	}

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"coordinator-topic"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinRespAny, err := sendKafkaRequest(joinAddr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup via non-coordinator error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != kafkaErrorNotCoordinatorForTest {
		t.Fatalf("JoinGroup error code = %d, want %d (NOT_COORDINATOR)", joinResp.ErrorCode, kafkaErrorNotCoordinatorForTest)
	}
}

func TestKafkaGroupOffsetsOnNonCoordinatorReturnNotCoordinator(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-offsets-non-coordinator", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-offsets-non-coordinator"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)

	nonCoordinatorAddr := addr1
	if findResp.Host == "127.0.0.1" && findResp.Port == int32(port1) {
		nonCoordinatorAddr = addr2
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = groupID
	commitReq.Topics = []kmsg.OffsetCommitRequestTopic{{
		Topic: "group-offsets-non-coordinator",
		Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
			Partition: 0,
			Offset:    5,
		}},
	}}
	commitRespAny, err := sendKafkaRequest(nonCoordinatorAddr, commitReq)
	if err != nil {
		t.Fatalf("OffsetCommit via non-coordinator error: %v", err)
	}
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	if len(commitResp.Topics) != 1 || len(commitResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected OffsetCommit response: %+v", commitResp.Topics)
	}
	if got := commitResp.Topics[0].Partitions[0].ErrorCode; got != kafkaErrorNotCoordinatorForTest {
		t.Fatalf("OffsetCommit error code = %d, want %d (NOT_COORDINATOR)", got, kafkaErrorNotCoordinatorForTest)
	}

	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = groupID
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "group-offsets-non-coordinator",
		Partitions: []int32{0},
	}}
	fetchRespAny, err := sendKafkaRequest(nonCoordinatorAddr, fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch via non-coordinator error: %v", err)
	}
	fetchResp := fetchRespAny.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected OffsetFetch response: %+v", fetchResp.Topics)
	}
	if got := fetchResp.Topics[0].Partitions[0].ErrorCode; got != kafkaErrorNotCoordinatorForTest {
		t.Fatalf("OffsetFetch error code = %d, want %d (NOT_COORDINATOR)", got, kafkaErrorNotCoordinatorForTest)
	}
}

func TestKafkaListAndDescribeGroups(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("kafka-group-introspection", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-introspection"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"kafka-group-introspection"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	joinRespAny, err := sendKafkaRequest(addr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup Request() error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	listReq := kmsg.NewPtrListGroupsRequest()
	listReq.SetVersion(4)
	listRespAny, err := sendKafkaRequest(addr, listReq)
	if err != nil {
		t.Fatalf("ListGroups Request() error: %v", err)
	}
	listResp := listRespAny.(*kmsg.ListGroupsResponse)
	found := false
	for _, group := range listResp.Groups {
		if group.Group == "group-introspection" {
			found = true
			if group.ProtocolType != "consumer" {
				t.Fatalf("ListGroups protocol type = %q, want consumer", group.ProtocolType)
			}
			if group.GroupState == "" {
				t.Fatal("ListGroups group state was empty")
			}
		}
	}
	if !found {
		t.Fatal("ListGroups did not include group-introspection")
	}

	describeReq := kmsg.NewPtrDescribeGroupsRequest()
	describeReq.SetVersion(4)
	describeReq.Groups = []string{"group-introspection"}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeGroupsResponse)
	if len(describeResp.Groups) != 1 {
		t.Fatalf("DescribeGroups groups = %d, want 1", len(describeResp.Groups))
	}
	group := describeResp.Groups[0]
	if group.Group != "group-introspection" {
		t.Fatalf("DescribeGroups group = %q, want group-introspection", group.Group)
	}
	if group.ProtocolType != "consumer" {
		t.Fatalf("DescribeGroups protocol type = %q, want consumer", group.ProtocolType)
	}
	if len(group.Members) != 1 {
		t.Fatalf("DescribeGroups members = %d, want 1", len(group.Members))
	}
	if group.Members[0].MemberID != joinResp.MemberID {
		t.Fatalf("DescribeGroups member id = %q, want %q", group.Members[0].MemberID, joinResp.MemberID)
	}
}

func TestKafkaDeleteGroups(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("kafka-delete-groups", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-delete"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"kafka-delete-groups"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	joinRespAny, err := sendKafkaRequest(addr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup Request() error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	deleteReq := kmsg.NewPtrDeleteGroupsRequest()
	deleteReq.Groups = []string{"group-delete"}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteGroups while non-empty error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteGroupsResponse)
	if len(deleteResp.Groups) != 1 || deleteResp.Groups[0].ErrorCode != kafkaErrorNonEmptyGroupForTest {
		t.Fatalf("DeleteGroups non-empty response = %+v, want NON_EMPTY_GROUP", deleteResp.Groups)
	}

	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = "group-delete"
	leaveReq.MemberID = joinResp.MemberID
	leaveRespAny, err := sendKafkaRequest(addr, leaveReq)
	if err != nil {
		t.Fatalf("LeaveGroup Request() error: %v", err)
	}
	leaveResp := leaveRespAny.(*kmsg.LeaveGroupResponse)
	if leaveResp.ErrorCode != 0 {
		t.Fatalf("LeaveGroup error code = %d, want 0", leaveResp.ErrorCode)
	}

	deleteRespAny, err = sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteGroups after leave error: %v", err)
	}
	deleteResp = deleteRespAny.(*kmsg.DeleteGroupsResponse)
	if len(deleteResp.Groups) != 1 || deleteResp.Groups[0].ErrorCode != 0 {
		t.Fatalf("DeleteGroups empty response = %+v, want success", deleteResp.Groups)
	}

	listReq := kmsg.NewPtrListGroupsRequest()
	listReq.SetVersion(4)
	listRespAny, err := sendKafkaRequest(addr, listReq)
	if err != nil {
		t.Fatalf("ListGroups Request() error: %v", err)
	}
	listResp := listRespAny.(*kmsg.ListGroupsResponse)
	for _, group := range listResp.Groups {
		if group.Group == "group-delete" {
			t.Fatal("deleted group still present in ListGroups")
		}
	}
}

func TestKafkaDeleteGroupsOnNonCoordinatorReturnsNotCoordinator(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-delete-non-coordinator"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)

	deleteAddr := addr1
	if findResp.Host == "127.0.0.1" && findResp.Port == int32(port1) {
		deleteAddr = addr2
	}

	deleteReq := kmsg.NewPtrDeleteGroupsRequest()
	deleteReq.Groups = []string{groupID}
	deleteRespAny, err := sendKafkaRequest(deleteAddr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteGroups via non-coordinator error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteGroupsResponse)
	if len(deleteResp.Groups) != 1 || deleteResp.Groups[0].ErrorCode != kafkaErrorNotCoordinatorForTest {
		t.Fatalf("DeleteGroups non-coordinator response = %+v, want NOT_COORDINATOR", deleteResp.Groups)
	}
}

func TestKafkaDeleteGroupsMissingGroupReturnsNotFound(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	deleteReq := kmsg.NewPtrDeleteGroupsRequest()
	deleteReq.Groups = []string{"group-missing"}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteGroups missing group error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteGroupsResponse)
	if len(deleteResp.Groups) != 1 || deleteResp.Groups[0].ErrorCode != kafkaErrorGroupIDNotFoundForTest {
		t.Fatalf("DeleteGroups missing group response = %+v, want GROUP_ID_NOT_FOUND", deleteResp.Groups)
	}
}

func TestKafkaOffsetDeleteRemovesCommittedOffsets(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	if err := httpClient.CreateTopic("offset-delete-a", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	if err := httpClient.CreateTopic("offset-delete-b", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-offset-delete"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"offset-delete-a"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	joinRespAny, err := sendKafkaRequest(addr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup Request() error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = "group-offset-delete"
	commitReq.Topics = []kmsg.OffsetCommitRequestTopic{
		{
			Topic: "offset-delete-a",
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    5,
			}},
		},
		{
			Topic: "offset-delete-b",
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    8,
			}},
		},
	}
	commitRespAny, err := sendKafkaRequest(addr, commitReq)
	if err != nil {
		t.Fatalf("OffsetCommit Request() error: %v", err)
	}
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	if len(commitResp.Topics) != 2 {
		t.Fatalf("OffsetCommit topics = %d, want 2", len(commitResp.Topics))
	}

	deleteReq := kmsg.NewPtrOffsetDeleteRequest()
	deleteReq.Group = "group-offset-delete"
	deleteReq.Topics = []kmsg.OffsetDeleteRequestTopic{{
		Topic: "offset-delete-a",
		Partitions: []kmsg.OffsetDeleteRequestTopicPartition{{
			Partition: 0,
		}},
	}}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("OffsetDelete Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.OffsetDeleteResponse)
	if deleteResp.ErrorCode != 0 {
		t.Fatalf("OffsetDelete top-level error = %d, want 0", deleteResp.ErrorCode)
	}
	if len(deleteResp.Topics) != 1 || len(deleteResp.Topics[0].Partitions) != 1 || deleteResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetDelete response = %+v, want success", deleteResp.Topics)
	}

	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = "group-offset-delete"
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{
		{Topic: "offset-delete-a", Partitions: []int32{0}},
		{Topic: "offset-delete-b", Partitions: []int32{0}},
	}
	fetchRespAny, err := sendKafkaRequest(addr, fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch Request() error: %v", err)
	}
	fetchResp := fetchRespAny.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Topics) != 2 {
		t.Fatalf("OffsetFetch topics = %d, want 2", len(fetchResp.Topics))
	}

	got := map[string]int64{}
	for _, topic := range fetchResp.Topics {
		if len(topic.Partitions) != 1 {
			t.Fatalf("OffsetFetch partitions for %s = %d, want 1", topic.Topic, len(topic.Partitions))
		}
		got[topic.Topic] = topic.Partitions[0].Offset
	}
	if got["offset-delete-a"] != -1 {
		t.Fatalf("offset-delete-a offset = %d, want -1 after delete", got["offset-delete-a"])
	}
	if got["offset-delete-b"] != 8 {
		t.Fatalf("offset-delete-b offset = %d, want 8", got["offset-delete-b"])
	}
}

func TestKafkaControllerEpochStampedInGroupStateAndOffsets(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-epoch-stamp", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-epoch-stamp"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"group-epoch-stamp"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	joinRespAny, err := sendKafkaRequest(addr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup Request() error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = "group-epoch-stamp"
	commitReq.Topics = []kmsg.OffsetCommitRequestTopic{{
		Topic: "group-epoch-stamp",
		Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
			Partition: 0,
			Offset:    9,
		}},
	}}
	commitRespAny, err := sendKafkaRequest(addr, commitReq)
	if err != nil {
		t.Fatalf("OffsetCommit Request() error: %v", err)
	}
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	if len(commitResp.Topics) != 1 || len(commitResp.Topics[0].Partitions) != 1 || commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetCommit response = %+v, want success", commitResp.Topics)
	}

	groupRaw, err := env.S3Client().Get(context.Background(), "_coordination/kafka-groups/group-epoch-stamp.json")
	if err != nil {
		t.Fatalf("S3 Get group state error: %v", err)
	}
	var groupState struct {
		ControllerEpoch string `json:"controller_epoch"`
	}
	if err := json.Unmarshal(groupRaw, &groupState); err != nil {
		t.Fatalf("unmarshal group state: %v", err)
	}
	if groupState.ControllerEpoch == "" {
		t.Fatal("group state controller_epoch was empty")
	}

	offsetRaw, err := env.S3Client().Get(context.Background(), "_coordination/groups/group-epoch-stamp/offsets.json")
	if err != nil {
		t.Fatalf("S3 Get group offsets error: %v", err)
	}
	var offsetsState struct {
		ControllerEpoch string `json:"controller_epoch"`
	}
	if err := json.Unmarshal(offsetRaw, &offsetsState); err != nil {
		t.Fatalf("unmarshal offsets state: %v", err)
	}
	if offsetsState.ControllerEpoch == "" {
		t.Fatal("offsets state controller_epoch was empty")
	}
	if offsetsState.ControllerEpoch != groupState.ControllerEpoch {
		t.Fatalf("controller epochs differ: group=%q offsets=%q", groupState.ControllerEpoch, offsetsState.ControllerEpoch)
	}
}

func TestKafkaGroupCoordinatorFailoverPreservesOffsets(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-failover-offsets", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-failover-offsets"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	coordinatorAddr := net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port))
	coordinatorIdx := 0
	survivorAddr := addr2
	if coordinatorAddr == addr2 {
		coordinatorIdx = 1
		survivorAddr = addr1
	}

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"group-failover-offsets"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}
	joinRespAny, err := sendKafkaRequest(coordinatorAddr, joinReq)
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = groupID
	commitReq.Topics = []kmsg.OffsetCommitRequestTopic{{
		Topic: "group-failover-offsets",
		Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
			Partition: 0,
			Offset:    33,
		}},
	}}
	commitRespAny, err := sendKafkaRequest(coordinatorAddr, commitReq)
	if err != nil {
		t.Fatalf("OffsetCommit error: %v", err)
	}
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	if len(commitResp.Topics) != 1 || len(commitResp.Topics[0].Partitions) != 1 || commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetCommit response = %+v, want success", commitResp.Topics)
	}

	env.StopInstance(coordinatorIdx)
	if err := env.WaitForInstance(1-coordinatorIdx, 5*time.Second); err != nil {
		t.Fatalf("survivor instance not ready: %v", err)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		findRespAny, err = sendKafkaRequest(survivorAddr, findReq)
		if err == nil {
			findResp = findRespAny.(*kmsg.FindCoordinatorResponse)
			if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) == survivorAddr {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) != survivorAddr {
		t.Fatalf("FindCoordinator after failover = %s:%d, want %s", findResp.Host, findResp.Port, survivorAddr)
	}

	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = groupID
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "group-failover-offsets",
		Partitions: []int32{0},
	}}
	fetchRespAny, err := sendKafkaRequest(survivorAddr, fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch after failover error: %v", err)
	}
	fetchResp := fetchRespAny.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("OffsetFetch response = %+v, want one topic/partition", fetchResp.Topics)
	}
	part := fetchResp.Topics[0].Partitions[0]
	if part.ErrorCode != 0 || part.Offset != 33 {
		t.Fatalf("OffsetFetch after failover = error %d offset %d, want error 0 offset 33", part.ErrorCode, part.Offset)
	}
}

func TestKafkaGroupCoordinatorFailoverAllowsRejoin(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-failover-rejoin", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-failover-rejoin"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	coordinatorAddr := net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port))
	coordinatorIdx := 0
	survivorAddr := addr2
	if coordinatorAddr == addr2 {
		coordinatorIdx = 1
		survivorAddr = addr1
	}

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"group-failover-rejoin"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinRespAny, err := sendKafkaRequest(coordinatorAddr, joinReq)
	if err != nil {
		t.Fatalf("initial JoinGroup error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("initial JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	env.StopInstance(coordinatorIdx)
	if err := env.WaitForInstance(1-coordinatorIdx, 5*time.Second); err != nil {
		t.Fatalf("survivor instance not ready: %v", err)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		findRespAny, err = sendKafkaRequest(survivorAddr, findReq)
		if err == nil {
			findResp = findRespAny.(*kmsg.FindCoordinatorResponse)
			if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) == survivorAddr {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) != survivorAddr {
		t.Fatalf("FindCoordinator after failover = %s:%d, want %s", findResp.Host, findResp.Port, survivorAddr)
	}

	rejoinRespAny, err := sendKafkaRequest(survivorAddr, joinReq)
	if err != nil {
		t.Fatalf("rejoin JoinGroup after failover error: %v", err)
	}
	rejoinResp := rejoinRespAny.(*kmsg.JoinGroupResponse)
	if rejoinResp.ErrorCode != 0 {
		t.Fatalf("rejoin JoinGroup error code = %d, want 0", rejoinResp.ErrorCode)
	}
}

func TestKafkaGroupCoordinatorFailoverAllowsHeartbeatForExistingMember(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-failover-heartbeat", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-failover-heartbeat"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	coordinatorAddr := net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port))
	coordinatorIdx := 0
	survivorAddr := addr2
	if coordinatorAddr == addr2 {
		coordinatorIdx = 1
		survivorAddr = addr1
	}

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"group-failover-heartbeat"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinRespAny, err := sendKafkaRequest(coordinatorAddr, joinReq)
	if err != nil {
		t.Fatalf("initial JoinGroup error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("initial JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	env.StopInstance(coordinatorIdx)
	if err := env.WaitForInstance(1-coordinatorIdx, 5*time.Second); err != nil {
		t.Fatalf("survivor instance not ready: %v", err)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		findRespAny, err = sendKafkaRequest(survivorAddr, findReq)
		if err == nil {
			findResp = findRespAny.(*kmsg.FindCoordinatorResponse)
			if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) == survivorAddr {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) != survivorAddr {
		t.Fatalf("FindCoordinator after failover = %s:%d, want %s", findResp.Host, findResp.Port, survivorAddr)
	}

	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = groupID
	hbReq.Generation = joinResp.Generation
	hbReq.MemberID = joinResp.MemberID
	hbRespAny, err := sendKafkaRequest(survivorAddr, hbReq)
	if err != nil {
		t.Fatalf("Heartbeat after failover error: %v", err)
	}
	hbResp := hbRespAny.(*kmsg.HeartbeatResponse)
	if hbResp.ErrorCode != 0 {
		t.Fatalf("Heartbeat after failover error code = %d, want 0", hbResp.ErrorCode)
	}
}

func TestKafkaGroupCoordinatorFailoverPreservesSyncAssignment(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	if err := env.Client().CreateTopic("group-failover-sync", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	groupID := "group-failover-sync"

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findRespAny, err := sendKafkaRequest(addr1, findReq)
	if err != nil {
		t.Fatalf("FindCoordinator error: %v", err)
	}
	findResp := findRespAny.(*kmsg.FindCoordinatorResponse)
	coordinatorAddr := net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port))
	coordinatorIdx := 0
	survivorAddr := addr2
	if coordinatorAddr == addr2 {
		coordinatorIdx = 1
		survivorAddr = addr1
	}

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = groupID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"group-failover-sync"}
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{{
		Name:     "range",
		Metadata: meta.AppendTo(nil),
	}}

	joinRespAny, err := sendKafkaRequest(coordinatorAddr, joinReq)
	if err != nil {
		t.Fatalf("initial JoinGroup error: %v", err)
	}
	joinResp := joinRespAny.(*kmsg.JoinGroupResponse)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("initial JoinGroup error code = %d, want 0", joinResp.ErrorCode)
	}

	assignment := kmsg.NewConsumerMemberAssignment()
	assignment.Version = 0
	assignment.Topics = []kmsg.ConsumerMemberAssignmentTopic{{
		Topic:      "group-failover-sync",
		Partitions: []int32{0},
	}}
	assignmentBytes := assignment.AppendTo(nil)

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.Generation = joinResp.Generation
	syncReq.MemberID = joinResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{
		MemberID:         joinResp.MemberID,
		MemberAssignment: assignmentBytes,
	}}

	syncRespAny, err := sendKafkaRequest(coordinatorAddr, syncReq)
	if err != nil {
		t.Fatalf("initial SyncGroup error: %v", err)
	}
	syncResp := syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("initial SyncGroup error code = %d, want 0", syncResp.ErrorCode)
	}
	if string(syncResp.MemberAssignment) != string(assignmentBytes) {
		t.Fatalf("initial SyncGroup assignment mismatch")
	}

	env.StopInstance(coordinatorIdx)
	if err := env.WaitForInstance(1-coordinatorIdx, 5*time.Second); err != nil {
		t.Fatalf("survivor instance not ready: %v", err)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		findRespAny, err = sendKafkaRequest(survivorAddr, findReq)
		if err == nil {
			findResp = findRespAny.(*kmsg.FindCoordinatorResponse)
			if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) == survivorAddr {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if net.JoinHostPort(findResp.Host, fmt.Sprintf("%d", findResp.Port)) != survivorAddr {
		t.Fatalf("FindCoordinator after failover = %s:%d, want %s", findResp.Host, findResp.Port, survivorAddr)
	}

	syncReq.GroupAssignment = nil
	syncRespAny, err = sendKafkaRequest(survivorAddr, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup after failover error: %v", err)
	}
	syncResp = syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup after failover error code = %d, want 0", syncResp.ErrorCode)
	}
	if string(syncResp.MemberAssignment) != string(assignmentBytes) {
		t.Fatalf("SyncGroup after failover assignment mismatch")
	}
}
