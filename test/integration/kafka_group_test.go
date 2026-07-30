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
	env, _, client, addr := newKafkaTopicBootstrappedEnv(t, "kafka-offsets")
	defer env.Cleanup()
	defer client.Close()

	createKafkaFixtureTopic(t, addr, "kafka-offsets-b", 1)

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

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	createKafkaFixtureTopic(t, addr1, "group-offsets-non-coordinator", 1)
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

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-group-introspection", 1)

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

func TestKafkaListGroupsFiltersByStateAndType(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-group-filters", 1)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = "group-filters"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"kafka-group-filters"}
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

	baseReq := kmsg.NewPtrListGroupsRequest()
	baseReq.SetVersion(5)
	baseRespAny, err := sendKafkaRequest(addr, baseReq)
	if err != nil {
		t.Fatalf("ListGroups baseline Request() error: %v", err)
	}
	baseResp := baseRespAny.(*kmsg.ListGroupsResponse)

	var groupState string
	found := false
	for _, group := range baseResp.Groups {
		if group.Group == "group-filters" {
			found = true
			groupState = group.GroupState
			if group.ProtocolType != "consumer" {
				t.Fatalf("baseline ListGroups protocol type = %q, want consumer", group.ProtocolType)
			}
			if group.GroupType != "consumer" {
				t.Fatalf("baseline ListGroups group type = %q, want consumer", group.GroupType)
			}
		}
	}
	if !found {
		t.Fatal("baseline ListGroups did not include group-filters")
	}
	if groupState == "" {
		t.Fatal("baseline ListGroups returned empty group state")
	}

	matchReq := kmsg.NewPtrListGroupsRequest()
	matchReq.SetVersion(5)
	matchReq.StatesFilter = []string{groupState}
	matchReq.TypesFilter = []string{"consumer"}
	matchRespAny, err := sendKafkaRequest(addr, matchReq)
	if err != nil {
		t.Fatalf("ListGroups filtered match Request() error: %v", err)
	}
	matchResp := matchRespAny.(*kmsg.ListGroupsResponse)
	if len(matchResp.Groups) != 1 {
		t.Fatalf("ListGroups filtered match groups = %d, want 1", len(matchResp.Groups))
	}
	if matchResp.Groups[0].Group != "group-filters" {
		t.Fatalf("ListGroups filtered match group = %q, want group-filters", matchResp.Groups[0].Group)
	}

	missStateReq := kmsg.NewPtrListGroupsRequest()
	missStateReq.SetVersion(5)
	missStateReq.StatesFilter = []string{"DefinitelyNotARealState"}
	missStateRespAny, err := sendKafkaRequest(addr, missStateReq)
	if err != nil {
		t.Fatalf("ListGroups filtered miss-state Request() error: %v", err)
	}
	missStateResp := missStateRespAny.(*kmsg.ListGroupsResponse)
	if len(missStateResp.Groups) != 0 {
		t.Fatalf("ListGroups filtered miss-state groups = %+v, want none", missStateResp.Groups)
	}

	missTypeReq := kmsg.NewPtrListGroupsRequest()
	missTypeReq.SetVersion(5)
	missTypeReq.TypesFilter = []string{"connect"}
	missTypeRespAny, err := sendKafkaRequest(addr, missTypeReq)
	if err != nil {
		t.Fatalf("ListGroups filtered miss-type Request() error: %v", err)
	}
	missTypeResp := missTypeRespAny.(*kmsg.ListGroupsResponse)
	if len(missTypeResp.Groups) != 0 {
		t.Fatalf("ListGroups filtered miss-type groups = %+v, want none", missTypeResp.Groups)
	}
}

func TestKafkaDescribeGroupsReflectsRebalanceStateTransitions(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-group-state-transitions", 1)

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.SetVersion(1)
	joinReq.Group = "group-state-transitions"
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{"kafka-group-state-transitions"}
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

	describeReq := kmsg.NewPtrDescribeGroupsRequest()
	describeReq.SetVersion(4)
	describeReq.Groups = []string{"group-state-transitions"}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups before sync Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeGroupsResponse)
	if len(describeResp.Groups) != 1 {
		t.Fatalf("DescribeGroups before sync groups = %d, want 1", len(describeResp.Groups))
	}
	group := describeResp.Groups[0]
	if group.State != "PreparingRebalance" {
		t.Fatalf("DescribeGroups state before sync = %q, want PreparingRebalance", group.State)
	}
	if len(group.Members) != 1 {
		t.Fatalf("DescribeGroups members before sync = %d, want 1", len(group.Members))
	}
	if len(group.Members[0].MemberAssignment) != 0 {
		t.Fatalf("DescribeGroups assignment before sync = %v, want empty", group.Members[0].MemberAssignment)
	}

	assignment := newConsumerMemberAssignment(t, "kafka-group-state-transitions", 0)
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.SetVersion(1)
	syncReq.Group = "group-state-transitions"
	syncReq.Generation = joinResp.Generation
	syncReq.MemberID = joinResp.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{
		MemberID:         joinResp.MemberID,
		MemberAssignment: assignment,
	}}
	syncRespAny, err := sendKafkaRequest(addr, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup Request() error: %v", err)
	}
	syncResp := syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup error code = %d, want 0", syncResp.ErrorCode)
	}

	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups after sync Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeGroupsResponse)
	if len(describeResp.Groups) != 1 {
		t.Fatalf("DescribeGroups after sync groups = %d, want 1", len(describeResp.Groups))
	}
	group = describeResp.Groups[0]
	if group.State != "Stable" {
		t.Fatalf("DescribeGroups state after sync = %q, want Stable", group.State)
	}
	if len(group.Members) != 1 {
		t.Fatalf("DescribeGroups members after sync = %d, want 1", len(group.Members))
	}
	if string(group.Members[0].MemberAssignment) != string(assignment) {
		t.Fatalf("DescribeGroups assignment after sync mismatch")
	}
}

func TestKafkaDescribeGroupsReflectsTwoMemberRebalance(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-group-two-member-state", 2)

	newJoinReq := func(memberID string) *kmsg.JoinGroupRequest {
		req := kmsg.NewPtrJoinGroupRequest()
		req.SetVersion(1)
		req.Group = "group-two-member-state"
		req.MemberID = memberID
		req.ProtocolType = "consumer"
		req.SessionTimeoutMillis = 10000
		meta := kmsg.NewConsumerMemberMetadata()
		meta.Version = 0
		meta.Topics = []string{"kafka-group-two-member-state"}
		req.Protocols = []kmsg.JoinGroupRequestProtocol{{
			Name:     "range",
			Metadata: meta.AppendTo(nil),
		}}
		return req
	}

	joinRespAny, err := sendKafkaRequest(addr, newJoinReq(""))
	if err != nil {
		t.Fatalf("JoinGroup leader Request() error: %v", err)
	}
	leaderJoin := joinRespAny.(*kmsg.JoinGroupResponse)
	if leaderJoin.ErrorCode != 0 {
		t.Fatalf("leader JoinGroup error code = %d, want 0", leaderJoin.ErrorCode)
	}

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.SetVersion(1)
	syncReq.Group = "group-two-member-state"
	syncReq.Generation = leaderJoin.Generation
	syncReq.MemberID = leaderJoin.MemberID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{
		MemberID:         leaderJoin.MemberID,
		MemberAssignment: newConsumerMemberAssignment(t, "kafka-group-two-member-state", 0),
	}}
	syncRespAny, err := sendKafkaRequest(addr, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup leader Request() error: %v", err)
	}
	syncResp := syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("leader SyncGroup error code = %d, want 0", syncResp.ErrorCode)
	}

	joinRespAny, err = sendKafkaRequest(addr, newJoinReq(""))
	if err != nil {
		t.Fatalf("JoinGroup second member Request() error: %v", err)
	}
	secondJoin := joinRespAny.(*kmsg.JoinGroupResponse)
	if secondJoin.ErrorCode != 0 {
		t.Fatalf("second JoinGroup error code = %d, want 0", secondJoin.ErrorCode)
	}
	if secondJoin.Generation <= leaderJoin.Generation {
		t.Fatalf("second JoinGroup generation = %d, want > %d", secondJoin.Generation, leaderJoin.Generation)
	}

	describeReq := kmsg.NewPtrDescribeGroupsRequest()
	describeReq.SetVersion(4)
	describeReq.Groups = []string{"group-two-member-state"}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups during rebalance Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeGroupsResponse)
	if len(describeResp.Groups) != 1 {
		t.Fatalf("DescribeGroups during rebalance groups = %d, want 1", len(describeResp.Groups))
	}
	group := describeResp.Groups[0]
	if group.State != "PreparingRebalance" {
		t.Fatalf("DescribeGroups during rebalance state = %q, want PreparingRebalance", group.State)
	}
	if len(group.Members) != 2 {
		t.Fatalf("DescribeGroups during rebalance members = %d, want 2", len(group.Members))
	}
	for _, member := range group.Members {
		if len(member.MemberAssignment) != 0 {
			t.Fatalf("DescribeGroups during rebalance assignment for %q = %v, want empty", member.MemberID, member.MemberAssignment)
		}
	}

	leaderAssignment := newConsumerMemberAssignment(t, "kafka-group-two-member-state", 0)
	secondAssignment := newConsumerMemberAssignment(t, "kafka-group-two-member-state", 1)
	syncReq = kmsg.NewPtrSyncGroupRequest()
	syncReq.SetVersion(1)
	syncReq.Group = "group-two-member-state"
	syncReq.Generation = secondJoin.Generation
	syncReq.MemberID = secondJoin.LeaderID
	syncReq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leaderJoin.MemberID, MemberAssignment: leaderAssignment},
		{MemberID: secondJoin.MemberID, MemberAssignment: secondAssignment},
	}
	syncRespAny, err = sendKafkaRequest(addr, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup leader rebalance Request() error: %v", err)
	}
	syncResp = syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("rebalance leader SyncGroup error code = %d, want 0", syncResp.ErrorCode)
	}

	syncReq = kmsg.NewPtrSyncGroupRequest()
	syncReq.SetVersion(1)
	syncReq.Group = "group-two-member-state"
	syncReq.Generation = secondJoin.Generation
	syncReq.MemberID = secondJoin.MemberID
	syncRespAny, err = sendKafkaRequest(addr, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup follower rebalance Request() error: %v", err)
	}
	syncResp = syncRespAny.(*kmsg.SyncGroupResponse)
	if syncResp.ErrorCode != 0 {
		t.Fatalf("rebalance follower SyncGroup error code = %d, want 0", syncResp.ErrorCode)
	}

	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups after rebalance Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeGroupsResponse)
	if len(describeResp.Groups) != 1 {
		t.Fatalf("DescribeGroups after rebalance groups = %d, want 1", len(describeResp.Groups))
	}
	group = describeResp.Groups[0]
	if group.State != "Stable" {
		t.Fatalf("DescribeGroups after rebalance state = %q, want Stable", group.State)
	}
	if len(group.Members) != 2 {
		t.Fatalf("DescribeGroups after rebalance members = %d, want 2", len(group.Members))
	}
	assignments := map[string][]byte{}
	for _, member := range group.Members {
		assignments[member.MemberID] = member.MemberAssignment
	}
	if string(assignments[leaderJoin.MemberID]) != string(leaderAssignment) {
		t.Fatalf("leader assignment after rebalance mismatch")
	}
	if string(assignments[secondJoin.MemberID]) != string(secondAssignment) {
		t.Fatalf("second member assignment after rebalance mismatch")
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

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-delete-groups", 1)

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
	waitForKafkaAddr(t, addr1)
	waitForKafkaAddr(t, addr2)
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
	waitForKafkaAddr(t, addr)
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

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "offset-delete-a", 1)
	createKafkaFixtureTopic(t, addr, "offset-delete-b", 1)

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

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "group-epoch-stamp", 1)

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

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	createKafkaFixtureTopic(t, addr1, "group-failover-offsets", 1)
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

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	createKafkaFixtureTopic(t, addr1, "group-failover-rejoin", 1)
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

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	createKafkaFixtureTopic(t, addr1, "group-failover-heartbeat", 1)
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

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	createKafkaFixtureTopic(t, addr1, "group-failover-sync", 1)
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
