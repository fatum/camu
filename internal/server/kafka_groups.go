package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	kafkaErrorNotCoordinator            int16 = 16
	kafkaErrorIllegalGeneration         int16 = 22
	kafkaErrorInconsistentGroupProtocol int16 = 23
	kafkaErrorUnknownMemberID           int16 = 25
	kafkaErrorRebalanceInProgress       int16 = 27
	kafkaErrorNonEmptyGroup             int16 = 68
	kafkaErrorGroupIDNotFound           int16 = 69
)

const kafkaGroupMaxCASRetries = 4
const kafkaGroupHeartbeatPersistInterval = 5 * time.Second

type kafkaGroupCoordinator struct {
	mu              sync.Mutex
	s3Client        *storage.S3Client
	instanceID      string
	nextID          uint64
	cache           map[string]*kafkaGroupCacheEntry
	now             func() time.Time
	controllerEpoch func() string
}

type kafkaGroupCacheEntry struct {
	state                *kafkaGroupState
	lastHeartbeatPersist time.Time
}

type kafkaGroupState struct {
	Group           string                      `json:"group"`
	Version         uint64                      `json:"version"`
	ETag            string                      `json:"-"`
	ControllerEpoch string                      `json:"controller_epoch,omitempty"`
	Generation      int32                       `json:"generation"`
	ProtocolType    string                      `json:"protocol_type"`
	Protocol        string                      `json:"protocol"`
	LeaderID        string                      `json:"leader_id"`
	Members         map[string]kafkaGroupMember `json:"members"`
	Assignments     map[string][]byte           `json:"assignments"`
}

type kafkaGroupMember struct {
	MemberID         string        `json:"member_id"`
	ProtocolMetadata []byte        `json:"protocol_metadata"`
	SessionTimeout   time.Duration `json:"session_timeout"`
	LastHeartbeat    time.Time     `json:"last_heartbeat"`
}

func newKafkaGroupCoordinator(s3Client *storage.S3Client, instanceID string) *kafkaGroupCoordinator {
	return &kafkaGroupCoordinator{
		s3Client:   s3Client,
		instanceID: instanceID,
		cache:      make(map[string]*kafkaGroupCacheEntry),
		now:        time.Now,
	}
}

func kafkaGroupKey(group string) string {
	return fmt.Sprintf("_coordination/kafka-groups/%s.json", group)
}

func (gc *kafkaGroupCoordinator) joinGroup(ctx context.Context, req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, error) {
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.Generation = -1

	if req.Group == "" || len(req.Protocols) == 0 {
		resp.ErrorCode = kafkaErrorInconsistentGroupProtocol
		return resp, nil
	}

	return mutateKafkaGroup(gc, ctx, req.Group, func(group *kafkaGroupState) (*kmsg.JoinGroupResponse, error) {
		now := gc.now()
		if group.ProtocolType == "" {
			group.ProtocolType = req.ProtocolType
		}
		if group.ProtocolType != req.ProtocolType {
			resp.ErrorCode = kafkaErrorInconsistentGroupProtocol
			return resp, nil
		}

		selected, ok := selectJoinProtocol(group.Protocol, req.Protocols)
		if !ok {
			resp.ErrorCode = kafkaErrorInconsistentGroupProtocol
			return resp, nil
		}
		group.Protocol = selected.Name

		memberID := req.MemberID
		membershipChanged := false
		if memberID == "" {
			memberID = gc.newMemberID(req.Group)
			membershipChanged = true
		}
		existing, exists := group.Members[memberID]
		if !exists ||
			existing.SessionTimeout != time.Duration(req.SessionTimeoutMillis)*time.Millisecond ||
			!slices.Equal(existing.ProtocolMetadata, selected.Metadata) {
			membershipChanged = true
		}

		group.Members[memberID] = kafkaGroupMember{
			MemberID:         memberID,
			ProtocolMetadata: append([]byte(nil), selected.Metadata...),
			SessionTimeout:   time.Duration(req.SessionTimeoutMillis) * time.Millisecond,
			LastHeartbeat:    now,
		}
		if group.Generation == 0 {
			group.Generation = 1
		} else if membershipChanged {
			group.Generation++
			group.Assignments = make(map[string][]byte)
		}
		if group.LeaderID == "" || group.Members[group.LeaderID].MemberID == "" {
			group.LeaderID = sortedGroupMemberIDs(group.Members)[0]
		}

		resp.Generation = group.Generation
		resp.ProtocolType = kmsg.StringPtr(group.ProtocolType)
		resp.Protocol = kmsg.StringPtr(group.Protocol)
		resp.LeaderID = group.LeaderID
		resp.MemberID = memberID
		for _, id := range sortedGroupMemberIDs(group.Members) {
			member := group.Members[id]
			resp.Members = append(resp.Members, kmsg.JoinGroupResponseMember{
				MemberID:         member.MemberID,
				ProtocolMetadata: append([]byte(nil), member.ProtocolMetadata...),
			})
		}
		return resp, nil
	})
}

func (gc *kafkaGroupCoordinator) syncGroup(ctx context.Context, req *kmsg.SyncGroupRequest) (*kmsg.SyncGroupResponse, error) {
	resp := kmsg.NewPtrSyncGroupResponse()
	resp.ProtocolType = kmsg.StringPtr("consumer")

	return mutateKafkaGroup(gc, ctx, req.Group, func(group *kafkaGroupState) (*kmsg.SyncGroupResponse, error) {
		member, ok := group.Members[req.MemberID]
		if !ok {
			resp.ErrorCode = kafkaErrorUnknownMemberID
			return resp, nil
		}
		if req.Generation != group.Generation {
			resp.ErrorCode = kafkaErrorIllegalGeneration
			return resp, nil
		}

		if len(req.GroupAssignment) > 0 {
			group.Assignments = make(map[string][]byte, len(req.GroupAssignment))
			for _, assignment := range req.GroupAssignment {
				group.Assignments[assignment.MemberID] = append([]byte(nil), assignment.MemberAssignment...)
			}
		}

		assignment, ok := group.Assignments[req.MemberID]
		if !ok {
			resp.ErrorCode = kafkaErrorRebalanceInProgress
			return resp, nil
		}
		resp.Protocol = kmsg.StringPtr(group.Protocol)
		resp.MemberAssignment = append([]byte(nil), assignment...)
		member.LastHeartbeat = gc.now()
		group.Members[req.MemberID] = member
		return resp, nil
	})
}

func (gc *kafkaGroupCoordinator) heartbeat(ctx context.Context, req *kmsg.HeartbeatRequest) (*kmsg.HeartbeatResponse, error) {
	resp := kmsg.NewPtrHeartbeatResponse()

	gc.mu.Lock()
	defer gc.mu.Unlock()

	var lastErr error
	for range kafkaGroupMaxCASRetries {
		group, err := gc.loadGroupLocked(ctx, req.Group)
		if err != nil {
			return nil, err
		}

		now := gc.now()
		expired := expireKafkaGroupMembers(group, now)
		member, ok := group.Members[req.MemberID]
		if !ok {
			resp.ErrorCode = kafkaErrorUnknownMemberID
		} else if req.Generation != group.Generation {
			resp.ErrorCode = kafkaErrorIllegalGeneration
		} else {
			member.LastHeartbeat = now
			group.Members[req.MemberID] = member
		}

		entry := gc.cache[req.Group]
		if !expired && resp.ErrorCode == 0 && entry != nil && now.Sub(entry.lastHeartbeatPersist) < kafkaGroupHeartbeatPersistInterval {
			entry.state = cloneKafkaGroupState(group)
			return resp, nil
		}
		if err := gc.writeGroup(ctx, group); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				lastErr = err
				gc.invalidateGroupCacheLocked(req.Group)
				continue
			}
			return nil, err
		}
		gc.storeGroupCacheLocked(group, now)
		return resp, nil
	}
	return nil, fmt.Errorf("kafka group %q heartbeat: CAS retries exhausted: %w", req.Group, lastErr)
}

func (gc *kafkaGroupCoordinator) leaveGroup(ctx context.Context, req *kmsg.LeaveGroupRequest) (*kmsg.LeaveGroupResponse, error) {
	resp := kmsg.NewPtrLeaveGroupResponse()

	return mutateKafkaGroup(gc, ctx, req.Group, func(group *kafkaGroupState) (*kmsg.LeaveGroupResponse, error) {
		if len(group.Members) == 0 {
			resp.ErrorCode = kafkaErrorUnknownMemberID
			return resp, nil
		}

		memberIDs := make([]string, 0, 1)
		if req.GetVersion() >= 3 {
			for _, member := range req.Members {
				memberIDs = append(memberIDs, member.MemberID)
			}
		} else if req.MemberID != "" {
			memberIDs = append(memberIDs, req.MemberID)
		}

		for _, memberID := range memberIDs {
			if _, ok := group.Members[memberID]; !ok {
				resp.ErrorCode = kafkaErrorUnknownMemberID
				continue
			}
			delete(group.Members, memberID)
			delete(group.Assignments, memberID)
			if req.GetVersion() >= 3 {
				resp.Members = append(resp.Members, kmsg.LeaveGroupResponseMember{MemberID: memberID})
			}
		}

		if len(group.Members) == 0 {
			group.LeaderID = ""
			group.Assignments = make(map[string][]byte)
			group.Generation++
			return resp, nil
		}

		memberIDs = sortedGroupMemberIDs(group.Members)
		if group.LeaderID == "" || !slices.Contains(memberIDs, group.LeaderID) {
			group.LeaderID = memberIDs[0]
			group.Generation++
			group.Assignments = make(map[string][]byte)
		}
		return resp, nil
	})
}

func (gc *kafkaGroupCoordinator) describeGroup(ctx context.Context, groupID string) (kmsg.DescribeGroupsResponseGroup, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	group, err := gc.loadGroupLocked(ctx, groupID)
	if err != nil {
		return kmsg.DescribeGroupsResponseGroup{}, err
	}
	expireKafkaGroupMembers(group, gc.now())

	resp := kmsg.NewDescribeGroupsResponseGroup()
	resp.Group = groupID
	resp.State = kafkaGroupStateName(group)
	resp.ProtocolType = group.ProtocolType
	resp.Protocol = group.Protocol
	for _, memberID := range sortedGroupMemberIDs(group.Members) {
		member := group.Members[memberID]
		memberResp := kmsg.NewDescribeGroupsResponseGroupMember()
		memberResp.MemberID = member.MemberID
		memberResp.ProtocolMetadata = append([]byte(nil), member.ProtocolMetadata...)
		memberResp.MemberAssignment = append([]byte(nil), group.Assignments[memberID]...)
		resp.Members = append(resp.Members, memberResp)
	}
	return resp, nil
}

func (gc *kafkaGroupCoordinator) listGroups(ctx context.Context, statesFilter, typesFilter []string) (*kmsg.ListGroupsResponse, error) {
	resp := kmsg.NewPtrListGroupsResponse()

	keys, err := gc.s3Client.List(ctx, "_coordination/kafka-groups/")
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		groupID := strings.TrimSuffix(path.Base(key), ".json")
		if groupID == "" {
			continue
		}

		group, err := gc.readGroup(ctx, groupID)
		if err != nil {
			return nil, err
		}
		expireKafkaGroupMembers(group, gc.now())

		groupState := kafkaGroupStateName(group)
		groupType := kafkaGroupTypeName(group)
		if len(statesFilter) > 0 && !slices.Contains(statesFilter, groupState) {
			continue
		}
		if len(typesFilter) > 0 && !slices.Contains(typesFilter, groupType) {
			continue
		}

		groupResp := kmsg.NewListGroupsResponseGroup()
		groupResp.Group = groupID
		groupResp.ProtocolType = group.ProtocolType
		groupResp.GroupState = groupState
		groupResp.GroupType = groupType
		resp.Groups = append(resp.Groups, groupResp)
	}

	slices.SortFunc(resp.Groups, func(a, b kmsg.ListGroupsResponseGroup) int {
		return strings.Compare(a.Group, b.Group)
	})
	return resp, nil
}

func (gc *kafkaGroupCoordinator) deleteGroup(ctx context.Context, groupID string) (int16, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	group, err := gc.readGroup(ctx, groupID)
	if err != nil {
		return kafkaErrorUnknownServer, err
	}
	if group.Version == 0 && len(group.Members) == 0 && len(group.Assignments) == 0 && group.ProtocolType == "" && group.Protocol == "" {
		return kafkaErrorGroupIDNotFound, nil
	}

	expireKafkaGroupMembers(group, gc.now())
	if len(group.Members) > 0 {
		return kafkaErrorNonEmptyGroup, nil
	}

	if err := gc.s3Client.Delete(ctx, kafkaGroupKey(groupID)); err != nil {
		return kafkaErrorUnknownServer, err
	}
	delete(gc.cache, groupID)
	return 0, nil
}

func mutateKafkaGroup[T any](gc *kafkaGroupCoordinator, ctx context.Context, groupID string, fn func(*kafkaGroupState) (T, error)) (T, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	var lastErr error
	for range kafkaGroupMaxCASRetries {
		group, err := gc.loadGroupLocked(ctx, groupID)
		if err != nil {
			var zero T
			return zero, err
		}
		expireKafkaGroupMembers(group, gc.now())
		result, err := fn(group)
		if err != nil {
			var zero T
			return zero, err
		}
		if err := gc.writeGroup(ctx, group); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				lastErr = err
				gc.invalidateGroupCacheLocked(groupID)
				continue
			}
			var zero T
			return zero, err
		}
		gc.storeGroupCacheLocked(group, gc.now())
		return result, nil
	}
	var zero T
	return zero, fmt.Errorf("kafka group %q: CAS retries exhausted: %w", groupID, lastErr)
}

func (gc *kafkaGroupCoordinator) loadGroupLocked(ctx context.Context, groupID string) (*kafkaGroupState, error) {
	entry := gc.ensureCacheEntryLocked(groupID)
	if entry.state != nil {
		return cloneKafkaGroupState(entry.state), nil
	}
	group, err := gc.readGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	entry.state = cloneKafkaGroupState(group)
	return group, nil
}

func (gc *kafkaGroupCoordinator) storeGroupCacheLocked(group *kafkaGroupState, persistedAt time.Time) {
	entry := gc.ensureCacheEntryLocked(group.Group)
	entry.state = cloneKafkaGroupState(group)
	entry.lastHeartbeatPersist = persistedAt
}

func (gc *kafkaGroupCoordinator) invalidateGroupCacheLocked(group string) {
	entry, ok := gc.cache[group]
	if !ok {
		return
	}
	entry.state = nil
}

func (gc *kafkaGroupCoordinator) ensureCacheEntryLocked(group string) *kafkaGroupCacheEntry {
	entry, ok := gc.cache[group]
	if !ok {
		entry = &kafkaGroupCacheEntry{}
		gc.cache[group] = entry
	}
	return entry
}

func cloneKafkaGroupState(group *kafkaGroupState) *kafkaGroupState {
	if group == nil {
		return nil
	}
	cloned := *group
	cloned.Members = make(map[string]kafkaGroupMember, len(group.Members))
	for id, member := range group.Members {
		member.ProtocolMetadata = append([]byte(nil), member.ProtocolMetadata...)
		cloned.Members[id] = member
	}
	cloned.Assignments = make(map[string][]byte, len(group.Assignments))
	for id, assignment := range group.Assignments {
		cloned.Assignments[id] = append([]byte(nil), assignment...)
	}
	return &cloned
}

func kafkaGroupStateName(group *kafkaGroupState) string {
	if group == nil || len(group.Members) == 0 {
		return "Empty"
	}
	if len(group.Assignments) == len(group.Members) {
		return "Stable"
	}
	return "PreparingRebalance"
}

func kafkaGroupTypeName(group *kafkaGroupState) string {
	if group != nil && group.ProtocolType != "" {
		return group.ProtocolType
	}
	return "classic"
}

func expireKafkaGroupMembers(group *kafkaGroupState, now time.Time) bool {
	if group == nil || len(group.Members) == 0 {
		return false
	}

	expired := false
	for memberID, member := range group.Members {
		if member.SessionTimeout <= 0 {
			continue
		}
		if now.Sub(member.LastHeartbeat) <= member.SessionTimeout {
			continue
		}
		delete(group.Members, memberID)
		delete(group.Assignments, memberID)
		expired = true
	}
	if !expired {
		return false
	}

	group.Generation++
	group.Assignments = make(map[string][]byte)
	if len(group.Members) == 0 {
		group.LeaderID = ""
		return true
	}

	memberIDs := sortedGroupMemberIDs(group.Members)
	if !slices.Contains(memberIDs, group.LeaderID) {
		group.LeaderID = memberIDs[0]
	}
	return true
}

func (gc *kafkaGroupCoordinator) readGroup(ctx context.Context, groupID string) (*kafkaGroupState, error) {
	data, etag, err := gc.s3Client.GetWithETag(ctx, kafkaGroupKey(groupID))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &kafkaGroupState{
				Group:       groupID,
				Members:     make(map[string]kafkaGroupMember),
				Assignments: make(map[string][]byte),
			}, nil
		}
		return nil, fmt.Errorf("kafka group %q: get: %w", groupID, err)
	}

	var group kafkaGroupState
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("kafka group %q: unmarshal: %w", groupID, err)
	}
	group.ETag = etag
	if group.Group == "" {
		group.Group = groupID
	}
	if group.Members == nil {
		group.Members = make(map[string]kafkaGroupMember)
	}
	if group.Assignments == nil {
		group.Assignments = make(map[string][]byte)
	}
	return &group, nil
}

func (gc *kafkaGroupCoordinator) writeGroup(ctx context.Context, group *kafkaGroupState) error {
	group.Version++
	if gc.controllerEpoch != nil {
		group.ControllerEpoch = gc.controllerEpoch()
	}
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("kafka group %q: marshal: %w", group.Group, err)
	}
	newETag, err := gc.s3Client.ConditionalPut(ctx, kafkaGroupKey(group.Group), data, group.ETag)
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return err
		}
		return fmt.Errorf("kafka group %q: conditional put: %w", group.Group, err)
	}
	group.ETag = newETag
	return nil
}

func (gc *kafkaGroupCoordinator) newMemberID(group string) string {
	gc.nextID++
	return fmt.Sprintf("%s-%s-member-%d", gc.instanceID, group, gc.nextID)
}

func selectJoinProtocol(current string, protocols []kmsg.JoinGroupRequestProtocol) (kmsg.JoinGroupRequestProtocol, bool) {
	if current != "" {
		for _, protocol := range protocols {
			if protocol.Name == current {
				return protocol, true
			}
		}
		return kmsg.JoinGroupRequestProtocol{}, false
	}
	return protocols[0], len(protocols) > 0
}

func sortedGroupMemberIDs(members map[string]kafkaGroupMember) []string {
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
