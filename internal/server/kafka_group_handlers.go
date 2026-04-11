package server

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

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
