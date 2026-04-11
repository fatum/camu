package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/storage"
)

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
