package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const aclKey = "_meta/acls.json"

type ACLStore struct {
	s3Client *S3Client
}

type ACLRecord struct {
	ResourceType        kmsg.ACLResourceType        `json:"resource_type"`
	ResourceName        string                      `json:"resource_name"`
	ResourcePatternType kmsg.ACLResourcePatternType `json:"resource_pattern_type"`
	Principal           string                      `json:"principal"`
	Host                string                      `json:"host"`
	Operation           kmsg.ACLOperation           `json:"operation"`
	PermissionType      kmsg.ACLPermissionType      `json:"permission_type"`
}

type aclData struct {
	ACLs []ACLRecord `json:"acls"`
}

func NewACLStore(s3 *S3Client) *ACLStore {
	return &ACLStore{s3Client: s3}
}

func (s *ACLStore) Create(ctx context.Context, creations []ACLRecord) error {
	acls, err := s.List(ctx)
	if err != nil {
		return err
	}
	acls = append(acls, creations...)
	return s.put(ctx, acls)
}

func (s *ACLStore) List(ctx context.Context) ([]ACLRecord, error) {
	raw, err := s.s3Client.Get(ctx, aclKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get acls: %w", err)
	}
	var data aclData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal acls: %w", err)
	}
	return data.ACLs, nil
}

func (s *ACLStore) DeleteMatching(ctx context.Context, filters []ACLFilter) ([][]ACLRecord, error) {
	acls, err := s.List(ctx)
	if err != nil {
		return nil, err
	}

	matchedByFilter := make([][]ACLRecord, len(filters))
	keep := make([]ACLRecord, 0, len(acls))

	for _, acl := range acls {
		matched := false
		for i, filter := range filters {
			if filter.Matches(acl) {
				matchedByFilter[i] = append(matchedByFilter[i], acl)
				matched = true
			}
		}
		if !matched {
			keep = append(keep, acl)
		}
	}

	if err := s.put(ctx, keep); err != nil {
		return nil, err
	}
	return matchedByFilter, nil
}

func (s *ACLStore) put(ctx context.Context, acls []ACLRecord) error {
	raw, err := json.Marshal(aclData{ACLs: acls})
	if err != nil {
		return fmt.Errorf("marshal acls: %w", err)
	}
	return s.s3Client.Put(ctx, aclKey, raw, PutOpts{ContentType: "application/json"})
}

type ACLFilter struct {
	ResourceType        kmsg.ACLResourceType
	ResourceName        *string
	ResourcePatternType kmsg.ACLResourcePatternType
	Principal           *string
	Host                *string
	Operation           kmsg.ACLOperation
	PermissionType      kmsg.ACLPermissionType
}

func (f ACLFilter) Matches(acl ACLRecord) bool {
	if f.ResourceType != kmsg.ACLResourceTypeAny && f.ResourceType != acl.ResourceType {
		return false
	}
	if !matchACLPattern(f.ResourcePatternType, f.ResourceName, acl.ResourcePatternType, acl.ResourceName) {
		return false
	}
	if f.Principal != nil && *f.Principal != acl.Principal {
		return false
	}
	if f.Host != nil && *f.Host != acl.Host {
		return false
	}
	if f.Operation != kmsg.ACLOperationAny && f.Operation != acl.Operation {
		return false
	}
	if f.PermissionType != kmsg.ACLPermissionTypeAny && f.PermissionType != acl.PermissionType {
		return false
	}
	return true
}

func matchACLPattern(filterType kmsg.ACLResourcePatternType, filterName *string, aclType kmsg.ACLResourcePatternType, aclName string) bool {
	switch filterType {
	case kmsg.ACLResourcePatternTypeAny:
		if filterName == nil {
			return true
		}
		return matchACLName(*filterName, aclType, aclName)
	case kmsg.ACLResourcePatternTypeMatch:
		if filterName == nil {
			return true
		}
		return matchACLName(*filterName, aclType, aclName)
	case kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypePrefixed:
		if aclType != filterType {
			return false
		}
		if filterName == nil {
			return true
		}
		return *filterName == aclName
	default:
		return false
	}
}

func matchACLName(filterName string, aclType kmsg.ACLResourcePatternType, aclName string) bool {
	switch aclType {
	case kmsg.ACLResourcePatternTypeLiteral:
		return filterName == aclName
	case kmsg.ACLResourcePatternTypePrefixed:
		return strings.HasPrefix(filterName, aclName)
	default:
		return false
	}
}
