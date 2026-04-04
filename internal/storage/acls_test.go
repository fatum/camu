package storage

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLStore_CreateListDeleteMatching(t *testing.T) {
	s3, err := NewS3Client(S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}
	store := NewACLStore(s3)
	ctx := context.Background()

	err = store.Create(ctx, []ACLRecord{
		{
			ResourceType:        kmsg.ACLResourceTypeTopic,
			ResourceName:        "orders",
			ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
			Principal:           "User:alice",
			Host:                "*",
			Operation:           kmsg.ACLOperationRead,
			PermissionType:      kmsg.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kmsg.ACLResourceTypeGroup,
			ResourceName:        "analytics-",
			ResourcePatternType: kmsg.ACLResourcePatternTypePrefixed,
			Principal:           "User:bob",
			Host:                "*",
			Operation:           kmsg.ACLOperationRead,
			PermissionType:      kmsg.ACLPermissionTypeAllow,
		},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	acls, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(acls) != 2 {
		t.Fatalf("List() len = %d, want 2", len(acls))
	}

	matched, err := store.DeleteMatching(ctx, []ACLFilter{{
		ResourceType:        kmsg.ACLResourceTypeGroup,
		ResourceName:        strPtr("analytics-consumers"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeMatch,
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAny,
	}})
	if err != nil {
		t.Fatalf("DeleteMatching() error = %v", err)
	}
	if len(matched) != 1 || len(matched[0]) != 1 {
		t.Fatalf("DeleteMatching() matched = %#v, want 1 match", matched)
	}

	acls, err = store.List(ctx)
	if err != nil {
		t.Fatalf("List() after delete error = %v", err)
	}
	if len(acls) != 1 {
		t.Fatalf("List() after delete len = %d, want 1", len(acls))
	}
}

func strPtr(v string) *string { return &v }
