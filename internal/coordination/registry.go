package coordination

import (
	"context"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// Registry provides instance discovery by inspecting active leases in S3.
// Instances that hold at least one active lease for a topic are considered alive.
type Registry struct {
	s3Client   *storage.S3Client
	instanceID string
	address    string
}

// NewRegistry creates a new Registry.
func NewRegistry(s3 *storage.S3Client, instanceID, address string) *Registry {
	return &Registry{
		s3Client:   s3,
		instanceID: instanceID,
		address:    address,
	}
}

// ActiveInstances returns the set of instanceIDs that hold at least one active
// (non-expired) lease for the given topic.
func (r *Registry) ActiveInstances(ctx context.Context, topic string) ([]string, error) {
	store := NewLeaseStore(r.s3Client)
	leases, err := store.ListForTopic(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("registry: active instances: %w", err)
	}

	seen := make(map[string]struct{})
	now := time.Now()
	for _, l := range leases {
		if now.Before(l.ExpiresAt) {
			seen[l.InstanceID] = struct{}{}
		}
	}

	instances := make([]string, 0, len(seen))
	for id := range seen {
		instances = append(instances, id)
	}
	return instances, nil
}

// InstanceID returns the instanceID this registry represents.
func (r *Registry) InstanceID() string { return r.instanceID }

// Address returns the network address of this instance.
func (r *Registry) Address() string { return r.address }
