package coordination

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// InstanceInfo represents a registered instance in the cluster.
type InstanceInfo struct {
	InstanceID      string    `json:"instance_id"`
	Address         string    `json:"address"`
	InternalAddress string    `json:"internal_address,omitempty"`
	HeartbeatAt     time.Time `json:"heartbeat_at"`
}

// Registry provides instance discovery via S3-based registration.
// Each instance registers itself at startup and heartbeats periodically.
// ActiveInstances reads all registrations and filters by heartbeat freshness.
type Registry struct {
	s3Client        *storage.S3Client
	instanceID      string
	address         string
	internalAddress string
	ttl             time.Duration
}

// NewRegistry creates a new Registry.
func NewRegistry(s3 *storage.S3Client, instanceID, address, internalAddress string, ttl time.Duration) *Registry {
	return &Registry{
		s3Client:        s3,
		instanceID:      instanceID,
		address:         address,
		internalAddress: internalAddress,
		ttl:             ttl,
	}
}

func registryKey(instanceID string) string {
	return fmt.Sprintf("_coordination/instances/%s.json", instanceID)
}

// Register writes this instance's registration to S3.
// Should be called at startup and periodically as a heartbeat.
func (r *Registry) Register(ctx context.Context) error {
	info := InstanceInfo{
		InstanceID:      r.instanceID,
		Address:         r.address,
		InternalAddress: r.internalAddress,
		HeartbeatAt:     time.Now(),
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("registry: marshal: %w", err)
	}
	return r.s3Client.Put(ctx, registryKey(r.instanceID), data, storage.PutOpts{})
}

// Deregister removes this instance's registration from S3.
// Should be called on graceful shutdown.
func (r *Registry) Deregister(ctx context.Context) error {
	return r.s3Client.Delete(ctx, registryKey(r.instanceID))
}

// ActiveInstances returns all instances with a heartbeat within the TTL.
func (r *Registry) ActiveInstances(ctx context.Context) ([]string, error) {
	keys, err := r.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}

	now := time.Now()
	var active []string
	for _, key := range keys {
		data, err := r.s3Client.Get(ctx, key)
		if err != nil {
			continue
		}
		var info InstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		if now.Sub(info.HeartbeatAt) < r.ttl {
			active = append(active, info.InstanceID)
		}
	}
	return active, nil
}

// GetInstanceInfo reads an instance's registration from S3.
func (r *Registry) GetInstanceInfo(ctx context.Context, instanceID string) (InstanceInfo, error) {
	data, err := r.s3Client.Get(ctx, registryKey(instanceID))
	if err != nil {
		return InstanceInfo{}, fmt.Errorf("registry: get instance %s: %w", instanceID, err)
	}
	var info InstanceInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return InstanceInfo{}, fmt.Errorf("registry: unmarshal instance %s: %w", instanceID, err)
	}
	return info, nil
}

// InstanceID returns the instanceID this registry represents.
func (r *Registry) InstanceID() string { return r.instanceID }

// Address returns the network address of this instance.
func (r *Registry) Address() string { return r.address }
