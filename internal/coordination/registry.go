package coordination

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// InstanceInfo represents a registered instance in the cluster.
type InstanceInfo struct {
	InstanceID         string    `json:"instance_id"`
	Address            string    `json:"address"`
	InternalAddress    string    `json:"internal_address,omitempty"`
	ReplicationAddress string    `json:"replication_address,omitempty"`
	KafkaAddress       string    `json:"kafka_address,omitempty"`
	HeartbeatAt        time.Time `json:"heartbeat_at"`
}

// Registry provides instance discovery via S3-based registration.
// Each instance registers itself at startup and heartbeats periodically.
// ActiveInstances reads all registrations and filters by heartbeat freshness.
//
// Membership reads (ActiveInstances / ActiveInstanceInfos) always hit the
// object store so a newly-registered node is visible to peers immediately —
// a stale membership snapshot would let the controller under-provision
// replication on topic creation. Per-instance address lookups (GetInstanceInfo)
// are served from a short-lived cache, since addresses are the hot routing path
// (one lookup per unique replica per routing request) and change rarely.
type Registry struct {
	s3Client           *storage.S3Client
	instanceID         string
	address            string
	internalAddress    string
	replicationAddress string
	kafkaAddress       string
	ttl                time.Duration
	cacheRefresh       time.Duration

	cacheMu     sync.Mutex
	infoCache   map[string]InstanceInfo
	cacheLoaded time.Time
}

// NewRegistry creates a new Registry.
func NewRegistry(s3 *storage.S3Client, instanceID, address, internalAddress, replicationAddress, kafkaAddress string, ttl time.Duration) *Registry {
	refresh := ttl / 6
	if refresh < time.Second {
		refresh = time.Second
	}
	if refresh > 5*time.Second {
		refresh = 5 * time.Second
	}
	return &Registry{
		s3Client:           s3,
		instanceID:         instanceID,
		address:            address,
		internalAddress:    internalAddress,
		replicationAddress: replicationAddress,
		kafkaAddress:       kafkaAddress,
		ttl:                ttl,
		cacheRefresh:       refresh,
		infoCache:          make(map[string]InstanceInfo),
	}
}

func registryKey(instanceID string) string {
	return fmt.Sprintf("_coordination/instances/%s.json", instanceID)
}

// Register writes this instance's registration to S3.
// Should be called at startup and periodically as a heartbeat.
func (r *Registry) Register(ctx context.Context) error {
	info := InstanceInfo{
		InstanceID:         r.instanceID,
		Address:            r.address,
		InternalAddress:    r.internalAddress,
		ReplicationAddress: r.replicationAddress,
		KafkaAddress:       r.kafkaAddress,
		HeartbeatAt:        time.Now(),
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("registry: marshal: %w", err)
	}
	r.cacheMu.Lock()
	r.infoCache[r.instanceID] = info
	r.cacheMu.Unlock()
	return r.s3Client.Put(ctx, registryKey(r.instanceID), data, storage.PutOpts{})
}

// Deregister removes this instance's registration from S3.
// Should be called on graceful shutdown.
func (r *Registry) Deregister(ctx context.Context) error {
	r.cacheMu.Lock()
	delete(r.infoCache, r.instanceID)
	r.cacheMu.Unlock()
	return r.s3Client.Delete(ctx, registryKey(r.instanceID))
}

// refreshCache reloads all instance registrations when the cache has aged past
// cacheRefresh. Callers must not hold cacheMu.
func (r *Registry) refreshCache(ctx context.Context) error {
	r.cacheMu.Lock()
	if time.Since(r.cacheLoaded) < r.cacheRefresh {
		r.cacheMu.Unlock()
		return nil
	}
	r.cacheMu.Unlock()

	keys, err := r.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		return fmt.Errorf("registry: list: %w", err)
	}
	fresh := make(map[string]InstanceInfo, len(keys))
	for _, key := range keys {
		data, err := r.s3Client.Get(ctx, key)
		if err != nil {
			continue
		}
		var info InstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		fresh[info.InstanceID] = info
	}
	r.cacheMu.Lock()
	r.infoCache = fresh
	r.cacheLoaded = time.Now()
	r.cacheMu.Unlock()
	return nil
}

// loadInstances reads every instance registration fresh from the object store.
func (r *Registry) loadInstances(ctx context.Context) ([]InstanceInfo, error) {
	keys, err := r.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}
	now := time.Now()
	var infos []InstanceInfo
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
			infos = append(infos, info)
		}
	}
	return infos, nil
}

// ActiveInstances returns all instances with a heartbeat within the TTL.
// Read fresh: a newly-registered node must be visible to the controller
// immediately (a cached snapshot could under-provision replication).
func (r *Registry) ActiveInstances(ctx context.Context) ([]string, error) {
	infos, err := r.loadInstances(ctx)
	if err != nil {
		return nil, err
	}
	active := make([]string, 0, len(infos))
	for _, info := range infos {
		active = append(active, info.InstanceID)
	}
	return active, nil
}

// GetInstanceInfo reads an instance's registration, serving the address from a
// short-lived cache to avoid one GET per replica per routing request.
func (r *Registry) GetInstanceInfo(ctx context.Context, instanceID string) (InstanceInfo, error) {
	if err := r.refreshCache(ctx); err != nil {
		return InstanceInfo{}, fmt.Errorf("registry: get instance %s: %w", instanceID, err)
	}
	r.cacheMu.Lock()
	info, ok := r.infoCache[instanceID]
	r.cacheMu.Unlock()
	if !ok {
		return InstanceInfo{}, fmt.Errorf("registry: get instance %s: %w", instanceID, storage.ErrNotFound)
	}
	return info, nil
}

// ActiveInstanceInfos returns all active instance registrations.
// Read fresh for the same reason as ActiveInstances.
func (r *Registry) ActiveInstanceInfos(ctx context.Context) ([]InstanceInfo, error) {
	return r.loadInstances(ctx)
}

// InstanceID returns the instanceID this registry represents.
func (r *Registry) InstanceID() string { return r.instanceID }

// Address returns the network address of this instance.
func (r *Registry) Address() string { return r.address }
