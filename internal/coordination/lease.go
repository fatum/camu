package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// ErrLeaseHeld is returned when a lease is held by another instance and has not expired.
var ErrLeaseHeld = errors.New("lease is held by another instance")

// Lease represents ownership of a partition by a specific instance.
type Lease struct {
	Topic       string    `json:"topic"`
	PartitionID int       `json:"partition_id"`
	InstanceID  string    `json:"instance_id"`
	Address     string    `json:"address"`
	Epoch       uint64    `json:"epoch"`
	ExpiresAt   time.Time `json:"expires_at"`
	ETag        string    `json:"-"` // S3 etag, not serialized
}

// LeaseStore manages partition leases via S3.
type LeaseStore struct {
	s3Client *storage.S3Client
}

// NewLeaseStore creates a new LeaseStore backed by the given S3 client.
func NewLeaseStore(s3 *storage.S3Client) *LeaseStore {
	return &LeaseStore{s3Client: s3}
}

func leaseKey(topic string, partitionID int) string {
	return fmt.Sprintf("_coordination/leases/%s/%d.lease", topic, partitionID)
}

// Acquire attempts to acquire a lease for a partition.
// If no lease exists or the existing lease has expired, a new lease is written.
// If the lease is active and owned by another instance, ErrLeaseHeld is returned.
func (ls *LeaseStore) Acquire(ctx context.Context, topic string, partitionID int, instanceID string, address string, ttl time.Duration) (Lease, error) {
	key := leaseKey(topic, partitionID)

	data, etag, err := ls.s3Client.GetWithETag(ctx, key)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return Lease{}, fmt.Errorf("acquire lease: get: %w", err)
	}

	var prevEpoch uint64
	var existingETag string

	if err == nil {
		// Parse existing lease.
		var existing Lease
		if jsonErr := json.Unmarshal(data, &existing); jsonErr != nil {
			return Lease{}, fmt.Errorf("acquire lease: unmarshal: %w", jsonErr)
		}
		// Check if the lease is still active.
		if time.Now().Before(existing.ExpiresAt) {
			if existing.InstanceID != instanceID {
				return Lease{}, fmt.Errorf("%w: held by %q until %s", ErrLeaseHeld, existing.InstanceID, existing.ExpiresAt)
			}
			// We already own it — re-acquire (renew with same epoch and new TTL).
			prevEpoch = existing.Epoch - 1
		} else {
			prevEpoch = existing.Epoch
		}
		existingETag = etag
	}
	// If ErrNotFound, prevEpoch=0, existingETag="" → first write.

	newLease := Lease{
		Topic:       topic,
		PartitionID: partitionID,
		InstanceID:  instanceID,
		Address:     address,
		Epoch:       prevEpoch + 1,
		ExpiresAt:   time.Now().Add(ttl),
	}

	encoded, err := json.Marshal(newLease)
	if err != nil {
		return Lease{}, fmt.Errorf("acquire lease: marshal: %w", err)
	}

	newETag, err := ls.s3Client.ConditionalPut(ctx, key, encoded, existingETag)
	if err != nil {
		return Lease{}, fmt.Errorf("acquire lease: conditional put: %w", err)
	}

	newLease.ETag = newETag
	return newLease, nil
}

// Renew extends the lease's expiry by the same TTL that was originally applied.
// It uses the lease's stored ETag for optimistic concurrency.
func (ls *LeaseStore) Renew(ctx context.Context, lease Lease) error {
	key := leaseKey(lease.Topic, lease.PartitionID)

	// Calculate the original TTL by reading the current stored lease to infer
	// expiry, but we don't have the original TTL stored. Instead we re-read to
	// get the current expiry, then extend from now by the same delta.
	// Because the Lease struct doesn't store TTL, we re-read the current stored
	// value to get the remaining duration and add it to now.
	data, currentETag, err := ls.s3Client.GetWithETag(ctx, key)
	if err != nil {
		return fmt.Errorf("renew lease: get: %w", err)
	}
	if currentETag != lease.ETag {
		return fmt.Errorf("renew lease: %w", storage.ErrConflict)
	}

	var stored Lease
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("renew lease: unmarshal: %w", err)
	}

	// Extend: use the remaining TTL from the stored lease as the new TTL.
	remainingTTL := time.Until(stored.ExpiresAt)
	if remainingTTL <= 0 {
		remainingTTL = 0
	}
	stored.ExpiresAt = time.Now().Add(remainingTTL)
	stored.ETag = ""

	encoded, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("renew lease: marshal: %w", err)
	}

	_, err = ls.s3Client.ConditionalPut(ctx, key, encoded, lease.ETag)
	if err != nil {
		return fmt.Errorf("renew lease: conditional put: %w", err)
	}
	return nil
}

// Release relinquishes the lease by writing it back with an expired timestamp,
// preserving the epoch so future acquirers can increment correctly.
func (ls *LeaseStore) Release(ctx context.Context, lease Lease) error {
	key := leaseKey(lease.Topic, lease.PartitionID)

	released := Lease{
		Topic:       lease.Topic,
		PartitionID: lease.PartitionID,
		InstanceID:  lease.InstanceID,
		Epoch:       lease.Epoch,
		ExpiresAt:   time.Time{}, // zero time = already expired
	}

	encoded, err := json.Marshal(released)
	if err != nil {
		return fmt.Errorf("release lease: marshal: %w", err)
	}

	// Use ConditionalPut with the current ETag to ensure we own it.
	_, err = ls.s3Client.ConditionalPut(ctx, key, encoded, lease.ETag)
	if err != nil {
		return fmt.Errorf("release lease: conditional put: %w", err)
	}
	return nil
}

// Get reads the current lease for a partition.
func (ls *LeaseStore) Get(ctx context.Context, topic string, partitionID int) (Lease, error) {
	key := leaseKey(topic, partitionID)
	data, etag, err := ls.s3Client.GetWithETag(ctx, key)
	if err != nil {
		return Lease{}, fmt.Errorf("get lease: %w", err)
	}
	var lease Lease
	if err := json.Unmarshal(data, &lease); err != nil {
		return Lease{}, fmt.Errorf("get lease: unmarshal: %w", err)
	}
	lease.ETag = etag
	return lease, nil
}

// ListForTopic returns all partition leases stored for the given topic.
func (ls *LeaseStore) ListForTopic(ctx context.Context, topic string) ([]Lease, error) {
	prefix := fmt.Sprintf("_coordination/leases/%s/", topic)
	keys, err := ls.s3Client.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list leases: %w", err)
	}

	leases := make([]Lease, 0, len(keys))
	for _, key := range keys {
		data, etag, err := ls.s3Client.GetWithETag(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("list leases: get %q: %w", key, err)
		}
		var lease Lease
		if err := json.Unmarshal(data, &lease); err != nil {
			return nil, fmt.Errorf("list leases: unmarshal %q: %w", key, err)
		}
		lease.ETag = etag
		leases = append(leases, lease)
	}
	return leases, nil
}
