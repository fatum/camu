package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const leaderKey = "_coordination/leader.json"

// LeaderLease represents the current leader's lease.
type LeaderLease struct {
	InstanceID string    `json:"instance_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	ETag       string    `json:"-"`
}

// LeaderElection manages leader election via S3 ConditionalPut.
type LeaderElection struct {
	s3Client   *storage.S3Client
	instanceID string
	ttl        time.Duration
}

// NewLeaderElection creates a new LeaderElection.
func NewLeaderElection(s3 *storage.S3Client, instanceID string, ttl time.Duration) *LeaderElection {
	return &LeaderElection{
		s3Client:   s3,
		instanceID: instanceID,
		ttl:        ttl,
	}
}

// TryAcquire attempts to become leader. Returns (lease, true, nil) if this
// instance is now the leader, or (lease, false, nil) if another instance holds
// a valid lease.
func (le *LeaderElection) TryAcquire(ctx context.Context) (LeaderLease, bool, error) {
	data, etag, err := le.s3Client.GetWithETag(ctx, leaderKey)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return LeaderLease{}, false, fmt.Errorf("leader: get: %w", err)
	}

	var existingETag string

	if err == nil {
		// Lease file exists — check if still valid.
		var existing LeaderLease
		if jsonErr := json.Unmarshal(data, &existing); jsonErr != nil {
			return LeaderLease{}, false, fmt.Errorf("leader: unmarshal: %w", jsonErr)
		}
		if time.Now().Before(existing.ExpiresAt) {
			if existing.InstanceID == le.instanceID {
				// We are already the leader.
				existing.ETag = etag
				return existing, true, nil
			}
			// Another instance is the leader.
			existing.ETag = etag
			return existing, false, nil
		}
		// Lease expired — try to take over.
		existingETag = etag
	}
	// No lease or expired lease — try to acquire.

	newLease := LeaderLease{
		InstanceID: le.instanceID,
		ExpiresAt:  time.Now().Add(le.ttl),
	}
	encoded, err := json.Marshal(newLease)
	if err != nil {
		return LeaderLease{}, false, fmt.Errorf("leader: marshal: %w", err)
	}

	newETag, err := le.s3Client.ConditionalPut(ctx, leaderKey, encoded, existingETag)
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			// Another instance won the race — read current leader.
			lease, getErr := le.GetLeader(ctx)
			if getErr != nil {
				return LeaderLease{}, false, fmt.Errorf("leader: read after conflict: %w", getErr)
			}
			return lease, false, nil
		}
		return LeaderLease{}, false, fmt.Errorf("leader: conditional put: %w", err)
	}

	newLease.ETag = newETag
	return newLease, true, nil
}

// Renew extends the leader lease TTL. Only works if this instance is the
// current leader (verified via ETag).
func (le *LeaderElection) Renew(ctx context.Context, lease LeaderLease) (LeaderLease, error) {
	renewed := LeaderLease{
		InstanceID: le.instanceID,
		ExpiresAt:  time.Now().Add(le.ttl),
	}
	encoded, err := json.Marshal(renewed)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("leader: renew marshal: %w", err)
	}

	newETag, err := le.s3Client.ConditionalPut(ctx, leaderKey, encoded, lease.ETag)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("leader: renew: %w", err)
	}

	renewed.ETag = newETag
	return renewed, nil
}

// GetLeader returns the current leader lease.
func (le *LeaderElection) GetLeader(ctx context.Context) (LeaderLease, error) {
	data, etag, err := le.s3Client.GetWithETag(ctx, leaderKey)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("leader: get: %w", err)
	}
	var lease LeaderLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return LeaderLease{}, fmt.Errorf("leader: unmarshal: %w", err)
	}
	lease.ETag = etag
	return lease, nil
}
