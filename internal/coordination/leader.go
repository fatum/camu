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

// ErrLeaseFenced is returned by Renew when the stored lease has advanced past
// the caller's held lease epoch. The caller is a stale leader and must stop
// acting as the cluster controller regardless of wall-clock lease expiry.
var ErrLeaseFenced = errors.New("leader lease fenced by a higher-epoch holder")

// LeaderLease represents the current leader's lease.
type LeaderLease struct {
	InstanceID string    `json:"instance_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	// LeaseEpoch is a monotonic generation counter of the lease object. It is
	// bumped on every successful acquire or renew, so the highest epoch is
	// always the most recent writer. Fencing decisions compare epochs first;
	// wall-clock expiry only bounds renewal.
	LeaseEpoch uint64 `json:"lease_epoch"`
	ETag       string `json:"-"`
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
	var nextEpoch uint64

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
		// Lease expired — try to take over, bumping past the prior generation.
		existingETag = etag
		nextEpoch = existing.LeaseEpoch + 1
	}
	// No lease or expired lease — try to acquire.

	newLease := LeaderLease{
		InstanceID: le.instanceID,
		ExpiresAt:  time.Now().Add(le.ttl),
		LeaseEpoch: nextEpoch,
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
// current leader (verified via ETag). As defense-in-depth beyond the CAS, Renew
// re-reads the stored lease and rejects with ErrLeaseFenced when the stored
// lease has advanced past the caller's held epoch. The stored lease is only
// fencing when it belongs to a DIFFERENT instance: an epoch bump recorded by
// this instance's own prior renew (whose response was lost) is our own advance,
// not another holder taking over, so it must not trigger a spurious handoff.
//
// When the CAS fails because this instance's own prior renew already advanced
// the stored lease (our held ETag is stale but the stored lease is still ours),
// Renew adopts the stored lease and returns it as the current lease instead of
// failing: ceding the controller lease over a lost response would cause a
// spurious handoff, and the next renew will then proceed from the adopted
// epoch.
func (le *LeaderElection) Renew(ctx context.Context, lease LeaderLease) (LeaderLease, error) {
	cur, _, err := le.s3Client.GetWithETag(ctx, leaderKey)
	if err == nil {
		var curLease LeaderLease
		if jsonErr := json.Unmarshal(cur, &curLease); jsonErr == nil &&
			curLease.InstanceID != le.instanceID && curLease.LeaseEpoch > lease.LeaseEpoch {
			return LeaderLease{}, fmt.Errorf("%w: stored epoch %d, held epoch %d", ErrLeaseFenced, curLease.LeaseEpoch, lease.LeaseEpoch)
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return LeaderLease{}, fmt.Errorf("leader: renew get: %w", err)
	}

	renewed := LeaderLease{
		InstanceID: le.instanceID,
		ExpiresAt:  time.Now().Add(le.ttl),
		LeaseEpoch: lease.LeaseEpoch + 1,
	}
	encoded, err := json.Marshal(renewed)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("leader: renew marshal: %w", err)
	}

	newETag, err := le.s3Client.ConditionalPut(ctx, leaderKey, encoded, lease.ETag)
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			// The CAS failed because the stored object changed since our held
			// read. If the change was our own prior renew (same instance, same
			// instance id), adopt the stored lease rather than ceding
			// leadership: the response to that renew was simply lost, and a
			// handoff here would be spurious.
			if adopted, ok := le.adoptOwnLease(ctx); ok {
				return adopted, nil
			}
			return LeaderLease{}, fmt.Errorf("leader: renew: %w", err)
		}
		return LeaderLease{}, fmt.Errorf("leader: renew: %w", err)
	}

	renewed.ETag = newETag
	return renewed, nil
}

// adoptOwnLease re-reads the stored lease after a renew CAS conflict and
// reports whether it belongs to this instance. When it does, it returns the
// stored lease (with its ETag) so the caller can continue renewing from the
// adopted epoch instead of spuriously ceding the controller lease.
func (le *LeaderElection) adoptOwnLease(ctx context.Context) (LeaderLease, bool) {
	cur, etag, err := le.s3Client.GetWithETag(ctx, leaderKey)
	if err != nil {
		return LeaderLease{}, false
	}
	var curLease LeaderLease
	if err := json.Unmarshal(cur, &curLease); err != nil {
		return LeaderLease{}, false
	}
	if curLease.InstanceID != le.instanceID {
		return LeaderLease{}, false
	}
	curLease.ETag = etag
	return curLease, true
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
