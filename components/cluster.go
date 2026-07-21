package components

import (
	"context"
	"time"
)

// ExclusiveController is implemented by providers that support cluster exclusive-access leases
// The ClusterAdmin uses it to take exclusive access to a cluster for a maintenance operation such as a data restore
// This isn't supported by the standalone provider
type ExclusiveController interface {
	// AcquireExclusiveLease acquires or re-acquires the cluster exclusive-access lease for owner, extending it to now+ttl
	// It returns ErrExclusiveHeld if a different owner currently holds a live (non-expired) lease
	AcquireExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error)

	// RenewExclusiveLease extends the exclusive-access lease for owner to now+ttl
	// It returns ErrExclusiveHeld if owner no longer holds a live lease, so the caller can treat the lease as lost
	RenewExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error)

	// ReleaseExclusiveLease clears the exclusive-access lease if it is held by owner
	// It is idempotent: releasing a lease this owner does not hold is not an error
	ReleaseExclusiveLease(ctx context.Context, owner string) error
}
