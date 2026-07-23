package internal

import (
	"context"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/clusterstate"
)

// The cluster-admission state is kept only in-memory: a standalone deployment runs a single runtime, and its host limit is fixed for the lifetime of that runtime, so there is nothing to persist or coordinate across processes.

// checkClusterAdmission enforces the exclusive-access lease and the host limit before a new host is registered
// It must be called while holding at least a read lock on Mu
// When the host may claim (or re-claim) the cluster's effective host limit, it returns the new cluster state that the caller must apply in memory (otherwise it returns a nil claim)
func (p *Provider) checkClusterAdmission(nowMs int64) (claim *clusterstate.State, err error) {
	// Reject registration while an exclusive-access lease is held
	if p.Cluster.LeaseLive(nowMs) {
		return nil, components.ErrClusterLocked
	}

	// Count the hosts that are currently healthy
	// Unhealthy hosts are cleaned up together with this registration, so they do not count against the limit
	healthy := 0
	for _, h := range p.Hosts {
		if p.IsHostHealthy(h) {
			healthy++
		}
	}

	// Reconcile the configured limit with the cluster's effective limit
	// An unset limit, or an empty cluster, lets this host claim (or re-claim) the limit
	// Otherwise the values must match
	switch {
	case p.Cluster.MaxHosts == nil || healthy == 0:
		v := p.Cfg.MaxHosts
		next := p.Cluster
		next.MaxHosts = &v
		claim = &next
	case *p.Cluster.MaxHosts != p.Cfg.MaxHosts:
		return nil, components.ErrMaxHostsMismatch
	}

	// Enforce the limit, where 0 means unlimited
	if p.Cfg.MaxHosts > 0 && healthy >= p.Cfg.MaxHosts {
		return nil, components.ErrClusterFull
	}

	return claim, nil
}

// AcquireExclusiveLease acquires or re-acquires the cluster exclusive-access lease for owner, extending it to now+ttl
// It returns components.ErrExclusiveHeld if a different owner currently holds a live (non-expired) lease
func (p *Provider) AcquireExclusiveLease(_ context.Context, owner string, ttl time.Duration) (time.Time, error) {
	// writeMu serializes this against host registration, so the lease and the host count cannot change out from under each other
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()
	nowMs := now.UnixMilli()
	expiresAt := now.Add(ttl)

	p.Mu.Lock()
	defer p.Mu.Unlock()

	// The lease may be taken when it is absent, expired, or already owned by this same owner (a re-acquire)
	if p.Cluster.ExclusiveOwner != "" && p.Cluster.ExclusiveOwner != owner && p.Cluster.ExclusiveExpiresAt >= nowMs {
		return time.Time{}, components.ErrExclusiveHeld
	}

	p.Cluster.ExclusiveOwner = owner
	p.Cluster.ExclusiveExpiresAt = expiresAt.UnixMilli()
	return expiresAt, nil
}

// RenewExclusiveLease extends the exclusive-access lease for owner to now+ttl
// It returns components.ErrExclusiveHeld if owner no longer holds a live lease, so the caller can treat the lease as lost
func (p *Provider) RenewExclusiveLease(_ context.Context, owner string, ttl time.Duration) (time.Time, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()
	nowMs := now.UnixMilli()
	expiresAt := now.Add(ttl)

	p.Mu.Lock()
	defer p.Mu.Unlock()

	// Only renew when this owner still holds a live lease
	if p.Cluster.ExclusiveOwner != owner || p.Cluster.ExclusiveExpiresAt < nowMs {
		return time.Time{}, components.ErrExclusiveHeld
	}

	p.Cluster.ExclusiveExpiresAt = expiresAt.UnixMilli()
	return expiresAt, nil
}

// ReleaseExclusiveLease clears the exclusive-access lease if it is held by owner
// It is idempotent: releasing a lease this owner does not hold is not an error
func (p *Provider) ReleaseExclusiveLease(_ context.Context, owner string) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.Lock()
	defer p.Mu.Unlock()

	// Nothing to do if this owner does not hold the lease
	if p.Cluster.ExclusiveOwner != owner {
		return nil
	}

	p.Cluster.ExclusiveOwner = ""
	p.Cluster.ExclusiveExpiresAt = 0
	return nil
}
