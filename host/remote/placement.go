package remote

import (
	"context"
	"fmt"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

const placementCacheMaxTTL = 5 * time.Second

// placementResolver adapts the host to the actorcore.PlacementResolver interface that the shared messaging logic depends on
type placementResolver struct {
	h *Host
}

func (r placementResolver) Resolve(ctx context.Context, aRef ref.ActorRef, skipCache bool, activeOnly bool) (*actorcore.Placement, error) {
	return r.h.lookupActor(ctx, aRef, skipCache, activeOnly)
}

func (r placementResolver) ConfirmLocal(ctx context.Context, aRef ref.ActorRef) error {
	return r.h.confirmLocal(ctx, aRef)
}

func (r placementResolver) Invalidate(aRef ref.ActorRef) {
	r.h.invalidatePlacement(aRef)
}

func (r placementResolver) IsLocal(p *actorcore.Placement) bool {
	return r.h.isLocal(p)
}

// lookupActor resolves where an actor is placed, checking the local host first, then the cache, then the runtime
func (h *Host) lookupActor(parentCtx context.Context, aRef ref.ActorRef, skipCache bool, activeOnly bool) (*actorcore.Placement, error) {
	key := aRef.String()

	// First, check if the actor is active locally
	// This check can be performed in-memory
	_, ok := h.core.Actors.Get(key)
	if ok {
		// Actor is running on the current host, so we can just return that
		// Note we don't need to do anything with activeOnly, since the actor is definitely active
		return &actorcore.Placement{
			HostID:  h.HostID(),
			Address: h.address,
		}, nil
	}

	// Check if we have a cached response, assuming the caller doesn't want to skip the cache
	// Active-only lookups always consult the runtime
	if !skipCache && !activeOnly {
		res, ok := h.placementCache.Get(key)
		if ok {
			// We have a cached value, so just use that
			// The owning host re-confirms ownership before activating, so a stale placement is rejected there and re-resolved
			return res, nil
		}
	}

	// Resolve the placement through the runtime
	ctx, cancel := context.WithTimeout(parentCtx, h.requestTimeout)
	defer cancel()
	resp, err := h.runtimeClient.LookupActor(ctx, protocol.LookupActorRequest{
		ActorType:  aRef.ActorType,
		ActorID:    aRef.ActorID,
		ActiveOnly: activeOnly,
		SkipCache:  skipCache,
	})
	// Translate the well-known placement errors to their public actor equivalents
	switch {
	case isProtocolErrorCode(err, protocol.ErrCodeNoHost):
		// No host can currently place the actor, which is distinct from the type being unsupported and may be transient
		return nil, actor.ErrNoHost
	case isProtocolErrorCode(err, protocol.ErrCodeActorNotActive):
		return nil, actor.ErrActorNotActive
	case err != nil:
		return nil, fmt.Errorf("runtime returned an error: %w", err)
	}

	res := &actorcore.Placement{
		HostID:  resp.HostID,
		Address: resp.Address,
	}

	// Cache the placement, but only up to the actor type's idle timeout
	ttl := time.Duration(resp.IdleTimeoutMs) * time.Millisecond
	if ttl <= 0 {
		// Make bigger than the max TTL
		ttl = placementCacheMaxTTL + time.Second
	}
	h.placementCache.Set(key, res, ttl)

	return res, nil
}

// confirmLocal authoritatively claims an actor for this host, used on the receiving side of a peer invocation before the actor is activated
// It does a fresh, cache-bypassing lookup through the runtime, which atomically resolves or allocates the placement, and rejects the invocation when the actor is owned elsewhere
// This keeps placement provider-authoritative even when the caller routed here from a stale cached placement
func (h *Host) confirmLocal(parentCtx context.Context, aRef ref.ActorRef) error {
	ap, err := h.lookupActor(parentCtx, aRef, true, false)
	if err != nil {
		return err
	}
	if !h.isLocal(ap) {
		// The actor is owned by another host, so the caller must re-resolve and retry
		return actor.ErrActorNotHosted
	}
	return nil
}

// isLocal reports whether the placement points at the current host
func (h *Host) isLocal(ap *actorcore.Placement) bool {
	hostID := h.HostID()
	// Before registration the host ID is empty, in which case nothing is local
	return hostID != "" && ap.HostID == hostID
}

// invalidatePlacement drops any cached placement for an actor so the next lookup re-resolves it
func (h *Host) invalidatePlacement(aRef ref.ActorRef) {
	if h.placementCache != nil {
		h.placementCache.Delete(aRef.String())
	}
}

// invalidateAllPlacements drops every cached placement so subsequent lookups re-resolve through the runtime
func (h *Host) invalidateAllPlacements() {
	if h.placementCache != nil {
		h.placementCache.Reset()
	}
}
