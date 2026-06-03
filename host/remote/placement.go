package remote

import (
	"context"
	"fmt"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

const placementCacheMaxTTL = 5 * time.Second

// actorPlacement is the resolved location of an actor
type actorPlacement struct {
	// Host ID
	HostID string
	// Host peer address (including port)
	Address string
}

// lookupActor resolves where an actor is placed, checking the local host first, then the cache, then the runtime
func (h *Host) lookupActor(parentCtx context.Context, aRef ref.ActorRef, skipCache bool, activeOnly bool) (*actorPlacement, error) {
	key := aRef.String()

	// First, check if the actor is active locally
	// This check can be performed in-memory
	_, ok := h.core.Actors.Get(key)
	if ok {
		// Actor is running on the current host, so we can just return that
		// Note we don't need to do anything with activeOnly, since the actor is definitely active
		return &actorPlacement{
			HostID:  h.HostID(),
			Address: h.address,
		}, nil
	}

	// Check if we have a cached response, assuming the caller doesn't want to skip the cache
	if !skipCache {
		res, ok := h.placementCache.Get(key)
		if ok {
			// We have a cached value, so just use that
			// The owning host enforces active-only on invocation, so a stale placement is rejected there and re-resolved
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
		return nil, actor.ErrActorTypeUnsupported
	case isProtocolErrorCode(err, protocol.ErrCodeActorNotActive):
		return nil, actor.ErrActorNotActive
	case err != nil:
		return nil, fmt.Errorf("runtime returned an error: %w", err)
	}

	res := &actorPlacement{
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

// isLocal reports whether the placement points at the current host
func (h *Host) isLocal(ap *actorPlacement) bool {
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
