package local

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
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

// lookupActor resolves where an actor is placed, checking the local host first, then the cache, then the provider
func (h *Host) lookupActor(parentCtx context.Context, aRef ref.ActorRef, skipCache bool, activeOnly bool) (*actorcore.Placement, error) {
	key := aRef.String()

	// First, check if the actor is active locally
	// This check can be performed in-memory
	_, ok := h.core.Actors.Get(key)
	if ok {
		// Actor is running on the current host, so we can just return that
		// Note we don't need to do anything with activeOnly, since the actor is definitely active
		return &actorcore.Placement{
			HostID:  h.hostID,
			Address: h.address,
		}, nil
	}

	// Check if we have a cached response, assuming the caller doesn't want to skip the cache
	// Active-only lookups always consult the provider
	if !skipCache && !activeOnly {
		res, ok := h.placementCache.Get(key)
		if ok {
			// We have a cached value, so just use that
			// The owning host re-confirms ownership before activating, so a stale placement is rejected there and re-resolved
			return res, nil
		}
	}

	// Span the provider lookup, the slow path taken only on a cold or cache-missed placement
	ctx, span := tracing.Start(parentCtx, "placement.lookup",
		trace.WithAttributes(
			tracing.ActorRef(key),
		),
	)
	defer span.End()

	// Perform a lookup with the provider
	ctx, cancel := context.WithTimeout(ctx, h.providerRequestTimeout)
	defer cancel()
	lar, err := h.actorProvider.LookupActor(ctx, aRef, components.LookupActorOpts{
		ActiveOnly: activeOnly,
	})
	switch {
	case errors.Is(err, components.ErrNoHost):
		// Delete from the cache in case it's present
		h.placementCache.Delete(key)
		// No host can currently place the actor, which is distinct from the type being unsupported and may be transient
		return nil, actor.ErrNoHost
	case errors.Is(err, components.ErrNoActor) && activeOnly:
		return nil, actor.ErrActorNotActive
	case err != nil:
		// Delete from the cache in case it's present
		h.placementCache.Delete(key)
		return nil, fmt.Errorf("provider returned an error: %w", err)
	}

	res := &actorcore.Placement{
		HostID:  lar.HostID,
		Address: lar.Address,
	}

	// Save the value in the cache but only up to the idle timeout
	ttl := lar.IdleTimeout
	if ttl <= 0 {
		// A non-positive idle timeout means the actor never idles out, so cache it beyond the max TTL
		ttl = placementCacheMaxTTL + time.Second
	}
	h.placementCache.Set(key, res, ttl)

	return res, nil
}

// confirmLocal authoritatively claims an actor for this host, used on the receiving side of a peer invocation before the actor is activated
// It does a fresh, cache-bypassing lookup through the provider, which atomically resolves or allocates the placement, and rejects the invocation when the actor is owned elsewhere
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
	// If the host ID is different from the current, the invocation is for a remote actor
	return h.hostID != "" && ap.HostID == h.hostID
}

// invalidatePlacement drops any cached placement for an actor so the next lookup re-resolves it
func (h *Host) invalidatePlacement(aRef ref.ActorRef) {
	if h.placementCache != nil {
		h.placementCache.Delete(aRef.String())
	}
}
