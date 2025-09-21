package host

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

const placementCacheMaxTTL = 5 * time.Second

func (h *Host) lookupActor(parentCtx context.Context, aRef ref.ActorRef, skipCache bool, activeOnly bool) (res *actorPlacement, err error) {
	key := aRef.String()

	// First, check if the actor is active locally
	// This check can be performed in-memory
	_, ok := h.actors.Get(key)
	if ok {
		// Actor is running on the current host, so we can just return that
		// Note that we don't need to do anything with activeOnly, since the actor is definitely active
		return &actorPlacement{
			HostID:  h.hostID,
			Address: h.address,
		}, nil
	}

	// Check if we have a cached response, assuming the caller doesn't want to accept cached data
	if !skipCache {
		res, ok = h.placementCache.Get(key)
		if ok {
			// We have a cached value, so just use that
			// Note that we don't need to do anything with activeOnly, since the actor is likely active
			// If not, the invocation will fail later on
			return res, nil
		}
	}

	// Perform a lookup with the provider
	ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	defer cancel()
	lar, err := h.actorProvider.LookupActor(ctx, aRef, components.LookupActorOpts{
		ActiveOnly: activeOnly,
	})

	switch {
	case errors.Is(err, components.ErrNoHost):
		// Delete from the cache in case it's present
		h.placementCache.Delete(key)
		return nil, actor.ErrActorTypeUnsupported
	case errors.Is(err, components.ErrNoActor) && activeOnly:
		return nil, actor.ErrActorNotActive
	case err != nil:
		// Delete from the cache in case it's present
		h.placementCache.Delete(key)
		return nil, fmt.Errorf("provider returned an error: %w", err)
	}

	res = &actorPlacement{
		HostID:  lar.HostID,
		Address: lar.Address,
	}

	// Save the value in the cache but only up to the idle timeout
	ttl := lar.IdleTimeout
	if ttl == 0 {
		// Make bigger than the max TTL
		ttl = placementCacheMaxTTL + time.Second
	}
	h.placementCache.Set(key, res, ttl)

	return res, nil
}

func (h *Host) isLocal(ap *actorPlacement) bool {
	// If the host ID is different from the current, the invocation is for a remote actor
	return ap.HostID == h.hostID
}

func (h *Host) deactivationTimeoutForActorType(actorType string) time.Duration {
	return h.actorsConfig[actorType].DeactivationTimeout
}

type actorPlacement struct {
	// Host ID
	HostID string
	// Host address (including port)
	Address string
}
