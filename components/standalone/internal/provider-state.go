package internal

import (
	"bytes"
	"context"
	"log/slog"
	"slices"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) GetState(ctx context.Context, r ref.ActorRef) ([]byte, error) {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	state, ok := p.ActorState[key]
	if !ok {
		return nil, components.ErrNoState
	}

	// Check expiration
	if state.IsExpired(p.Clock.Now()) {
		return nil, components.ErrNoState
	}

	return state.Data, nil
}

func (p *Provider) SetState(ctx context.Context, r ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.stateWriteMu.Lock()
	defer p.stateWriteMu.Unlock()

	entry := &StateEntry{
		Data: data,
	}
	if opts.TTL > 0 {
		entry.Expiration = new(p.Clock.Now().Add(opts.TTL))
	}

	// Persist first, then apply in memory
	changes := NewChanges()
	defer changes.Release()
	changes.ActorState.Set = append(changes.ActorState.Set, ActorStateChange{Key: key, Value: entry})

	return p.persistThenApply(ctx, &p.StateMu, changes, func() {
		p.ActorState[key] = entry
	})
}

func (p *Provider) ListStates(ctx context.Context, req components.ListStatesReq) (components.ListStatesRes, error) {
	limit := req.EffectiveLimit()
	now := p.Clock.Now()

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	// Collect the actor IDs that match the type and cursor, skipping expired state so the listing agrees with GetState even before the background cleanup runs
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	matches := make([]string, 0, len(p.ActorState))
	for key, state := range p.ActorState {
		if key.ActorType != req.ActorType || state.IsExpired(now) {
			continue
		}

		if key.ActorID <= req.After {
			continue
		}

		matches = append(matches, key.ActorID)
	}

	// The map has no order of its own, so the ascending order the API promises has to be established here
	slices.Sort(matches)

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	hasMore := len(matches) > limit
	if hasMore {
		matches = matches[:limit]
	}

	res := components.ListStatesRes{
		States:  make([]components.ActorStateInfo, len(matches)),
		HasMore: hasMore,
	}
	for i, actorID := range matches {
		res.States[i] = components.ActorStateInfo{
			ActorID: actorID,
		}

		// The data is cloned because the entry stays live in the map, where a concurrent SetState could otherwise hand the caller a shared slice
		if req.IncludeData {
			res.States[i].Data = bytes.Clone(p.ActorState[NewActorKey(req.ActorType, actorID)].Data)
		}
	}

	return res, nil
}

func (p *Provider) DeleteState(ctx context.Context, r ref.ActorRef) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.stateWriteMu.Lock()
	defer p.stateWriteMu.Unlock()

	p.StateMu.RLock()
	state, ok := p.ActorState[key]
	expired := ok && state.IsExpired(p.Clock.Now())
	p.StateMu.RUnlock()

	if !ok {
		return components.ErrNoState
	}

	changes := NewChanges()
	defer changes.Release()
	changes.ActorState.Delete = append(changes.ActorState.Delete, key)

	apply := func() {
		delete(p.ActorState, key)
	}

	// Expired state is treated as absent
	// We still remove it (best-effort), but always return ErrNoState
	if expired {
		err := p.persistThenApply(ctx, &p.StateMu, changes, apply)
		if err != nil {
			// Only log the error here: the expired state isn't returned anyway, and the background cleanup will retry the removal later
			p.Log.WarnContext(ctx, "Error while persisting removal of expired state in DeleteState", slog.Any("error", err))
		}
		return components.ErrNoState
	}

	return p.persistThenApply(ctx, &p.StateMu, changes, apply)
}
