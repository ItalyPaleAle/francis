package internal

import (
	"context"
	"log/slog"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ptr"
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
		entry.Expiration = ptr.Of(p.Clock.Now().Add(opts.TTL))
	}

	// Persist first, then apply in memory
	changes := NewChanges()
	defer changes.Release()
	changes.ActorState.Set = append(changes.ActorState.Set, ActorStateChange{Key: key, Value: entry})

	return p.persistThenApply(ctx, &p.StateMu, changes, func() {
		p.ActorState[key] = entry
	})
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

	// Expired state is treated as absent: we still remove it (best-effort), but always
	// return ErrNoState
	if expired {
		err := p.persistThenApply(ctx, &p.StateMu, changes, apply)
		if err != nil {
			// Only log the error here: the expired state isn't returned anyway, and the
			// background cleanup will retry the removal later
			p.Log.WarnContext(ctx, "Error while persisting removal of expired state in DeleteState", slog.Any("error", err))
		}
		return components.ErrNoState
	}

	return p.persistThenApply(ctx, &p.StateMu, changes, apply)
}
