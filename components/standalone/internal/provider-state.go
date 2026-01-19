package internal

import (
	"context"
	"fmt"
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

	p.StateMu.Lock()
	defer p.StateMu.Unlock()

	// Check if this is an update or create
	previous, exists := p.ActorState[key]

	entry := &StateEntry{
		Data: data,
	}

	if opts.TTL > 0 {
		entry.Expiration = ptr.Of(p.Clock.Now().Add(opts.TTL))
	}

	p.ActorState[key] = entry

	// Persist changes
	changes := NewChanges()
	if exists {
		changes.ActorState.Update[key] = entry
	} else {
		changes.ActorState.Create[key] = entry
	}
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		if exists {
			p.ActorState[key] = previous
		} else {
			delete(p.ActorState, key)
		}
		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) DeleteState(ctx context.Context, r ref.ActorRef) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.StateMu.Lock()
	defer p.StateMu.Unlock()

	state, ok := p.ActorState[key]
	if !ok {
		return components.ErrNoState
	}

	// Check if the state is expired (expired state should also return ErrNoState)
	if state.IsExpired(p.Clock.Now()) {
		// Still delete it from the map, but return error
		delete(p.ActorState, key)

		// Persist the deletion
		changes := NewChanges()
		changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		err := p.PersistHook.PersistChanges(ctx, changes)
		if err != nil {
			// Only log the error here, the expired state isn't returned anyways
			p.Log.WarnContext(ctx, "Error while persisting removal of expired state in DeleteState", slog.Any("error", err))
		}

		return components.ErrNoState
	}

	delete(p.ActorState, key)

	// Persist changes
	changes := NewChanges()
	changes.ActorState.Delete = append(changes.ActorState.Delete, key)
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		p.ActorState[key] = state

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}
