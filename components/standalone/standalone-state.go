package standalone

import (
	"context"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ptr"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *provider) GetState(ctx context.Context, r ref.ActorRef) ([]byte, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	p.stateMu.RLock()
	defer p.stateMu.RUnlock()

	state, ok := p.actorState[key]
	if !ok {
		return nil, components.ErrNoState
	}

	// Check expiration
	if state.expiration != nil && p.clock.Now().After(*state.expiration) {
		return nil, components.ErrNoState
	}

	return state.data, nil
}

func (p *provider) SetState(ctx context.Context, r ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	key := newActorKey(r.ActorType, r.ActorID)

	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	// Check if this is an update or create
	_, exists := p.actorState[key]

	entry := &stateEntry{
		data: data,
	}

	if opts.TTL > 0 {
		entry.expiration = ptr.Of(p.clock.Now().Add(opts.TTL))
	}

	p.actorState[key] = entry

	// Persist changes
	changes := newChanges()
	if exists {
		changes.ActorState.Update[key] = entry
	} else {
		changes.ActorState.Create[key] = entry
	}
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		// Rollback
		if exists {
			// We can't easily rollback an update, but at least we tried
		} else {
			delete(p.actorState, key)
		}
		return err
	}

	return nil
}

func (p *provider) DeleteState(ctx context.Context, r ref.ActorRef) error {
	key := newActorKey(r.ActorType, r.ActorID)

	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	state, ok := p.actorState[key]
	if !ok {
		return components.ErrNoState
	}

	// Check if the state is expired (expired state should also return ErrNoState)
	if state.expiration != nil && p.clock.Now().After(*state.expiration) {
		// Still delete it from the map, but return error
		delete(p.actorState, key)

		// Persist the deletion
		changes := newChanges()
		changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		_ = p.persistHook.PersistChanges(ctx, changes) // Best effort

		return components.ErrNoState
	}

	delete(p.actorState, key)

	// Persist changes
	changes := newChanges()
	changes.ActorState.Delete = append(changes.ActorState.Delete, key)
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}
