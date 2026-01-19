package memory

import (
	"context"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ptr"
	"github.com/italypaleale/francis/internal/ref"
)

func (m *MemoryProvider) GetState(ctx context.Context, r ref.ActorRef) ([]byte, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	state, ok := m.actorState[key]
	if !ok {
		return nil, components.ErrNoState
	}

	// Check expiration
	if state.expiration != nil && m.clock.Now().After(*state.expiration) {
		return nil, components.ErrNoState
	}

	return state.data, nil
}

func (m *MemoryProvider) SetState(ctx context.Context, r ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	key := newActorKey(r.ActorType, r.ActorID)

	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	entry := &stateEntry{
		data: data,
	}

	if opts.TTL > 0 {
		entry.expiration = ptr.Of(m.clock.Now().Add(opts.TTL))
	}

	m.actorState[key] = entry
	return nil
}

func (m *MemoryProvider) DeleteState(ctx context.Context, r ref.ActorRef) error {
	key := newActorKey(r.ActorType, r.ActorID)

	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	state, ok := m.actorState[key]
	if !ok {
		return components.ErrNoState
	}

	// Check if the state is expired (expired state should also return ErrNoState)
	if state.expiration != nil && m.clock.Now().After(*state.expiration) {
		// Still delete it from the map, but return error
		delete(m.actorState, key)
		return components.ErrNoState
	}

	delete(m.actorState, key)
	return nil
}
