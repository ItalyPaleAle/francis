package internal

import (
	"context"
	"sync"
)

// changesPool is a pool of *Changes objects to reduce allocations.
var changesPool = sync.Pool{
	New: func() any {
		return &Changes{}
	},
}

// HostChange represents a host to be set (upserted).
type HostChange struct {
	Key   string // host_id
	Value *Host
}

// ActiveActorChange represents an active actor to be set (upserted).
type ActiveActorChange struct {
	Key   ActorKey
	Value *ActiveActor
}

// AlarmChange represents an alarm to be set (upserted).
type AlarmChange struct {
	Key   string // alarm_id
	Value *Alarm
}

// ActorStateChange represents actor state to be set (upserted).
type ActorStateChange struct {
	Key   ActorKey
	Value *StateEntry
}

// Changes represents all changes made during a single operation.
// This is passed to the PersistHook to persist changes to the backing store.
// Set operations perform upserts (insert or update).
type Changes struct {
	Hosts struct {
		Set    []HostChange // Upsert hosts
		Delete []string     // host_ids to delete
	}
	HostActorTypes struct {
		Set    []*HostActorType   // Upsert host actor types
		Delete []HostActorTypeKey // Delete by host_id + actor_type
	}
	ActiveActors struct {
		Set    []ActiveActorChange // Upsert active actors
		Delete []ActorKey
	}
	Alarms struct {
		Set    []AlarmChange // Upsert alarms
		Delete []string      // alarm_ids to delete
	}
	ActorState struct {
		Set    []ActorStateChange // Upsert actor state
		Delete []ActorKey
	}
}

// NewChanges returns a Changes instance from the pool.
// Call Release() when done to return it to the pool.
func NewChanges() *Changes {
	//nolint:forcetypeassert
	return changesPool.Get().(*Changes)
}

// Release returns the Changes to the pool after resetting all slices.
// The Changes object should not be used after calling Release.
func (c *Changes) Release() {
	// Reset all slices to zero length but keep capacity
	c.Hosts.Set = c.Hosts.Set[:0]
	c.Hosts.Delete = c.Hosts.Delete[:0]
	c.HostActorTypes.Set = c.HostActorTypes.Set[:0]
	c.HostActorTypes.Delete = c.HostActorTypes.Delete[:0]
	c.ActiveActors.Set = c.ActiveActors.Set[:0]
	c.ActiveActors.Delete = c.ActiveActors.Delete[:0]
	c.Alarms.Set = c.Alarms.Set[:0]
	c.Alarms.Delete = c.Alarms.Delete[:0]
	c.ActorState.Set = c.ActorState.Set[:0]
	c.ActorState.Delete = c.ActorState.Delete[:0]

	changesPool.Put(c)
}

// IsEmpty returns true if no changes have been recorded.
func (c *Changes) IsEmpty() bool {
	return len(c.Hosts.Set) == 0 &&
		len(c.Hosts.Delete) == 0 &&
		len(c.HostActorTypes.Set) == 0 &&
		len(c.HostActorTypes.Delete) == 0 &&
		len(c.ActiveActors.Set) == 0 &&
		len(c.ActiveActors.Delete) == 0 &&
		len(c.Alarms.Set) == 0 &&
		len(c.Alarms.Delete) == 0 &&
		len(c.ActorState.Set) == 0 &&
		len(c.ActorState.Delete) == 0
}

// PersistHook is called after each operation to persist changes to the backing store.
type PersistHook interface {
	// PersistChanges persists the given changes to the backing store.
	// The implementation should use a transaction to ensure atomicity.
	// Returns an error if persistence fails (caller should rollback in-memory changes).
	PersistChanges(ctx context.Context, changes *Changes) error
}

// NoopPersistHook is a no-op implementation used by StandaloneMemory.
type NoopPersistHook struct{}

func (n *NoopPersistHook) PersistChanges(ctx context.Context, changes *Changes) error {
	return nil
}
