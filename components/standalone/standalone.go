// Package standalone provides in-memory actor providers with optional persistence.
//
// The standalone providers keep all data in memory for fast access, but can optionally persist changes to a backing store (SQLite or PostgreSQL) for durability across restarts.
//
// Available providers:
//   - StandaloneMemory: Pure in-memory, no persistence
//   - StandaloneSQLiteBacked: In-memory with SQLite persistence
//   - StandalonePostgresBacked: In-memory with PostgreSQL persistence
//
// These providers are designed for single-instance deployments. For multi-instance eployments with coordination, use the sqlite or postgres packages instead.
package standalone

import (
	"context"
)

// changes represents all changes made during a single operation.
// This is passed to the PersistHook to persist changes to the backing store.
type changes struct {
	Hosts struct {
		Create []*host
		Update map[string]*host // host_id -> host
		Delete []string         // host_ids to delete
	}
	HostActorTypes struct {
		Create []*hostActorType
		Delete []hostActorTypeKey // Delete by host_id + actor_type
	}
	ActiveActors struct {
		Create []*activeActor
		Update map[actorKey]*activeActor
		Delete []actorKey
	}
	Alarms struct {
		Create []*alarm
		Update map[string]*alarm // alarm_id -> alarm
		Delete []string          // alarm_ids to delete
	}
	ActorState struct {
		Create map[actorKey]*stateEntry
		Update map[actorKey]*stateEntry
		Delete []actorKey
	}
}

// newChanges creates a new empty Changes instance.
func newChanges() *changes {
	return &changes{
		Hosts: struct {
			Create []*host
			Update map[string]*host
			Delete []string
		}{
			Update: make(map[string]*host),
		},
		HostActorTypes: struct {
			Create []*hostActorType
			Delete []hostActorTypeKey
		}{},
		ActiveActors: struct {
			Create []*activeActor
			Update map[actorKey]*activeActor
			Delete []actorKey
		}{
			Update: make(map[actorKey]*activeActor),
		},
		Alarms: struct {
			Create []*alarm
			Update map[string]*alarm
			Delete []string
		}{
			Update: make(map[string]*alarm),
		},
		ActorState: struct {
			Create map[actorKey]*stateEntry
			Update map[actorKey]*stateEntry
			Delete []actorKey
		}{
			Create: make(map[actorKey]*stateEntry),
			Update: make(map[actorKey]*stateEntry),
		},
	}
}

// isEmpty returns true if no changes have been recorded.
func (c *changes) isEmpty() bool {
	return len(c.Hosts.Create) == 0 &&
		len(c.Hosts.Update) == 0 &&
		len(c.Hosts.Delete) == 0 &&
		len(c.HostActorTypes.Create) == 0 &&
		len(c.HostActorTypes.Delete) == 0 &&
		len(c.ActiveActors.Create) == 0 &&
		len(c.ActiveActors.Update) == 0 &&
		len(c.ActiveActors.Delete) == 0 &&
		len(c.Alarms.Create) == 0 &&
		len(c.Alarms.Update) == 0 &&
		len(c.Alarms.Delete) == 0 &&
		len(c.ActorState.Create) == 0 &&
		len(c.ActorState.Update) == 0 &&
		len(c.ActorState.Delete) == 0
}

// persistHook is called after each operation to persist changes to the backing store.
type persistHook interface {
	// PersistChanges persists the given changes to the backing store.
	// The implementation should use a transaction to ensure atomicity.
	// Returns an error if persistence fails (caller should rollback in-memory changes).
	PersistChanges(ctx context.Context, changes *changes) error
}

// noopPersistHook is a no-op implementation used by StandaloneMemory.
type noopPersistHook struct{}

func (n *noopPersistHook) PersistChanges(ctx context.Context, changes *changes) error {
	return nil
}
