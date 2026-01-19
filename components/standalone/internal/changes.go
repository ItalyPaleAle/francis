package internal

import (
	"context"
)

// Changes represents all changes made during a single operation.
// This is passed to the PersistHook to persist changes to the backing store.
type Changes struct {
	Hosts struct {
		Create []*Host
		Update map[string]*Host // host_id -> host
		Delete []string         // host_ids to delete
	}
	HostActorTypes struct {
		Create []*HostActorType
		Delete []HostActorTypeKey // Delete by host_id + actor_type
	}
	ActiveActors struct {
		Create []*ActiveActor
		Update map[ActorKey]*ActiveActor
		Delete []ActorKey
	}
	Alarms struct {
		Create []*Alarm
		Update map[string]*Alarm // alarm_id -> alarm
		Delete []string          // alarm_ids to delete
	}
	ActorState struct {
		Create map[ActorKey]*StateEntry
		Update map[ActorKey]*StateEntry
		Delete []ActorKey
	}
}

// NewChanges creates a new empty Changes instance.
func NewChanges() *Changes {
	return &Changes{
		Hosts: struct {
			Create []*Host
			Update map[string]*Host
			Delete []string
		}{
			Update: make(map[string]*Host),
		},
		HostActorTypes: struct {
			Create []*HostActorType
			Delete []HostActorTypeKey
		}{},
		ActiveActors: struct {
			Create []*ActiveActor
			Update map[ActorKey]*ActiveActor
			Delete []ActorKey
		}{
			Update: make(map[ActorKey]*ActiveActor),
		},
		Alarms: struct {
			Create []*Alarm
			Update map[string]*Alarm
			Delete []string
		}{
			Update: make(map[string]*Alarm),
		},
		ActorState: struct {
			Create map[ActorKey]*StateEntry
			Update map[ActorKey]*StateEntry
			Delete []ActorKey
		}{
			Create: make(map[ActorKey]*StateEntry),
			Update: make(map[ActorKey]*StateEntry),
		},
	}
}

// IsEmpty returns true if no changes have been recorded.
func (c *Changes) IsEmpty() bool {
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
