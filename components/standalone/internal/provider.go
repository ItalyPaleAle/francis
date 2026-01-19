package internal

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
)

// Provider is the internal core provider.
// It stores all data in memory and calls the PersistHook for each mutation.
type Provider struct {
	Cfg             components.ProviderConfig
	Running         atomic.Bool
	Log             *slog.Logger
	Clock           clock.WithTicker
	CleanupInterval time.Duration
	Mu              sync.RWMutex // Lock for Hosts, HostsByAddress, HostActorTypes, ActiveActors, Alarms, AlarmsByID
	StateMu         sync.RWMutex // Lock for ActorState

	// Persistence hook
	PersistHook PersistHook

	// In-memory data
	Hosts          map[string]*Host            // host_id -> host
	HostsByAddress map[string]string           // address -> host_id
	HostActorTypes map[string][]*HostActorType // host_id -> actor types
	ActiveActors   map[ActorKey]*ActiveActor   // actor_type/actor_id -> active actor
	Alarms         map[AlarmKey]*Alarm         // actor_type/actor_id/alarm_name -> alarm
	AlarmsByID     map[string]*Alarm           // alarm_id -> alarm
	ActorState     map[ActorKey]*StateEntry    // actor_type/actor_id -> state
}

// DefaultCleanupInterval is the default interval at which the provider purges expired state.
const DefaultCleanupInterval = 5 * time.Minute

// ProviderOptions contains options for creating the internal provider.
type ProviderOptions struct {
	components.ProviderOptions

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration

	// PersistHook is called after each mutation to persist changes.
	PersistHook PersistHook
}

// NewProvider creates a new internal provider.
func NewProvider(log *slog.Logger, opts ProviderOptions, providerConfig components.ProviderConfig) (*Provider, error) {
	err := providerConfig.Validate()
	if err != nil {
		return nil, err
	}

	cleanupInterval := opts.CleanupInterval
	if cleanupInterval == 0 {
		// Zero value means default
		cleanupInterval = DefaultCleanupInterval
	} else if cleanupInterval < 0 {
		// Negative value disables cleanup
		cleanupInterval = 0
	}

	persistHook := opts.PersistHook
	if persistHook == nil {
		persistHook = &NoopPersistHook{}
	}

	p := &Provider{
		Cfg:             providerConfig,
		Log:             log,
		Clock:           opts.Clock,
		CleanupInterval: cleanupInterval,
		PersistHook:     persistHook,

		Hosts:          make(map[string]*Host),
		HostsByAddress: make(map[string]string),
		HostActorTypes: make(map[string][]*HostActorType),
		ActiveActors:   make(map[ActorKey]*ActiveActor),
		Alarms:         make(map[AlarmKey]*Alarm),
		AlarmsByID:     make(map[string]*Alarm),
		ActorState:     make(map[ActorKey]*StateEntry),
	}

	if p.Clock == nil {
		p.Clock = clock.RealClock{}
	}

	return p, nil
}

func (p *Provider) Init(ctx context.Context) error {
	// Nothing to initialize for the base provider
	return nil
}

func (p *Provider) Run(ctx context.Context) error {
	if !p.Running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	// Start the cleanup loop if enabled
	if p.CleanupInterval > 0 {
		go p.stateCleanupLoop(ctx)
	}

	// Wait for the context to be canceled
	<-ctx.Done()

	return nil
}

// stateCleanupLoop periodically purges expired state from memory.
func (p *Provider) stateCleanupLoop(ctx context.Context) {
	ticker := p.Clock.NewTicker(p.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			p.runCleanup(ctx)
		}
	}
}

// runCleanup runs the cleanup for unhealthy hosts and expired state.
func (p *Provider) runCleanup(ctx context.Context) {
	// Clean up unhealthy hosts
	p.Mu.Lock()
	changes := NewChanges()
	defer changes.Release()
	rollback := p.CleanupUnhealthyHosts(changes)
	if !changes.IsEmpty() {
		err := p.PersistHook.PersistChanges(ctx, changes)
		if err != nil {
			p.Log.Error("Failed to persist cleanup changes", "error", err)
			// Rollback in-memory changes to stay consistent with database
			rollback()
		}
	}
	p.Mu.Unlock()

	// Clean up expired state
	p.StateMu.Lock()
	stateChanges := NewChanges()
	defer stateChanges.Release()
	stateRollback := p.CleanupExpiredState(stateChanges)
	if !stateChanges.IsEmpty() {
		err := p.PersistHook.PersistChanges(ctx, stateChanges)
		if err != nil {
			p.Log.Error("Failed to persist state cleanup changes", "error", err)
			// Rollback in-memory changes to stay consistent with database
			stateRollback()
		}
	}
	p.StateMu.Unlock()
}

func (p *Provider) HealthCheckInterval() time.Duration {
	// Use a simple interval: half of the health check deadline, with a minimum of 1s
	return max(p.Cfg.HostHealthCheckDeadline/2, time.Second)
}

func (p *Provider) RenewLeaseInterval() time.Duration {
	// The recommended interval is the bigger of: the lease duration less 10s, or half of the lease duration
	if p.Cfg.AlarmsLeaseDuration < 20*time.Second {
		return p.Cfg.AlarmsLeaseDuration / 2
	}

	return p.Cfg.AlarmsLeaseDuration - 10*time.Second
}

// IsHostHealthy checks if a host is healthy based on its last health check time.
// Must be called while holding at least a read lock.
func (p *Provider) IsHostHealthy(h *Host) bool {
	return p.Clock.Since(h.LastHealthCheck) < p.Cfg.HostHealthCheckDeadline
}

// CleanupUnhealthyHosts removes unhealthy hosts and their associated data, recording the changes for persistence.
// Returns a rollback function that restores the in-memory state if persistence fails.
// Must be called while holding a write lock.
func (p *Provider) CleanupUnhealthyHosts(changes *Changes) (rollback func()) {
	// Track deleted data for rollback
	rollbackHosts := make(map[string]*Host)
	rollbackHostAddresses := make(map[string]string) // address -> host_id
	rollbackHostActorTypes := make(map[string][]*HostActorType)
	rollbackActiveActors := make(map[ActorKey]*ActiveActor)

	for id, h := range p.Hosts {
		if p.IsHostHealthy(h) {
			continue
		}

		// Track for rollback
		rollbackHosts[id] = h
		rollbackHostAddresses[h.Address] = id

		delete(p.HostsByAddress, h.Address)
		delete(p.Hosts, id)
		changes.Hosts.Delete = append(changes.Hosts.Delete, id)

		// Delete host actor types
		rollbackHostActorTypes[id] = p.HostActorTypes[id]
		for _, hat := range p.HostActorTypes[id] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}
		delete(p.HostActorTypes, id)

		// Remove active actors on this host
		for key, actor := range p.ActiveActors {
			if actor.HostID != id {
				continue
			}

			rollbackActiveActors[key] = actor
			delete(p.ActiveActors, key)
			changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
		}
	}

	return func() {
		// Restore hosts
		maps.Copy(p.Hosts, rollbackHosts)
		maps.Copy(p.HostsByAddress, rollbackHostAddresses)
		// Restore host actor types
		maps.Copy(p.HostActorTypes, rollbackHostActorTypes)
		// Restore active actors
		maps.Copy(p.ActiveActors, rollbackActiveActors)
	}
}

// CleanupExpiredState removes state that has expired, recording the changes.
// Returns a rollback function that restores the in-memory state if persistence fails.
// Must be called while holding a write lock on StateMu.
func (p *Provider) CleanupExpiredState(changes *Changes) (rollback func()) {
	now := p.Clock.Now()

	// Track deleted state for rollback
	rollbackState := make(map[ActorKey]*StateEntry)

	for key, state := range p.ActorState {
		if state.IsExpired(now) {
			rollbackState[key] = state
			delete(p.ActorState, key)
			changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		}
	}

	return func() {
		maps.Copy(p.ActorState, rollbackState)
	}
}

// CountActiveActorsOnHost counts actors of a specific type on a host.
// Must be called while holding at least a read lock.
func (p *Provider) CountActiveActorsOnHost(hostID, actorType string) int {
	count := 0
	for _, actor := range p.ActiveActors {
		if actor.HostID == hostID && actor.ActorType == actorType {
			count++
		}
	}
	return count
}
