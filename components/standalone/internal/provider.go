package internal

import (
	"context"
	"fmt"
	"log/slog"
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

	// writeMu and stateWriteMu serialize writers (mutations):
	// - writeMu for the Mu domain (hosts/actors/alarms)
	// - stateWriteMu for the StateMu domain (state)
	// Writers hold these for the whole "compute change set -> persist -> apply" sequence
	// This keeps the RWMutex (Mu/StateMu) off the persistence I/O path so readers can proceed while a write is being persisted, while still guaranteeing that no other writer can alter the state being persisted
	writeMu      sync.Mutex
	stateWriteMu sync.Mutex

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

	// Interval at which to purge expired state from memory
	// Default is 5 minutes
	// Set to a negative value to disable
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
	defer p.Running.Store(false)

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

// runCleanup runs the cleanup for unhealthy hosts and expired state
func (p *Provider) runCleanup(ctx context.Context) {
	err := p.CleanupExpired(ctx)
	if err != nil {
		// Errors are logged
		p.Log.Error("Failed to perform cleanup", "error", err)
	}
}

// CleanupExpired performs garbage collection of unhealthy hosts and expired state, persisting the changes before applying them in memory
//
// Unlike the per-request operations, CleanupExpired holds the map lock across the whole compute+persist+apply sequence (rather than releasing it during persistence)
// This keeps the snapshot it computed valid through apply, so it cannot act on stale data
// The latency-sensitive per-request operations still keep persistence off the lock
func (p *Provider) CleanupExpired(ctx context.Context) error {
	// Clean up unhealthy hosts (Mu domain)
	err := p.cleanupDomain(ctx, &p.writeMu, &p.Mu, p.CleanupUnhealthyHosts)
	if err != nil {
		return fmt.Errorf("failed to clean up unhealthy hosts: %w", err)
	}

	// Clean up expired state (StateMu domain)
	err = p.cleanupDomain(ctx, &p.stateWriteMu, &p.StateMu, p.CleanupExpiredState)
	if err != nil {
		return fmt.Errorf("failed to clean up expired state: %w", err)
	}

	return nil
}

// cleanupDomain computes, persists, and applies a cleanup change set atomically while holding the domain's write mutex and map lock
func (p *Provider) cleanupDomain(ctx context.Context, writeMu *sync.Mutex, lock *sync.RWMutex, compute func(changes *Changes) func()) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	lock.Lock()
	defer lock.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// Compute the changes to apply
	// This is an in-memory operation
	apply := compute(changes)
	if changes.IsEmpty() {
		return nil
	}

	// Persist the changes to the DB
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		return fmt.Errorf("error persisting changes: %w", err)
	}

	// Apply the changes in-memory
	// This can't fail
	apply()

	return nil
}

// persistThenApply persists the change set and, only if persistence succeeds, applies the in-memory mutation under the given lock
//
// This implements the "persist before apply" strategy: the backing store is updated first, and memory is touched only after a successful commit
// As a result the in-memory state and the backing store cannot diverge, and there is nothing to roll back on failure
//
// The caller must hold the domain write mutex (writeMu or stateWriteMu) for the whole compute+persist+apply sequence, and must not be holding lock when calling this
// apply must not fail and is a no-op when there are no changes
func (p *Provider) persistThenApply(ctx context.Context, lock *sync.RWMutex, changes *Changes, apply func()) error {
	if changes.IsEmpty() {
		return nil
	}

	// Persist changes in the DB
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		return fmt.Errorf("error persisting changes: %w", err)
	}

	// Apply the changes in-memory
	lock.Lock()
	// This can't fail
	apply()
	lock.Unlock()

	return nil
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

// CleanupUnhealthyHosts computes the changes needed to remove unhealthy hosts and their
// associated data, recording them in changes
// It returns an apply function that performs the in-memory deletions
// Must be called while holding at least a read lock on Mu
// apply must be called while holding the Mu write lock (after the changes have been persisted)
func (p *Provider) CleanupUnhealthyHosts(changes *Changes) (apply func()) {
	var (
		deleteHostIDs   []string
		deleteAddresses []string
		deleteActors    []ActorKey
	)

	for id, h := range p.Hosts {
		if p.IsHostHealthy(h) {
			continue
		}

		deleteHostIDs = append(deleteHostIDs, id)
		deleteAddresses = append(deleteAddresses, h.Address)
		changes.Hosts.Delete = append(changes.Hosts.Delete, id)

		// Delete host actor types
		for _, hat := range p.HostActorTypes[id] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}

		// Remove active actors on this host
		for key, actor := range p.ActiveActors {
			if actor.HostID != id {
				continue
			}

			deleteActors = append(deleteActors, key)
			changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
		}
	}

	// Return the function that applies the changes in-memory
	return func() {
		for i, id := range deleteHostIDs {
			delete(p.Hosts, id)
			delete(p.HostsByAddress, deleteAddresses[i])
			delete(p.HostActorTypes, id)
		}
		for _, key := range deleteActors {
			delete(p.ActiveActors, key)
		}
	}
}

// CleanupExpiredState computes the changes needed to remove expired state, recording them in changes
// It returns an apply function that performs the in-memory deletions
// Must be called while holding at least a read lock on StateMu
// apply must be called while holding the StateMu write lock (after the changes have been persisted)
func (p *Provider) CleanupExpiredState(changes *Changes) (apply func()) {
	now := p.Clock.Now()

	var deleteKeys []ActorKey
	for key, state := range p.ActorState {
		if state.IsExpired(now) {
			deleteKeys = append(deleteKeys, key)
			changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		}
	}

	// Return the function that applies the changes in-memory
	return func() {
		for _, key := range deleteKeys {
			delete(p.ActorState, key)
		}
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
