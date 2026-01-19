package standalone

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
)

// provider is the internal core provider (unexported).
// It stores all data in memory and calls the persistHook for each mutation.
type provider struct {
	cfg             components.ProviderConfig
	running         atomic.Bool
	log             *slog.Logger
	clock           clock.WithTicker
	cleanupInterval time.Duration
	mu              sync.RWMutex // Lock for hosts, hostsByAddress, hostActorTypes, activeActors, alarms, alarmsByID
	stateMu         sync.RWMutex // Lock for actorState

	// Persistence hook
	persistHook persistHook

	// In-memory data
	hosts          map[string]*host            // host_id -> host
	hostsByAddress map[string]string           // address -> host_id
	hostActorTypes map[string][]*hostActorType // host_id -> actor types
	activeActors   map[actorKey]*activeActor   // actor_type/actor_id -> active actor
	alarms         map[alarmKey]*alarm         // actor_type/actor_id/alarm_name -> alarm
	alarmsByID     map[string]*alarm           // alarm_id -> alarm
	actorState     map[actorKey]*stateEntry    // actor_type/actor_id -> state
}

// DefaultCleanupInterval is the default interval at which the provider purges expired state.
const DefaultCleanupInterval = 5 * time.Minute

// providerOptions contains options for creating the internal provider.
type providerOptions struct {
	components.ProviderOptions

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration

	// PersistHook is called after each mutation to persist changes.
	PersistHook persistHook
}

// newProvider creates a new internal provider.
func newProvider(log *slog.Logger, opts providerOptions, providerConfig components.ProviderConfig) (*provider, error) {
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
		persistHook = &noopPersistHook{}
	}

	p := &provider{
		cfg:             providerConfig,
		log:             log,
		clock:           opts.Clock,
		cleanupInterval: cleanupInterval,
		persistHook:     persistHook,

		hosts:          make(map[string]*host),
		hostsByAddress: make(map[string]string),
		hostActorTypes: make(map[string][]*hostActorType),
		activeActors:   make(map[actorKey]*activeActor),
		alarms:         make(map[alarmKey]*alarm),
		alarmsByID:     make(map[string]*alarm),
		actorState:     make(map[actorKey]*stateEntry),
	}

	if p.clock == nil {
		p.clock = clock.RealClock{}
	}

	return p, nil
}

func (p *provider) Init(ctx context.Context) error {
	// Nothing to initialize for the base provider
	return nil
}

func (p *provider) Run(ctx context.Context) error {
	if !p.running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	// Start the cleanup loop if enabled
	if p.cleanupInterval > 0 {
		go p.stateCleanupLoop(ctx)
	}

	// Wait for the context to be canceled
	<-ctx.Done()

	return nil
}

// stateCleanupLoop periodically purges expired state from memory.
func (p *provider) stateCleanupLoop(ctx context.Context) {
	ticker := p.clock.NewTicker(p.cleanupInterval)
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
func (p *provider) runCleanup(ctx context.Context) {
	// Clean up unhealthy hosts
	p.mu.Lock()
	changes := newChanges()
	p.cleanupUnhealthyHostsWithChanges(changes)
	if !changes.isEmpty() {
		if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
			p.log.Error("Failed to persist cleanup changes", "error", err)
		}
	}
	p.mu.Unlock()

	// Clean up expired state
	p.stateMu.Lock()
	stateChanges := newChanges()
	p.cleanupExpiredStateWithChanges(stateChanges)
	if !stateChanges.isEmpty() {
		if err := p.persistHook.PersistChanges(ctx, stateChanges); err != nil {
			p.log.Error("Failed to persist state cleanup changes", "error", err)
		}
	}
	p.stateMu.Unlock()
}

func (p *provider) HealthCheckInterval() time.Duration {
	// Use a simple interval: half of the health check deadline, with a minimum of 1s
	return max(p.cfg.HostHealthCheckDeadline/2, time.Second)
}

func (p *provider) RenewLeaseInterval() time.Duration {
	// The recommended interval is the bigger of: the lease duration less 10s, or half of the lease duration
	if p.cfg.AlarmsLeaseDuration < 20*time.Second {
		return p.cfg.AlarmsLeaseDuration / 2
	}

	return p.cfg.AlarmsLeaseDuration - 10*time.Second
}

// isHostHealthy checks if a host is healthy based on its last health check time.
// Must be called while holding at least a read lock.
func (p *provider) isHostHealthy(h *host) bool {
	return p.clock.Since(h.lastHealthCheck) < p.cfg.HostHealthCheckDeadline
}

// cleanupUnhealthyHostsWithChanges removes unhealthy hosts and their associated data,
// recording the changes for persistence.
// Must be called while holding a write lock.
func (p *provider) cleanupUnhealthyHostsWithChanges(changes *changes) {
	for id, h := range p.hosts {
		if p.isHostHealthy(h) {
			continue
		}

		delete(p.hostsByAddress, h.address)
		delete(p.hosts, id)
		changes.Hosts.Delete = append(changes.Hosts.Delete, id)

		// Delete host actor types
		for _, hat := range p.hostActorTypes[id] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, hostActorTypeKey{
				hostID:    hat.hostID,
				actorType: hat.actorType,
			})
		}
		delete(p.hostActorTypes, id)

		// Remove active actors on this host
		for key, actor := range p.activeActors {
			if actor.hostID != id {
				continue
			}

			delete(p.activeActors, key)
			changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
		}
	}
}

// cleanupExpiredStateWithChanges removes state that has expired, recording the changes.
// Must be called while holding a write lock on stateMu.
func (p *provider) cleanupExpiredStateWithChanges(changes *changes) {
	now := p.clock.Now()

	for key, state := range p.actorState {
		if state.expiration != nil && now.After(*state.expiration) {
			delete(p.actorState, key)
			changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		}
	}
}

// countActiveActorsOnHost counts actors of a specific type on a host.
// Must be called while holding at least a read lock.
func (p *provider) countActiveActorsOnHost(hostID, actorType string) int {
	count := 0
	for _, actor := range p.activeActors {
		if actor.hostID == hostID && actor.actorType == actorType {
			count++
		}
	}
	return count
}
