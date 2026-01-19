package memory

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
)

// MemoryProvider is an in-memory implementation of ActorProvider.
// It stores all data in memory and is suitable for single-instance deployments or testing.
type MemoryProvider struct {
	cfg             components.ProviderConfig
	running         atomic.Bool
	log             *slog.Logger
	clock           clock.WithTicker
	cleanupInterval time.Duration
	mu              sync.RWMutex // Lock for hosts, hostsByAddress, hostActorTypes, activeActors, alarms, alarmsByID
	stateMu         sync.RWMutex // Lock for actorState

	hosts          map[string]*host            // host_id -> host
	hostsByAddress map[string]string           // address -> host_id
	hostActorTypes map[string][]*hostActorType // host_id -> actor types
	activeActors   map[actorKey]*activeActor   // actor_type/actor_id -> active actor
	alarms         map[alarmKey]*alarm         // actor_type/actor_id/alarm_name -> alarm
	alarmsByID     map[string]*alarm           // alarm_id -> alarm
	actorState     map[actorKey]*stateEntry    // actor_type/actor_id -> state
}

// DefaultCleanupInterval is the default interval at which the memory provider purges expired state.
const DefaultCleanupInterval = 5 * time.Minute

// MemoryProviderOptions contains options for creating a MemoryProvider.
type MemoryProviderOptions struct {
	components.ProviderOptions

	// Clock, used to pass a mock one for testing
	Clock clock.WithTicker

	// Interval at which to purge expired state from memory.
	// Default is 5 minutes; set to a negative value to disable.
	CleanupInterval time.Duration
}

// NewMemoryProvider creates a new in-memory ActorProvider.
func NewMemoryProvider(log *slog.Logger, opts MemoryProviderOptions, providerConfig components.ProviderConfig) (*MemoryProvider, error) {
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

	m := &MemoryProvider{
		cfg:             providerConfig,
		log:             log,
		clock:           opts.Clock,
		cleanupInterval: cleanupInterval,

		hosts:          make(map[string]*host),
		hostsByAddress: make(map[string]string),
		hostActorTypes: make(map[string][]*hostActorType),
		activeActors:   make(map[actorKey]*activeActor),
		alarms:         make(map[alarmKey]*alarm),
		alarmsByID:     make(map[string]*alarm),
		actorState:     make(map[actorKey]*stateEntry),
	}

	if m.clock == nil {
		m.clock = clock.RealClock{}
	}

	return m, nil
}

func (m *MemoryProvider) Init(ctx context.Context) error {
	// Nothing to initialize for in-memory provider
	return nil
}

func (m *MemoryProvider) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return components.ErrAlreadyRunning
	}

	// Start the cleanup loop if enabled
	if m.cleanupInterval > 0 {
		go m.stateCleanupLoop(ctx)
	}

	// Wait for the context to be canceled
	<-ctx.Done()

	return nil
}

// stateCleanupLoop periodically purges expired state from memory.
func (m *MemoryProvider) stateCleanupLoop(ctx context.Context) {
	ticker := m.clock.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			m.stateMu.Lock()
			m.cleanupExpiredState()
			m.stateMu.Unlock()
		}
	}
}

func (m *MemoryProvider) HealthCheckInterval() time.Duration {
	// Use a simple interval: half of the health check deadline, with a minimum of 1s
	return max(m.cfg.HostHealthCheckDeadline/2, time.Second)
}

func (m *MemoryProvider) RenewLeaseInterval() time.Duration {
	// The recommended interval is the bigger of: the lease duration less 10s, or half of the lease duration
	if m.cfg.AlarmsLeaseDuration < 20*time.Second {
		return m.cfg.AlarmsLeaseDuration / 2
	}

	return m.cfg.AlarmsLeaseDuration - 10*time.Second
}

// isHostHealthy checks if a host is healthy based on its last health check time.
// Must be called while holding at least a read lock.
func (m *MemoryProvider) isHostHealthy(h *host) bool {
	return m.clock.Since(h.lastHealthCheck) < m.cfg.HostHealthCheckDeadline
}

// cleanupUnhealthyHosts removes unhealthy hosts and their associated data.
// Must be called while holding a write lock.
func (m *MemoryProvider) cleanupUnhealthyHosts() {
	for id, h := range m.hosts {
		if m.isHostHealthy(h) {
			continue
		}

		delete(m.hostsByAddress, h.address)
		delete(m.hosts, id)
		delete(m.hostActorTypes, id)

		// Remove active actors on this host
		for key, actor := range m.activeActors {
			if actor.hostID != id {
				continue
			}

			delete(m.activeActors, key)
		}
	}
}

// cleanupExpiredState removes state that has expired.
// Must be called while holding a write lock on stateMu.
func (m *MemoryProvider) cleanupExpiredState() {
	now := m.clock.Now()

	for key, state := range m.actorState {
		if state.expiration != nil && now.After(*state.expiration) {
			delete(m.actorState, key)
		}
	}
}

// countActiveActorsOnHost counts actors of a specific type on a host.
// Must be called while holding at least a read lock.
func (m *MemoryProvider) countActiveActorsOnHost(hostID, actorType string) int {
	count := 0
	for _, actor := range m.activeActors {
		if actor.hostID == hostID && actor.actorType == actorType {
			count++
		}
	}
	return count
}
