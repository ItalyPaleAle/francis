package memory

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/ptr"
)

func TestMemoryProvider(t *testing.T) {
	p := initTestProvider(t)

	// Run the test suites
	suite := comptesting.NewSuite(p)
	t.Run("suite", suite.RunTests)
	// Note: Concurrency tests may not work as expected for in-memory provider
	// since it's a single instance. Skip them as mentioned in the requirements.
}

func initTestProvider(t *testing.T) *MemoryProvider {
	clock := clocktesting.NewFakeClock(time.Now())
	h := comptesting.NewSlogClockHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}), clock)
	log := slog.New(h)

	providerOpts := MemoryProviderOptions{
		Clock: clock,
	}
	providerConfig := comptesting.GetProviderConfig()

	// Create the provider
	p, err := NewMemoryProvider(log, providerOpts, providerConfig)
	require.NoError(t, err, "Error creating provider")

	// Init the provider
	err = p.Init(t.Context())
	require.NoError(t, err, "Error initializing provider")

	// Run the provider in background
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = p.Run(ctx)
	}()

	// Give a brief moment for Run to start
	time.Sleep(10 * time.Millisecond)

	return p
}

// CleanupExpired performs garbage collection of expired records.
func (m *MemoryProvider) CleanupExpired() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up unhealthy hosts
	m.cleanupUnhealthyHosts()

	// Clean up expired state
	m.cleanupExpiredState()

	return nil
}

func (m *MemoryProvider) clearData() {
	m.hosts = make(map[string]*host)
	m.hostsByAddress = make(map[string]string)
	m.hostActorTypes = make(map[string][]*hostActorType)
	m.activeActors = make(map[actorKey]*activeActor)
	m.alarms = make(map[alarmKey]*alarm)
	m.alarmsByID = make(map[string]*alarm)
	m.actorState = make(map[actorKey]*stateEntry)
}

// Seed seeds the data into the provider.
func (m *MemoryProvider) Seed(ctx context.Context, spec comptesting.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()

	// Clear all data
	m.clearData()

	// Seed hosts
	for _, h := range spec.Hosts {
		host := &host{
			id:              h.HostID,
			address:         h.Address,
			lastHealthCheck: now.Add(-h.LastHealthAgo),
		}
		m.hosts[h.HostID] = host
		m.hostsByAddress[h.Address] = h.HostID
	}

	// Seed host actor types
	for _, hat := range spec.HostActorTypes {
		if m.hostActorTypes[hat.HostID] == nil {
			m.hostActorTypes[hat.HostID] = make([]*hostActorType, 0)
		}
		m.hostActorTypes[hat.HostID] = append(m.hostActorTypes[hat.HostID], &hostActorType{
			hostID:           hat.HostID,
			actorType:        hat.ActorType,
			idleTimeout:      hat.ActorIdleTimeout,
			concurrencyLimit: int32(hat.ActorConcurrencyLimit), // #nosec G115
		})
	}

	// Seed active actors
	for _, aa := range spec.ActiveActors {
		key := newActorKey(aa.ActorType, aa.ActorID)
		m.activeActors[key] = &activeActor{
			actorType:   aa.ActorType,
			actorID:     aa.ActorID,
			hostID:      aa.HostID,
			idleTimeout: aa.ActorIdleTimeout,
			activation:  now.Add(-aa.ActivationAgo),
		}
	}

	// Seed alarms
	for _, a := range spec.Alarms {
		key := newAlarmKey(a.ActorType, a.ActorID, a.Name)
		alm := &alarm{
			id:        a.AlarmID,
			actorType: a.ActorType,
			actorID:   a.ActorID,
			name:      a.Name,
			dueTime:   now.Add(a.DueIn),
			interval:  a.Interval,
			data:      a.Data,
		}

		if a.TTL > 0 {
			alm.ttl = ptr.Of(now.Add(a.TTL))
		}

		if a.LeaseTTL != nil {
			leaseExp := now.Add(*a.LeaseTTL)
			alm.leaseExpiration = &leaseExp
			alm.leaseID = ptr.Of(uuid.New().String())
		}

		m.alarms[key] = alm
		m.alarmsByID[a.AlarmID] = alm
	}

	return nil
}

// Now returns the current time.
func (m *MemoryProvider) Now() time.Time {
	return m.clock.Now()
}

// AdvanceClock advances the clock.
func (m *MemoryProvider) AdvanceClock(d time.Duration) error {
	m.clock.Sleep(d)
	return nil
}

// GetAllActorState returns all stored actor state.
func (m *MemoryProvider) GetAllActorState(ctx context.Context) (comptesting.ActorStateSpecCollection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(comptesting.ActorStateSpecCollection, 0, len(m.actorState))
	for key, state := range m.actorState {
		result = append(result, comptesting.ActorStateSpec{
			ActorType: key.actorType,
			ActorID:   key.actorID,
			Data:      state.data,
		})
	}

	return result, nil
}

// GetAllHosts returns all stored hosts, host actor types, active actors, and alarms.
func (m *MemoryProvider) GetAllHosts(ctx context.Context) (comptesting.Spec, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.clock.Now()
	spec := comptesting.Spec{}

	// Hosts
	spec.Hosts = make([]comptesting.HostSpec, 0, len(m.hosts))
	for _, h := range m.hosts {
		spec.Hosts = append(spec.Hosts, comptesting.HostSpec{
			HostID:        h.id,
			Address:       h.address,
			LastHealthAgo: now.Sub(h.lastHealthCheck),
		})
	}

	// Host actor types
	spec.HostActorTypes = make([]comptesting.HostActorTypeSpec, 0)
	for hostID, types := range m.hostActorTypes {
		for _, hat := range types {
			spec.HostActorTypes = append(spec.HostActorTypes, comptesting.HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             hat.actorType,
				ActorIdleTimeout:      hat.idleTimeout,
				ActorConcurrencyLimit: int(hat.concurrencyLimit),
			})
		}
	}

	// Active actors
	spec.ActiveActors = make([]comptesting.ActiveActorSpec, 0, len(m.activeActors))
	for _, aa := range m.activeActors {
		spec.ActiveActors = append(spec.ActiveActors, comptesting.ActiveActorSpec{
			ActorType:        aa.actorType,
			ActorID:          aa.actorID,
			HostID:           aa.hostID,
			ActorIdleTimeout: aa.idleTimeout,
			ActivationAgo:    now.Sub(aa.activation),
		})
	}

	// Alarms
	spec.Alarms = make([]comptesting.AlarmSpec, 0, len(m.alarms))
	for _, a := range m.alarms {
		as := comptesting.AlarmSpec{
			AlarmID:   a.id,
			ActorType: a.actorType,
			ActorID:   a.actorID,
			Name:      a.name,
			DueIn:     a.dueTime.Sub(now),
			Interval:  a.interval,
			Data:      a.data,
		}

		if a.ttl != nil {
			as.TTL = a.ttl.Sub(now)
		}

		if a.leaseID != nil {
			as.LeaseID = a.leaseID
		}

		if a.leaseExpiration != nil {
			as.LeaseExp = ptr.Of(*a.leaseExpiration)
		}

		spec.Alarms = append(spec.Alarms, as)
	}

	return spec, nil
}
