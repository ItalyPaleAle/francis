package actorcore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/italypaleale/go-kit/eventqueue"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// defaultActorsMapSize is the initial size of the active actors map; must be a power of 2
	defaultActorsMapSize = 128
	// actorBusyReEnqueueInterval extends the idle time of an actor that is busy when selected for deactivation
	actorBusyReEnqueueInterval = 10 * time.Second
)

// RemoveActorFunc removes an actor from the placement store when it is deactivated on this host
// In local mode this calls the provider directly; in remote mode it notifies the runtime
type RemoveActorFunc func(ctx context.Context, r ref.ActorRef) error

// Options configures a Manager
type Options struct {
	// Service is passed to actor factories so actors can call back into the host
	Service *actor.Service
	// RemoveActor removes an actor from the placement store on deactivation
	RemoveActor RemoveActorFunc
	// Logger is the slog logger
	Logger *slog.Logger
	// Clock, overridable for testing
	Clock clock.WithTicker
	// ProviderRequestTimeout bounds calls made during deactivation
	ProviderRequestTimeout time.Duration
	// ShutdownGracePeriod bounds how long a halted actor's in-flight call is allowed to drain
	ShutdownGracePeriod time.Duration
}

// Manager owns the lifecycle of the actors active on a host: activation, turn-based invocation, idle deactivation, and halting
// It is shared by the local and remote host implementations
type Manager struct {
	service                *actor.Service
	removeActor            RemoveActorFunc
	log                    *slog.Logger
	clock                  clock.WithTicker
	providerRequestTimeout time.Duration
	shutdownGracePeriod    time.Duration

	started atomic.Bool

	// ActorFactories holds the factory for each registered actor type, keyed by actor type
	ActorFactories map[string]actor.Factory
	// ActorsConfig holds the configuration for each registered actor type, keyed by actor type
	ActorsConfig map[string]components.ActorHostType
	// Actors holds the actors currently active on this host, keyed by "actorType/actorID"
	Actors *haxmap.Map[string, *ActiveActor]
	// IdleProcessor schedules actors for deactivation when they become idle; created by Start
	IdleProcessor *eventqueue.Processor[string, *ActiveActor]
}

// NewManager returns a Manager ready to register actors
func NewManager(opts Options) *Manager {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Clock == nil {
		opts.Clock = &clock.RealClock{}
	}

	return &Manager{
		service:                opts.Service,
		removeActor:            opts.RemoveActor,
		log:                    opts.Logger,
		clock:                  opts.Clock,
		providerRequestTimeout: opts.ProviderRequestTimeout,
		shutdownGracePeriod:    opts.ShutdownGracePeriod,
		ActorFactories:         map[string]actor.Factory{},
		ActorsConfig:           map[string]components.ActorHostType{},
		Actors:                 haxmap.New[string, *ActiveActor](defaultActorsMapSize),
	}
}

// SetLogger replaces the logger, used once the host has learned its ID
func (m *Manager) SetLogger(log *slog.Logger) {
	m.log = log
}

// RegisterActor registers a factory and configuration for an actor type
// It must be called before Start
func (m *Manager) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if m.started.Load() {
		return errors.New("cannot call RegisterActor after the host has started")
	}

	err := opts.Validate()
	if err != nil {
		return err
	}

	// #nosec G115 -- We have validated in opts.Validate that this is <= MaxInt32
	concurrencyLimit := int32(opts.ConcurrencyLimit)

	m.ActorsConfig[actorType] = components.ActorHostType{
		ActorType:           actorType,
		IdleTimeout:         opts.IdleTimeout,
		ConcurrencyLimit:    concurrencyLimit,
		DeactivationTimeout: opts.DeactivationTimeout,
		MaxAttempts:         opts.MaxAttempts,
		InitialRetryDelay:   opts.InitialRetryDelay,
	}
	m.ActorFactories[actorType] = factory

	return nil
}

// ActorConfig returns the configuration for an actor type
func (m *Manager) ActorConfig(actorType string) (components.ActorHostType, bool) {
	cfg, ok := m.ActorsConfig[actorType]
	return cfg, ok
}

// RegisteredActorTypes returns the configuration of all registered actor types
func (m *Manager) RegisteredActorTypes() []components.ActorHostType {
	list := make([]components.ActorHostType, len(m.ActorsConfig))
	var i int
	for _, ac := range m.ActorsConfig {
		list[i] = ac
		i++
	}
	return list
}

// Start creates the idle actor processor and marks the manager started so no more actor types can be registered
func (m *Manager) Start() {
	m.started.Store(true)
	m.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
		Clock:     m.clock,
		ExecuteFn: m.HandleIdleActor,
	})
}

// Close stops the idle actor processor
func (m *Manager) Close() {
	if m.IdleProcessor != nil {
		_ = m.IdleProcessor.Close()
	}
}

// LockAndInvoke gets or creates the actor, acquires its turn-based lock, and runs fn with the actor instance
func (m *Manager) LockAndInvoke(parentCtx context.Context, r ref.ActorRef, fn func(ctx context.Context, act *ActiveActor) (any, error)) (any, error) {
	// Get the actor, which may create it
	act, err := m.getOrCreateActor(r)
	if err != nil {
		return nil, err
	}

	return m.lockAndInvokeActor(parentCtx, act, fn)
}

// LockAndInvokeActive runs fn against an actor only if it is already active on this host
// It never activates the actor, returning actor.ErrActorNotActive when the actor is not active, so active-only invocations can be honored authoritatively here
func (m *Manager) LockAndInvokeActive(parentCtx context.Context, r ref.ActorRef, fn func(ctx context.Context, act *ActiveActor) (any, error)) (any, error) {
	// Only proceed if the actor is already active; do not create it
	act, ok := m.Actors.Get(r.String())
	if !ok || act == nil {
		return nil, actor.ErrActorNotActive
	}

	return m.lockAndInvokeActor(parentCtx, act, fn)
}

// lockAndInvokeActor acquires the actor's turn-based lock and runs fn, canceling the call if the actor is halted mid-flight
func (m *Manager) lockAndInvokeActor(parentCtx context.Context, act *ActiveActor, fn func(ctx context.Context, act *ActiveActor) (any, error)) (any, error) {
	// Create a context for this request, which allows us to stop it in-flight if needed
	ctx, cancel := context.WithCancelCause(parentCtx)
	defer cancel(nil)

	// Acquire a lock for turn-based concurrency
	haltCh, err := act.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for actor: %w", err)
	}
	defer act.Unlock()

	go func() {
		select {
		case <-haltCh:
			// The actor is being halted, so we need to cancel the context
			t := m.clock.NewTimer(m.shutdownGracePeriod)
			select {
			case <-t.C():
				// Graceful timeout has passed: forcefully cancel the context
				cancel(actor.ErrActorHalted)
				return
			case <-ctx.Done():
				// The method is returning (either fn() is done, or context was canceled)
				if !t.Stop() {
					<-t.C()
				}
				return
			}
		case <-ctx.Done():
			// The method is returning (either fn() is done, or context was canceled)
			return
		}
	}()

	return fn(ctx, act)
}

func (m *Manager) getOrCreateActor(r ref.ActorRef) (*ActiveActor, error) {
	// Get the factory function
	fn, err := m.createActorFn(r)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	a, _ := m.Actors.GetOrCompute(r.String(), fn)
	return a, nil
}

func (m *Manager) createActorFn(r ref.ActorRef) (func() *ActiveActor, error) {
	// We don't need a locking mechanism here as these maps are "locked" after the host has started
	factoryFn := m.ActorFactories[r.ActorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	idleTimeout := m.ActorsConfig[r.ActorType].IdleTimeout
	return func() *ActiveActor {
		instance := factoryFn(r.ActorID, m.service)
		return NewActiveActor(r, instance, idleTimeout, m.IdleProcessor, m.clock)
	}, nil
}

// HaltAll halts all actors active on the host, gracefully
func (m *Manager) HaltAll() error {
	// Deactivate all actors, each in its own goroutine
	errCh := make(chan error)
	var count int
	for _, act := range m.Actors.Iterator() {
		count++
		go func(act *ActiveActor) {
			haltErr := m.HaltActiveActor(act, true)
			if haltErr != nil {
				haltErr = fmt.Errorf("failed to halt actor '%s': %w", act.Key(), haltErr)
			}
			errCh <- haltErr
		}(act)
	}

	// Collect all errors
	errs := make([]error, 0)
	for range count {
		err := <-errCh
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error halting actors: %w", errors.Join(errs...))
	}

	return nil
}

// Halt gracefully halts a single actor that is active on this host
func (m *Manager) Halt(actorType string, actorID string) error {
	aRef := ref.NewActorRef(actorType, actorID)
	act, ok := m.Actors.Get(aRef.String())
	if !ok || act == nil {
		return actor.ErrActorNotHosted
	}

	err := m.HaltActiveActor(act, true)
	if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	return nil
}

// HaltDeferred halts an actor in the background, logging any error
func (m *Manager) HaltDeferred(actorType string, actorID string) {
	go func() {
		err := m.Halt(actorType, actorID)
		if err != nil {
			m.log.Error(
				"Failed to halt actor",
				slog.String("actorRef", ref.NewActorRef(actorType, actorID).String()),
				slog.Any("error", err),
			)
		}
	}()
}

// HandleIdleActor is the idle processor callback that deactivates an actor that has become idle, unless it is busy
func (m *Manager) HandleIdleActor(act *ActiveActor) {
	// Just because the actor is marked as idle, doesn't mean it's inactive
	// For example, there could be a long-running operation still in progress
	// We need to confirm the actor isn't busy, and we need to prevent others from starting new work on it
	// To do that, we use TryLock, which will give us a lock only if the actor isn't busy
	// If we get the lock, it means it's safe for us to dispose of it
	// (Note that TryLock does also reset the idleAt time, but we will ignore that)
	ok, _, err := act.TryLock()
	if err != nil {
		m.log.Error("Failed to try locking idle actor for deactivation", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}

	// If we did not acquire the lock, the actor is still busy
	// We will increase its idle time and re-enqueue it
	if !ok {
		m.log.Debug("Actor is busy and will not be deactivated; re-enqueueing it", slog.String("actorRef", act.Key()))
		act.UpdateIdleAt(actorBusyReEnqueueInterval)
		return
	}

	// Proceed with halting in a background goroutine, so we don't block other idle actors from being deactivated
	go func() {
		// We don't need to drain the active calls because we just acquired the lock
		haltErr := m.HaltActiveActor(act, false)
		if haltErr != nil {
			m.log.Error("Failed to deactivate idle actor", slog.String("actorRef", act.Key()), slog.Any("error", haltErr))
			return
		}
	}()
}

// HaltActiveActor gracefully halts an actor's instance and removes it from the placement store
func (m *Manager) HaltActiveActor(act *ActiveActor, drain bool) error {
	key := act.Key()

	m.log.Debug("Halting actor", slog.String("actorRef", key))

	// First, signal the actor's instance to halt, so it drains the current call and prevents more calls
	// Note that this call blocks until the current in-process request stops
	err := act.Halt(drain)
	if errors.Is(err, ErrActorAlreadyHalted) {
		// The actor is already halting, so nothing else to do here
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	// Send the actor a message it has been deactivated
	err = m.deactivateActor(act)
	if err != nil {
		// Even though the call to the actor's Deactivate method failed, we still need to continue with the deactivation process
		// Otherwise, we are in a state where the object is still in-memory and that will cause many issues
		m.log.Error("Actor returned an error during deactivation", slog.Any("error", err))
	}

	// Remove the actor from the table
	// This will prevent more state changes
	act, ok := m.Actors.GetAndDel(key)
	if !ok || act == nil {
		// If nothing was loaded, the actor was already deactivated
		return nil
	}

	// Report to the placement store that the actor has been deactivated
	// This uses a background context because at this point it needs to not be tied to the caller's context
	// Once the decision to deactivate an actor has been made, we must go through with it or we could have an inconsistent state
	// TODO: Handle this error - should retry, and then maybe gracefully exit?
	ctx, cancel := context.WithTimeout(context.Background(), m.providerRequestTimeout)
	defer cancel()
	err = m.removeActor(ctx, act.ref)
	if errors.Is(err, components.ErrNoActor) {
		// If the error is ErrNoActor, the actor was already deactivated in the placement store, so we can ignore it
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to remove actor from the placement store: %w", err)
	}

	return nil
}

func (m *Manager) deactivateActor(act *ActiveActor) error {
	m.log.Debug("Deactivated actor", slog.String("actorRef", act.Key()))

	// Check if the actor implements the Deactivate method
	obj, ok := act.Instance.(actor.ActorDeactivate)
	if !ok {
		// Not an error - this is an optional interface
		return nil
	}

	// If the timeout is empty, there's nothing to do
	timeout := m.ActorsConfig[act.ActorType()].DeactivationTimeout
	if timeout <= 0 {
		return nil
	}

	// This uses a background context because it should be unrelated from the caller's context
	// Once the decision to deactivate an actor has been made, we must go through with it or we could have an inconsistent state
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Call the Deactivate method on the actor
	err := obj.Deactivate(ctx)
	if err != nil {
		return fmt.Errorf("error from actor: %w", err)
	}

	return nil
}
