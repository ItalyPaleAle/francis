package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/components/sqlite"
	"github.com/italypaleale/actors/internal/eventqueue"
	"github.com/italypaleale/actors/internal/servicerunner"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

const (
	defaultActorsMapSize       = 128
	defaultIdleTimeout         = 5 * time.Minute
	defaultDeactivationTimeout = 10 * time.Second
	// If an idle actor is getting deactivated, but it's still busy, will be re-enqueued with its idle timeout increased by this duration
	actorBusyReEnqueueInterval = 10 * time.Second
)

// Re-export provider options
type SQLiteProviderOptions = sqlite.SQLiteProviderOptions

// Host is an actor host.
type Host struct {
	// Address the host is reachable at
	address string

	// Host ID for the registered host
	hostID string

	running       atomic.Bool
	actorProvider components.ActorProvider
	service       *actor.Service

	// Actor factory methods; key is actor type
	actorFactories map[string]actor.Factory

	// Active actors; key is "actorType/actorID"
	actors             *haxmap.Map[string, *activeActor]
	idleActorProcessor *eventqueue.Processor[string, *activeActor]

	// Map of actor configuration objects; key is actor type
	actorsConfig map[string]components.ActorHostType

	log   *slog.Logger
	clock clock.WithTicker
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	IdleTimeout           time.Duration
	DeactivationTimeout   time.Duration
	ConcurrencyLimit      int
	AlarmConcurrencyLimit int
}

type NewHostOptions struct {
	// Address where the host can be reached at
	Address string

	// Instance of a slog.Logger
	Logger *slog.Logger

	// Options for the provider
	ProviderOptions components.ProviderOptions

	// Maximum interval between pings received from an actor host.
	HostHealthCheckDeadline time.Duration

	// Alarms lease duration
	AlarmsLeaseDuration time.Duration

	// Pre-fetch interval for alarms
	AlarmsFetchAheadInterval time.Duration

	// Batch size for pre-fetching alarms
	AlarmsFetchAheadBatchSize int

	// Allows setting a clock for testing
	clock clock.WithTicker
}

func (o NewHostOptions) getProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   o.HostHealthCheckDeadline,
		AlarmsLeaseDuration:       o.AlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  o.AlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: o.AlarmsFetchAheadBatchSize,
	}
}

// NewHost returns a new actor host.
func NewHost(opts NewHostOptions) (*Host, error) {
	// Validate the options passed
	if opts.Address == "" {
		return nil, errors.New("option Address is required")
	}

	// Set a default logger, which sends logs to /dev/null, if none is passed
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	// Init a real clock if none is passed
	if opts.clock == nil {
		opts.clock = &clock.RealClock{}
	}

	// Get the provider
	var (
		actorProvider components.ActorProvider
		err           error
	)
	switch x := opts.ProviderOptions.(type) {
	case sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(opts.Logger, x, opts.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case *sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(opts.Logger, *x, opts.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case nil:
		return nil, errors.New("option ProviderOptions is required")
	default:
		return nil, fmt.Errorf("unsupported value for ProviderOptions: %T", opts.ProviderOptions)
	}

	h := &Host{
		address:        opts.Address,
		actorProvider:  actorProvider,
		actorsConfig:   map[string]components.ActorHostType{},
		actorFactories: map[string]actor.Factory{},
		actors:         haxmap.New[string, *activeActor](defaultActorsMapSize),
		log:            opts.Logger,
		clock:          opts.clock,
	}
	h.service = actor.NewService(h)
	h.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		Clock:     h.clock,
		ExecuteFn: h.handleIdleActor,
	})

	return h, nil
}

// Service returns a Service object configured to interact with this host.
func (h *Host) Service() *actor.Service {
	return h.service
}

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}

	switch {
	case opts.IdleTimeout == 0:
		// Set default idle timeout if empty
		opts.IdleTimeout = defaultIdleTimeout
	case opts.IdleTimeout < 0:
		// A negative number means no timeout
		opts.IdleTimeout = -1
	}

	switch {
	case opts.ConcurrencyLimit <= 0:
		opts.ConcurrencyLimit = 0
	case opts.ConcurrencyLimit > math.MaxInt32:
		return errors.New("option ConcurrencyLimit must fit in int32 (2^31-1)")
	}

	switch {
	case opts.AlarmConcurrencyLimit <= 0:
		opts.AlarmConcurrencyLimit = 0
	case opts.AlarmConcurrencyLimit > math.MaxInt32:
		return errors.New("option AlarmConcurrencyLimit must fit in int32 (2^31-1)")
	case opts.AlarmConcurrencyLimit < opts.ConcurrencyLimit:
		return errors.New("option AlarmConcurrencyLimit must not be smaller than ConcurrencyLimit")
	}

	switch {
	case opts.DeactivationTimeout == 0:
		opts.DeactivationTimeout = defaultDeactivationTimeout
	case opts.DeactivationTimeout < 0:
		return errors.New("option DeactivationTimeout must not be negative")
	}

	h.actorsConfig[actorType] = components.ActorHostType{
		ActorType:             actorType,
		IdleTimeout:           opts.IdleTimeout,
		ConcurrencyLimit:      int32(opts.ConcurrencyLimit),
		AlarmConcurrencyLimit: int32(opts.AlarmConcurrencyLimit),
		DeactivationTimeout:   opts.DeactivationTimeout,
	}

	h.actorFactories[actorType] = factory

	return nil
}

// Run the host service.
// Note this function is blocking, and will return only when the service is shut down via context cancellation.
func (h *Host) Run(parentCtx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return errors.New("service is already running")
	}
	defer h.running.Store(false)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Perform initialization steps
	err := h.actorProvider.Init(ctx)
	if err != nil {
		return fmt.Errorf("failed to init provider: %w", err)
	}

	// Register the host
	actorsConfigList := make([]components.ActorHostType, len(h.actorsConfig))
	var i int
	for _, ac := range h.actorsConfig {
		actorsConfigList[i] = ac
		i++
	}

	res, err := h.actorProvider.RegisterHost(ctx, components.RegisterHostReq{
		Address:    h.address,
		ActorTypes: actorsConfigList,
	})
	if err != nil {
		return fmt.Errorf("failed to register actor host: %w", err)
	}

	h.hostID = res.HostID

	h.log.InfoContext(ctx, "Registered actor host", slog.String("hostId", h.hostID), slog.String("address", h.address))

	// Before returning, we halt all remaining actors
	defer func() {
		haltErr := h.HaltAll()
		if haltErr != nil {
			h.log.Warn("Error halting actors", slog.Any("error", haltErr), slog.String("hostId", res.HostID))
		}
	}()

	// Upon returning, we unregister the host so it can be removed cleanly
	// If the application crashes and this code isn't executed, eventually the host will be removed for not sending health checks periodically
	defer func() {
		// Use a background context here as the parent one is likely canceled at this point
		unregisterCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		unregisterErr := h.actorProvider.UnregisterHost(unregisterCtx, res.HostID)
		if unregisterErr != nil {
			h.log.WarnContext(unregisterCtx, "Error unregistering actor host", slog.Any("error", unregisterErr), slog.String("hostId", res.HostID))
			return
		}

		h.log.InfoContext(ctx, "Unregistered actor host", slog.String("hostId", res.HostID))
	}()

	return servicerunner.
		NewServiceRunner(
			// Perform health checks in background
			h.runHealthChecks,

			// Run the actor provider
			h.actorProvider.Run,
		).
		Run(ctx)
}

func (h *Host) HaltAll() error {
	// Deactivate all actors, each in its own goroutine
	errCh := make(chan error)
	var count int
	for _, act := range h.actors.Iterator() {
		count++
		go func(act *activeActor) {
			err := h.haltActor(act)
			if err != nil {
				err = fmt.Errorf("failed to halt actor '%s': %w", act.Key(), err)
			}
			errCh <- err
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

func (h *Host) runHealthChecks(ctx context.Context) error {
	var err error

	// Perform periodic health checks
	interval := h.actorProvider.HealthCheckInterval()
	h.log.DebugContext(ctx, "Starting background health checks", slog.Any("interval", interval))

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			h.log.DebugContext(ctx, "Sending health check to the provider")
			err = h.actorProvider.UpdateActorHost(ctx, h.hostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
			if err != nil {
				// TODO: Should we retry once in case of errors?
				h.log.ErrorContext(ctx, "Health check failed", slog.Any("error", err))
				return fmt.Errorf("failed to perform health check: %w", err)
			}
		case <-ctx.Done():
			// Stop when the context is canceled
			return ctx.Err()
		}
	}
}

func (h *Host) handleIdleActor(act *activeActor) {
	key := act.Key()

	// Just because the actor is marked as idle, doesn't mean it's inactive
	// For example, there could be a long-running operation still in progress
	// We need to confirm the actor isn't busy, and we need to prevent others from starting new work on it
	// To do that, we use TryLock, which will give us a lock only if the actor isn't busy
	// If we get the lock, it means it's safe for us to dispose of it
	// (Note that TryLock does also reset the idleAt time, but we will ignore that)
	ok, _, err := act.TryLock()
	if err != nil {
		h.log.Error("Failed to try locking idle actor for deactivation", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}

	// If we did not acquire the lock, the actor is still busy.
	// Wwe will increase its idle time and re-enqueue it
	if !ok {
		h.log.Debug("Actor is busy and will not be deactivated; re-enqueueing it", slog.String("actorRef", act.Key()))
		act.updateIdleAt(actorBusyReEnqueueInterval)
		return
	}

	// Remove the actor from the table
	// This will prevent more state changes
	_, ok = h.actors.GetAndDel(key)
	if !ok {
		// If nothing was loaded, the actor was probably already deactivated
		return
	}

	// Halt the actor
	err = act.Halt()
	if err != nil {
		h.log.Error("Failed to halt idle actor", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}

	// Proceed with deactivating the actor
	err = h.deactivateActor(act)
	if err != nil {
		h.log.Error("Failed to deactivate idle actor", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}
}

func (h *Host) haltActor(act *activeActor) error {
	key := act.Key()

	h.log.Debug("Halting actor", slog.String("actorRef", key))

	// Remove the actor from the table
	// This will prevent more state changes
	act, ok := h.actors.GetAndDel(key)
	if !ok || act == nil {
		// If nothing was loaded, the actor was already deactivated
		return nil
	}

	// Halt the actor, so it drains the current call and prevents more calls
	err := act.Halt()
	if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	// Send the actor a message it has been deactivated
	err = h.deactivateActor(act)
	if err != nil {
		return fmt.Errorf("failed to deactivate actor: %w", err)
	}

	return nil
}

func (h *Host) deactivateActor(act *activeActor) error {
	h.log.Debug("Deactivated actor", slog.String("actorRef", act.Key()))

	// This uses a background context because it should be unrelated from the caller's context
	// Once the decision to deactivate an actor has been made, we must go through with it or we could have an inconsistent state
	ctx, cancel := context.WithTimeout(context.Background(), h.actorsConfig[act.ActorType()].DeactivationTimeout)
	defer cancel()

	// Call the Deactivate method on the actor
	err := act.instance.Deactivate(ctx)
	if err != nil {
		return fmt.Errorf("error from actor: %w", err)
	}

	return nil
}

func actorRef(actorType string, actorID string) components.ActorRef {
	return components.ActorRef{
		ActorType: actorType,
		ActorID:   actorID,
	}
}
