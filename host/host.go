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
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

const (
	defaultActorsMapSize = 32
	defaultIdleTimeout   = int64(15 * 60) // 15 minutes
)

// Host is an actor host.
type Host struct {
	running       atomic.Bool
	actorProvider components.ActorProvider
	service       *actor.Service

	// Actor factory methods; key is actor type
	actorFactories map[string]actor.Factory

	// Active actors; key is "actorType/actorID"
	actors       *haxmap.Map[string, actor.Actor]
	actorsConfig []components.ActorHostType
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	IdleTimeout           time.Duration
	ConcurrencyLimit      int
	AlarmConcurrencyLimit int
}

// NewHost returns a new actor host.
func NewHost(actorProvider components.ActorProvider) (*Host, error) {
	if actorProvider == nil {
		return nil, errors.New("actor provider is nil")
	}

	h := &Host{
		actorProvider:  actorProvider,
		actorsConfig:   []components.ActorHostType{},
		actorFactories: map[string]actor.Factory{},
		actors:         haxmap.New[string, actor.Actor](defaultActorsMapSize),
	}
	h.service = actor.NewService(h)
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

	idleTimeout := int64(opts.IdleTimeout.Seconds())
	switch {
	case idleTimeout == 0:
		// Set default idle timeout if empty
		idleTimeout = defaultIdleTimeout
	case idleTimeout < 0:
		// A negative number means no timeout
		idleTimeout = -1
	case idleTimeout > math.MaxInt32:
		return errors.New("option IdleTimeout's seconds must fit in int32 (2^31-1)")
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

	h.actorsConfig = append(h.actorsConfig, components.ActorHostType{
		ActorType:             actorType,
		IdleTimeout:           int32(idleTimeout),
		ConcurrencyLimit:      int32(opts.ConcurrencyLimit),
		AlarmConcurrencyLimit: int32(opts.AlarmConcurrencyLimit),
	})

	h.actorFactories[actorType] = factory

	return nil
}

// Run the host service.
// Note this function is blocking, and will return only when the service is shut down via context cancellation.
func (h *Host) Run(ctx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return errors.New("service is already running")
	}
	defer h.running.Store(false)

	// Register the host
	address := "todo"
	res, err := h.actorProvider.RegisterHost(ctx, components.RegisterHostReq{
		Address:    address,
		ActorTypes: h.actorsConfig,
	})
	if err != nil {
		return fmt.Errorf("failed to register actor host: %w", err)
	}

	slog.InfoContext(ctx, "Registered actor host", "hostId", res.HostID, "address", address)

	defer func() {
		// Use a background context here as the parent one is likely canceled at this point
		unregisterCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		unregisterErr := h.actorProvider.UnregisterHost(unregisterCtx, res.HostID)
		if unregisterErr != nil {
			slog.WarnContext(unregisterCtx, "Error unregistering actor host", "error", unregisterErr, "hostId", res.HostID)
			return
		}

		slog.InfoContext(ctx, "Unregistered actor host", "hostId", res.HostID)
	}()

	// Block until the context is canceled
	err = h.actorProvider.Run(ctx)
	if err != nil {
		return fmt.Errorf("error running actor provider: %w", err)
	}

	return nil
}

func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (any, error) {
	// TODO: Check if actor is not local
	return h.invokeLocal(ctx, actorType, actorID, method, data)
}

func (h *Host) invokeLocal(ctx context.Context, actorType string, actorID string, method string, data any) (any, error) {
	actor, err := h.getOrCreateActor(actorType, actorID)
	if err != nil {
		return nil, err
	}

	// Invoke the actor
	return actor.Invoke(ctx, method, data)
}

func (h *Host) executeAlarm(ctx context.Context, actorType string, actorID string, name string, data any) error {
	actor, err := h.getOrCreateActor(actorType, actorID)
	if err != nil {
		return err
	}

	// Invoke the actor
	return actor.Alarm(ctx, name, data)
}

func (h *Host) getOrCreateActor(actorType string, actorID string) (actor.Actor, error) {
	// Get the factory function
	fn, err := h.createActorFn(actorType, actorID)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(actorType+"/"+actorID, fn)

	return actor, nil
}

func (h *Host) createActorFn(actorType string, actorID string) (func() actor.Actor, error) {
	// We don't need a locking mechanism here as this map is "locked" after the service has started
	factoryFn := h.actorFactories[actorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	return func() actor.Actor {
		return factoryFn(actorID, h.service)
	}, nil
}

func (h *Host) SetState(ctx context.Context, actorType string, actorID string, state any) error {
	// Encode the state using msgpack
	data, err := msgpack.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed serializing state using msgpack: %w", err)
	}

	err = h.actorProvider.SetState(ctx, actorRef(actorType, actorID), data, components.SetStateOpts{
		// TODO: support TTL
		TTL: 0,
	})
	if err != nil {
		return fmt.Errorf("failed saving state: %w", err)
	}

	return nil
}

func (h *Host) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	data, err := h.actorProvider.GetState(ctx, actorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed retrieving state: %w", err)
	}

	err = msgpack.Unmarshal(data, dest)
	if err != nil {
		return fmt.Errorf("failed unserializing state using msgpack: %w", err)
	}

	return nil
}

func (h *Host) DeleteState(ctx context.Context, actorType string, actorID string) error {
	err := h.actorProvider.DeleteState(ctx, actorRef(actorType, actorID))
	if errors.Is(err, components.ErrNoState) {
		return actor.ErrStateNotFound
	} else if err != nil {
		return fmt.Errorf("failed deleting state: %w", err)
	}

	return nil
}

func actorRef(actorType string, actorID string) components.ActorRef {
	return components.ActorRef{
		ActorType: actorType,
		ActorID:   actorID,
	}
}
