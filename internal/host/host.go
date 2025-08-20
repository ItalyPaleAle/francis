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

	"github.com/italypaleale/actors/internal/components"
)

const (
	defaultActorsMapSize = 32
	defaultIdleTimeout   = int64(15 * 60) // 15 minutes
)

type Host struct {
	running       atomic.Bool
	actorProvider components.ActorProvider

	// Actor factory methods; key is actor type
	actorFactories map[string]ActorFactory

	// Active actors; key is "actorType/actorID"
	actors       *haxmap.Map[string, Actor]
	actorsConfig []components.ActorHostType
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	IdleTimeout           time.Duration
	ConcurrencyLimit      int
	AlarmConcurrencyLimit int
}

// NewHost returns a new actor host
func NewHost(actorProvider components.ActorProvider) (*Host, error) {
	if actorProvider == nil {
		return nil, errors.New("actor provider is nil")
	}

	h := &Host{
		actorProvider:  actorProvider,
		actorsConfig:   []components.ActorHostType{},
		actorFactories: map[string]ActorFactory{},
		actors:         haxmap.New[string, Actor](defaultActorsMapSize),
	}
	return h, nil
}

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory ActorFactory, opts RegisterActorOptions) error {
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

// Run the host service
// Note this function is blocking, and will return only when the service is shut down via context cancellation.
func (s *Host) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("service is already running")
	}
	defer s.running.Store(false)

	// Register the host
	address := "todo"
	res, err := s.actorProvider.RegisterActorHost(ctx, components.RegisterActorHostRequest{
		Address:    address,
		ActorTypes: s.actorsConfig,
	})
	if err != nil {
		return fmt.Errorf("failed to register actor host: %w", err)
	}

	slog.InfoContext(ctx, "Registered actor host", "hostId", res.HostID, "address", address)

	defer func() {
		// Use a background context here as the parent one is likely canceled at this point
		unregisterCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		unregisterErr := s.actorProvider.UnregisterActorHost(unregisterCtx, res.HostID)
		if unregisterErr != nil {
			slog.WarnContext(unregisterCtx, "Error unregistering actor host", "error", unregisterErr, "hostId", res.HostID)
			return
		}

		slog.InfoContext(ctx, "Unregistered actor host", "hostId", res.HostID)
	}()

	// Block until the context is canceled
	err = s.actorProvider.Run(ctx)
	if err != nil {
		return fmt.Errorf("error running actor provider: %w", err)
	}

	return nil
}
