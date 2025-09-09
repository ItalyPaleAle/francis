package host

import (
	"errors"
	"math"
	"time"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

const (
	defaultActorIdleTimeout         = 5 * time.Minute
	defaultActorDeactivationTimeout = 5 * time.Second
	defaultAlarmMaxAttempts         = 3
	defaultAlarmInitialRetryDelay   = 2 * time.Second
)

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}

	err := opts.Validate()
	if err != nil {
		return err
	}

	h.actorsConfig[actorType] = components.ActorHostType{
		ActorType:           actorType,
		IdleTimeout:         opts.IdleTimeout,
		ConcurrencyLimit:    int32(opts.ConcurrencyLimit),
		DeactivationTimeout: opts.DeactivationTimeout,
		MaxAttempts:         opts.MaxAttempts,
		InitialRetryDelay:   opts.InitialRetryDelay,
	}

	h.actorFactories[actorType] = factory

	return nil
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	// Maximum idle time before the actor is deactivated
	// Defaults to 5 minutes
	// A negative value means no timeout
	IdleTimeout time.Duration
	// Timeout for deactivating actors (because they are idle or they are being halted)
	// Defaults to 5s
	DeactivationTimeout time.Duration
	// Maximum number of actors of the same type active on this host
	// Defaults to 0, indicating no limit on the host
	// This must be between 0 (unlimited) and MaxInt32
	ConcurrencyLimit int
	// Maximum number of attempts when invoking the actor or executing alarms
	// Defaults to 3
	MaxAttempts int
	// Initial retry delay after failed invocation attempts
	// Defaults to 2s
	InitialRetryDelay time.Duration
}

func (o *RegisterActorOptions) Validate() error {
	switch {
	case o.IdleTimeout == 0:
		// Set default idle timeout if empty
		o.IdleTimeout = defaultActorIdleTimeout
	case o.IdleTimeout < 0:
		// A negative number means no timeout
		o.IdleTimeout = -1
	}

	switch {
	case o.ConcurrencyLimit <= 0:
		o.ConcurrencyLimit = 0
	case o.ConcurrencyLimit > math.MaxInt32:
		return errors.New("option ConcurrencyLimit must fit in int32 (2^31-1)")
	}

	switch {
	case o.DeactivationTimeout == 0:
		o.DeactivationTimeout = defaultActorDeactivationTimeout
	case o.DeactivationTimeout < 0:
		return errors.New("option DeactivationTimeout must not be negative")
	}

	if o.MaxAttempts <= 0 {
		o.MaxAttempts = defaultAlarmMaxAttempts
	}

	if o.InitialRetryDelay <= 0 {
		o.InitialRetryDelay = defaultAlarmInitialRetryDelay
	}

	return nil
}
