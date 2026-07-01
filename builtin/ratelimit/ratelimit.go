// Package ratelimit provides a built-in actor that rate-limits calls per key, keeping all state in memory
//
// Build one with New and pass the result to a host via the host's WithBuiltInActor option, then obtain a RateLimitService with Service to call Take
// There is one actor instance per rate-limit key (a free-form string such as an IP address or user ID), so each key is throttled independently
// A key's limiter lives only in the activated actor's memory and is never persisted to storage, so deactivating the actor simply resets that key's throttle state
//
// Take follows the semantics of go.uber.org/ratelimit: it blocks until the key's leaky bucket admits the call, smoothing bursts down to the configured rate rather than rejecting them
// Calls for the same key are serialized by the actor's turn lock, so holding the turn for the throttle delay is the intended backpressure
// Calls for different keys run on independent instances and never block each other
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/ratelimit"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// rateLimitActorTypePrefix namespaces rate limit actor types within the rate limiter's own bare type space
	// The reserved built-in prefix is added by the host when registering, so it is not included here
	rateLimitActorTypePrefix = "ratelimit."

	// methodTake admits one call against a key's limiter, blocking until the configured rate permits it
	methodTake = "take"
)

// New builds a rate limit built-in actor identified by name
//
// It admits the rate set by WithRate per period (one second unless changed with WithPer), keyed by a free-form rate-limit key such as an IP address or user ID
// Each key is a separate actor instance with its own in-memory limiter, so keys are throttled independently and no state is ever persisted to storage
//
// Pass the returned value to a host via the host's WithBuiltInActor option, then call RateLimitService.Take (obtained from Service) to throttle by key
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*RateLimit, error) {
	if name == "" {
		return nil, errors.New("rate limit name is required")
	}

	err := ref.ValidateComponents(name)
	if err != nil {
		return nil, fmt.Errorf("invalid rate limit name: %w", err)
	}

	var o rateLimitOptions
	for _, opt := range opts {
		opt(&o)
	}

	// A positive rate is required: it is the number of calls admitted per period
	if o.rate <= 0 {
		return nil, errors.New("WithRate is required and must be greater than zero")
	}

	// A negative period or slack is nonsensical
	// A zero period falls back to the library default of one second
	if o.per < 0 {
		return nil, errors.New("WithPer must not be negative")
	}
	if o.slack < 0 {
		return nil, errors.New("WithSlack must not be negative")
	}

	// Set default idleTimeout
	// We do not allow negative idle timeouts here, as that would make the actor never be expired, causing unbounded memory growth
	if o.idleTimeout <= 0 {
		// The default value is double the "per" interval, with a minimum of 1 minute
		o.idleTimeout = max(2*o.per, time.Minute)
	}

	// Build the go.uber.org/ratelimit options once: every key's limiter is constructed from the same configuration
	limiterOpts := make([]ratelimit.Option, 0, 2)
	if o.per > 0 {
		limiterOpts = append(limiterOpts, ratelimit.Per(o.per))
	}
	if o.slack > 0 {
		// WithSlack opts into bursting, accumulating unspent calls
		limiterOpts = append(limiterOpts, ratelimit.WithSlack(o.slack))
	} else {
		// Strict by default: never accumulate unspent calls for future bursts, so calls for a key are evenly spaced
		limiterOpts = append(limiterOpts, ratelimit.WithoutSlack)
	}

	return &RateLimit{
		actorType: rateLimitActorTypePrefix + name,
		factory: func(string, *actor.Service) actor.Actor {
			// Each key gets its own activation, and therefore its own limiter, built from the shared configuration
			return &rateLimitActor{
				limiter: ratelimit.New(o.rate, limiterOpts...),
			}
		},
		regOpts: actorcore.RegisterActorOptions{
			IdleTimeout: o.idleTimeout,
		},
	}, nil
}

// RateLimit is a built-in rate limit actor, returned by New and passed to a host via WithBuiltInActor
// It satisfies the framework's built-in actor contract (ActorType, Factory, RegisterOptions, Bootstrap) and exposes a Service method that returns a RateLimitService for the per-key Take operation
// The actor behavior itself lives in the unexported rateLimitActor instances that Factory builds, one per rate-limit key
type RateLimit struct {
	actorType string
	factory   actor.Factory
	regOpts   actorcore.RegisterActorOptions
}

// ActorType returns the reserved actor type registered for this rate limiter
func (r *RateLimit) ActorType() string {
	return r.actorType
}

// Factory returns the actor factory the host registers
func (r *RateLimit) Factory() actor.Factory {
	return r.factory
}

// RegisterOptions returns the registration options the host uses to register the actor
func (r *RateLimit) RegisterOptions() actorcore.RegisterActorOptions {
	return r.regOpts
}

// Bootstrap the actor
// The host calls this once it is ready
func (r *RateLimit) Bootstrap(context.Context, *actor.Service) error {
	// No-op: a rate limiter has no durable work to set up, since each key's state is in-memory and created lazily on first use
	return nil
}

// Service binds the rate limiter to an actor.Service, returning a RateLimitService that exposes Take pre-configured for that service
// Obtain the service from a host with host.Service()
func (r *RateLimit) Service(svc *actor.Service) *RateLimitService {
	return &RateLimitService{
		actorType: r.actorType,
		svc:       svc,
	}
}

// RateLimitService exposes the per-key Take operation of a rate limiter, bound to a specific actor.Service
// Obtain one from RateLimit.Service
type RateLimitService struct {
	actorType string
	svc       *actor.Service
}

// Take throttles the given key against the configured rate, blocking until the key's limiter admits the call
// The key is free-form (an IP address, user ID, route, and so on) and must not contain '/'
// It returns the caller's context error if the context is cancelled before the call is admitted
func (s *RateLimitService) Take(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("rate limit key is required")
	}

	err := ref.ValidateComponents(key)
	if err != nil {
		return fmt.Errorf("invalid rate limit key: %w", err)
	}

	// The key is the actor ID, so each key is served by its own instance and limiter
	_, err = builtinactor.InvokeActor(ctx, s.svc, s.actorType, key, methodTake, nil)
	return err
}

// rateLimitActor is one key's rate limiter: a single actor instance, keyed by the rate-limit key, holding an in-memory leaky-bucket limiter
// It implements actor.ActorInvoke for the take method, which the public client cannot reach because the Service rejects built-in actor types
type rateLimitActor struct {
	limiter ratelimit.Limiter
}

// Invoke handles the take method, the only call this actor services
func (a *rateLimitActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	if method != methodTake {
		// Only the take method is invoked on a rate limit actor, so anything else is a programming error
		return nil, fmt.Errorf("unknown rate limit method %q", method)
	}

	// Bail out before consuming a slot if the caller already gave up, so a cancelled call does not count against the rate
	err := ctx.Err()
	if err != nil {
		return nil, err
	}

	// Take blocks until the key's leaky bucket admits this call, which is how the limiter smooths bursts down to the configured rate
	// The turn lock already serializes calls for this key, so holding the turn for the throttle delay is the intended backpressure
	a.limiter.Take()

	return nil, nil
}
