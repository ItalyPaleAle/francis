// Package ratelimit provides a built-in actor that rate-limits calls per key, keeping all state in memory
//
// Build one with New and register the result on a host with the host's RegisterBuiltInActor method, then obtain a RateLimitService with Service to call Allow
// There is one actor instance per rate-limit key (a free-form string such as an IP address or user ID), so each key is throttled independently
// A key's limiter lives only in the activated actor's memory and is never persisted to storage, so deactivating the actor simply resets that key's throttle state
//
// Allow is a non-blocking token-bucket check: it reports whether the call is admitted right now and, when it is not, how long the caller should wait before retrying (suitable for a Retry-After header on a 429 response)
// Because it never blocks, a throttled call returns immediately instead of holding the actor's turn, and respects context cancellation like any other invocation
// Calls for different keys run on independent instances and never contend with each other
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// rateLimitActorTypePrefix namespaces rate limit actor types within the rate limiter's own bare type space
	// The reserved built-in prefix is added by the host when registering, so it is not included here
	rateLimitActorTypePrefix = "ratelimit."

	// methodAllow checks a key's limiter and reports whether the call is admitted, without blocking
	methodAllow = "allow"
)

// New builds a rate limit built-in actor identified by name
//
// It admits the rate set by WithRate per period (one second unless changed with WithPer), keyed by a free-form rate-limit key such as an IP address or user ID
// Each key is a separate actor instance with its own in-memory limiter, so keys are throttled independently and no state is ever persisted to storage
//
// Register the returned value on a host with the host's RegisterBuiltInActor method, then call RateLimitService.Allow (obtained from Service) to throttle by key
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

	// A negative period or burst is nonsensical
	// A zero period falls back to one second, and a zero burst to a capacity of one
	if o.per < 0 {
		return nil, errors.New("WithPer must not be negative")
	}
	if o.burst < 0 {
		return nil, errors.New("WithBurst must not be negative")
	}

	// The period defaults to one second, so WithRate(n) alone means n calls per second
	per := o.per
	if per <= 0 {
		per = time.Second
	}

	// Set default idleTimeout
	// We do not allow negative idle timeouts here, as that would make the actor never be expired, causing unbounded memory growth
	if o.idleTimeout <= 0 {
		// The default value is double the "per" interval, with a minimum of 1 minute
		o.idleTimeout = max(2*per, time.Minute)
	}

	// Translate the rate/period into a token refill rate (tokens per second) and the burst into the bucket's capacity
	// The bucket starts full, so the first calls for a fresh key are admitted immediately, then refill at limit
	limit := rate.Limit(float64(o.rate) / per.Seconds())
	burst := o.burst
	if burst <= 0 {
		// Strict by default: a capacity of one admits calls one at a time and rejects any excess until the bucket refills
		burst = 1
	}

	return &RateLimit{
		actorType: rateLimitActorTypePrefix + name,
		factory: func(string, *actor.Service) actor.Actor {
			// Each key gets its own activation, and therefore its own limiter, built from the shared configuration
			return &rateLimitActor{
				limiter: rate.NewLimiter(limit, burst),
			}
		},
		regOpts: actorcore.RegisterActorOptions{
			IdleTimeout: o.idleTimeout,
		},
	}, nil
}

// RateLimit is a built-in rate limit actor, returned by New and registered on a host with RegisterBuiltInActor
// It satisfies the framework's built-in actor contract (ActorType, Factory, RegisterOptions, Singleton) and exposes a Service method that returns a RateLimitService for the per-key Allow operation
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

// Singleton reports that the rate limiter is not a singleton and needs no bootstrapping
// It keeps one instance per rate-limit key, and each key's state is in-memory and created lazily on first use, so there is no durable work to set up
func (r *RateLimit) Singleton() bool {
	return false
}

// Service binds the rate limiter to an actor.Service, returning a RateLimitService that exposes Allow pre-configured for that service
// Obtain the service from a host with host.Service()
func (r *RateLimit) Service(svc *actor.Service) *RateLimitService {
	return &RateLimitService{
		actorType: r.actorType,
		svc:       svc,
	}
}

// RateLimitService exposes the per-key Allow operation of a rate limiter, bound to a specific actor.Service
// Obtain one from RateLimit.Service
type RateLimitService struct {
	actorType string
	svc       *actor.Service
}

// Allow reports whether a call for the given key is admitted under the configured rate, without blocking
//
// When allowed is true the call has consumed a slot and may proceed.
// When it is false the call was throttled and retryAfter is how long the caller should wait before the key admits another call, which maps directly onto a Retry-After header for a 429 response. retryAfter is zero whenever allowed is true.
//
// The key is free-form (e.g. an IP address, user ID, route, etc) and must not contain '/'.
// The returned error is non-nil only when the key is invalid or the underlying actor invocation fails (including context cancellation) - it is not used to signal throttling.
func (s *RateLimitService) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error) {
	if key == "" {
		return false, 0, errors.New("rate limit key is required")
	}

	err = ref.ValidateComponents(key)
	if err != nil {
		return false, 0, fmt.Errorf("invalid rate limit key: %w", err)
	}

	// The key is the actor ID, so each key is served by its own instance and limiter
	env, err := builtinactor.InvokeActor(ctx, s.svc, s.actorType, key, methodAllow, nil)
	if err != nil {
		return false, 0, err
	}
	if env == nil {
		// The allow handler always returns a result, so an empty response means the invocation contract was violated
		return false, 0, errors.New("rate limiter returned an empty response")
	}

	var res allowResult
	err = env.Decode(&res)
	if err != nil {
		return false, 0, fmt.Errorf("failed to decode rate limit response: %w", err)
	}

	return res.Allowed, res.RetryAfter, nil
}

// allowResult is the reply an allow call carries back from the actor to the service across the invocation boundary
type allowResult struct {
	// Allowed reports whether the call was admitted under the configured rate
	Allowed bool `msgpack:"allowed"`
	// RetryAfter is how long the caller should wait before retrying when the call was not admitted
	// It is zero when Allowed is true
	RetryAfter time.Duration `msgpack:"retryAfter"`
}

// rateLimitActor is one key's rate limiter: a single actor instance, keyed by the rate-limit key, holding an in-memory token-bucket limiter
// It implements actor.ActorInvoke for the allow method, which the public client cannot reach because the Service rejects built-in actor types
type rateLimitActor struct {
	limiter *rate.Limiter
}

// Invoke handles the allow method, the only call this actor services
func (a *rateLimitActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	if method != methodAllow {
		// Only the allow method is invoked on a rate limit actor, so anything else is a programming error
		return nil, fmt.Errorf("unknown rate limit method %q", method)
	}

	// Bail out before consuming a token if the caller already gave up, so a cancelled call does not count against the rate
	err := ctx.Err()
	if err != nil {
		return nil, err
	}

	// Reserve a single token without blocking: the reservation reports how far in the future the token becomes available
	// A bucket capacity of at least one means a single-token reservation is always satisfiable, so DelayFrom is the only thing to inspect
	now := time.Now()
	r := a.limiter.ReserveN(now, 1)
	delay := r.DelayFrom(now)
	if delay > 0 {
		// Throttled: hand the token back so a rejected call does not drain the bucket, and report how long until a slot frees up
		r.CancelAt(now)
		return allowResult{
			Allowed:    false,
			RetryAfter: delay,
		}, nil
	}

	// Admitted right now: the reserved token is consumed
	return allowResult{
		Allowed: true,
	}, nil
}
