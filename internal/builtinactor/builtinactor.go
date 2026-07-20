// Package builtinactor defines the contract that framework-managed (built-in) actors implement, plus helpers for driving them
//
// A host registers a built-in actor automatically and bootstraps it once ready, reaching it through the privileged client because the public Service rejects built-in actor types
// The concrete implementations (such as the cron job) live in their own packages and satisfy the BuiltInActor interface
package builtinactor

import (
	"context"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// SingletonActorID is the fixed actor ID used for built-in singleton actors, so every host targets the same cluster-wide instance
	SingletonActorID = actor.SingletonActorID

	// MethodUnregister is the lifecycle method that tears down the actor's durable work
	MethodUnregister = "unregister"
)

// BuiltInActor is the contract a framework-managed actor implements so a host can register and bootstrap it
// Concrete actors implement it and may expose additional, actor-specific methods of their own
type BuiltInActor interface {
	// ActorType returns this actor's type without the reserved built-in prefix
	// The host adds the prefix (see FullActorType) when registering, so individual built-in actors do not deal with it
	ActorType() string
	// Factory returns the actor factory the host registers
	Factory() actor.Factory
	// RegisterOptions returns the registration options the host uses to register the actor
	RegisterOptions() actorcore.RegisterActorOptions
	// Singleton reports whether the host bootstraps this actor's cluster-wide singleton instance once it is ready
	// When true, the instance the Factory produces at SingletonActorID must implement actor.ActorBootstrapper
	Singleton() bool
}

// BuiltInActorRegistration describes a single actor type a built-in registers on the host
// A built-in that registers more than one type (such as a work pool with one type per capability) returns several of these from MultiBuiltInActor.Registrations
type BuiltInActorRegistration struct {
	// ActorType is the built-in's bare actor type for this registration, without the reserved prefix
	ActorType string
	// Factory is the actor factory the host registers for this type
	Factory actor.Factory
	// RegisterOptions are the registration options the host uses for this type
	RegisterOptions actorcore.RegisterActorOptions
	// Singleton reports whether the host bootstraps this type's cluster-wide singleton instance once ready
	Singleton bool
}

// MultiBuiltInActor is an optional contract a BuiltInActor implements when it registers more than one actor type
// A host checks for it in RegisterBuiltInActor and, when present, registers every type it returns instead of the single BuiltInActor type
type MultiBuiltInActor interface {
	BuiltInActor
	// Registrations returns every actor type this built-in registers on the host
	Registrations() []BuiltInActorRegistration
}

// RegistrationsFor returns the actor-type registrations a host must create for a built-in actor
// It returns every type from a MultiBuiltInActor, or the single type of a plain BuiltInActor, so hosts have one code path for both
func RegistrationsFor(b BuiltInActor) []BuiltInActorRegistration {
	multi, ok := b.(MultiBuiltInActor)
	if ok {
		return multi.Registrations()
	}

	return []BuiltInActorRegistration{{
		ActorType:       b.ActorType(),
		Factory:         b.Factory(),
		RegisterOptions: b.RegisterOptions(),
		Singleton:       b.Singleton(),
	}}
}

// FullActorType returns the reserved actor type a built-in actor is registered under, by prefixing its bare type
// The host applies it when registering, so a built-in actor only ever deals with its own bare type
func FullActorType(bareActorType string) string {
	return ref.BuiltInActorTypePrefix + bareActorType
}

// NewClient returns a privileged client bound to a built-in actor's singleton-or-other instance, resolving the reserved type from the bare one
// Built-in actor implementations use it so they never have to construct the reserved actor type themselves
func NewClient[T any](bareActorType string, actorID string, svc *actor.Service) actor.Client[T] {
	return actor.NewBuiltInActorClient[T](builtinkey.Key{}, FullActorType(bareActorType), actorID, svc)
}

// Invoke invokes a method on a built-in actor's cluster-wide singleton through the privileged client, returning the response envelope
// Built-in actor implementations use it to drive their lifecycle and on-demand methods, which the public Service and client both reject
// bareActorType is the actor's bare type (without the reserved prefix), and payload is optional: pass nil when the method carries no request data
func Invoke(ctx context.Context, svc *actor.Service, bareActorType string, method string, payload any) (actor.Envelope, error) {
	return InvokeActor(ctx, svc, bareActorType, SingletonActorID, method, payload)
}

// InvokeActor invokes a method on a specific instance of a built-in actor through the privileged client, returning the response envelope
// It is the actor-ID-addressed counterpart of Invoke, which always targets the cluster-wide singleton: built-in actors that key one instance per actor ID (such as the rate limiter, one instance per rate-limit key) use it to reach an arbitrary instance
// bareActorType is the actor's bare type (without the reserved prefix), and payload is optional: pass nil when the method carries no request data
func InvokeActor(ctx context.Context, svc *actor.Service, bareActorType string, actorID string, method string, payload any) (actor.Envelope, error) {
	fullType := FullActorType(bareActorType)
	client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, fullType, actorID, svc)
	return client.Invoke(ctx, fullType, actorID, method, payload)
}
