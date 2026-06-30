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
	SingletonActorID = "singleton"

	// MethodRegister is the lifecycle method a host invokes once to set up the actor's durable work, and is a no-op afterwards
	MethodRegister = "register"
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
	// Bootstrap is called by the host once it is ready, to set up the actor's durable work
	// It is safe to call from every host: invocations of the cluster-wide singleton are serialized by its turn lock, and the register handler is idempotent
	Bootstrap(ctx context.Context, svc *actor.Service) error
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
	fullType := FullActorType(bareActorType)
	client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, fullType, SingletonActorID, svc)
	return client.Invoke(ctx, fullType, SingletonActorID, method, payload)
}
