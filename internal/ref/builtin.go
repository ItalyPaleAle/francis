package ref

import (
	"strings"
)

// BuiltInActorTypePrefix is the reserved prefix for built-in (framework-provided) actor types
// Built-in actors are registered automatically by the host and bootstrapped at startup, and clients cannot invoke them directly
const BuiltInActorTypePrefix = "francis.builtin."

// reservedMethodPrefix namespaces framework lifecycle methods so they cannot collide with an application method, and lets the public client and service reject them
const reservedMethodPrefix = "francis."

// MethodBootstrap is the reserved lifecycle method the host invokes on a singleton actor's instance to drive its Bootstrapper.Bootstrap hook
// It routes like any other invocation, so the actor core recognizes it and calls Bootstrap instead of the actor's Invoke handler
const MethodBootstrap = reservedMethodPrefix + "bootstrap"

// IsBuiltInActorType reports whether an actor type is a reserved built-in actor type
func IsBuiltInActorType(actorType string) bool {
	return strings.HasPrefix(actorType, BuiltInActorTypePrefix)
}

// IsReservedMethod reports whether a method name is reserved for framework lifecycle use, which the public client and service reject so only the framework can invoke it
func IsReservedMethod(method string) bool {
	return strings.HasPrefix(method, reservedMethodPrefix)
}
