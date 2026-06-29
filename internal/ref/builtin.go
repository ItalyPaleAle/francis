package ref

import (
	"strings"
)

// BuiltInActorTypePrefix is the reserved prefix for built-in (framework-provided) actor types
// Built-in actors are registered automatically by the host and bootstrapped at startup, and clients cannot invoke them directly
const BuiltInActorTypePrefix = "francis.builtin."

// IsBuiltInActorType reports whether an actor type is a reserved built-in actor type
func IsBuiltInActorType(actorType string) bool {
	return strings.HasPrefix(actorType, BuiltInActorTypePrefix)
}
