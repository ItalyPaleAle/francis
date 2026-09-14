// Package builtinkey gates construction of the privileged built-in actor client to francis-internal callers
package builtinkey

// Key authorizes an operation reserved for Francis' own built-in actors: constructing a privileged client that bypasses the guard preventing clients from operating on built-in actors, and attaching the workflow engine's labels to a state write or listing
// Only francis-internal packages can supply it, since this package cannot be imported from outside the module, so it keeps those paths private to the framework
type Key struct{}
