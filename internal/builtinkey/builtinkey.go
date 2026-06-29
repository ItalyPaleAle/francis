// Package builtinkey gates construction of the privileged built-in actor client to francis-internal callers
package builtinkey

// Key authorizes constructing a privileged client that bypasses the guard preventing clients from operating on built-in actors
// Only francis-internal packages can supply it, since this package cannot be imported from outside the module, so it keeps the privileged path private to the framework
type Key struct{}
