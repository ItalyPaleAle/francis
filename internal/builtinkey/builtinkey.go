// Package builtinkey gates construction of the privileged built-in actor client to francis-internal callers
package builtinkey

// secret is the private capability token that authenticates a valid Key at runtime
var secret = new(byte)

// Key authorizes constructing a privileged client or attaching built-in metadata that public callers cannot set
// An unexported field keeps unnamed struct literals in external packages from satisfying the type at compile time, and MustBeValid rejects zero values constructed via reflection
type Key struct {
	tok *byte
}

// New returns an authorized Key for francis-internal callers
func New() Key {
	return Key{tok: secret}
}

// MustBeValid panics if k was not constructed with New
func (k Key) MustBeValid() {
	if k.tok != secret {
		panic("builtinkey: unauthorized or zero-value Key")
	}
}
