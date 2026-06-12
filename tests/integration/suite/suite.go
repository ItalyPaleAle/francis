//go:build integration

// Package suite is the global registry of integration test cases
//
// Each scenario self-registers from an init() function, and the single TestIntegration entrypoint iterates the registry to run them
package suite

import (
	"maps"
	"reflect"
	"strings"
	"sync"

	"github.com/italypaleale/francis/tests/integration/framework"
)

var (
	mu       sync.Mutex
	registry = map[string]framework.Case{}
)

// Named is an optional interface a Case can implement to provide an explicit registry name
// It lets a single generic case type register multiple times, for example once per provider variant, without name collisions
type Named interface {
	Name() string
}

// Register adds a Case to the global registry
// Call it from an init() function: registering two cases that resolve to the same name panics, surfacing accidental duplicates at startup
func Register(c framework.Case) {
	name := caseName(c)

	mu.Lock()
	defer mu.Unlock()
	_, dup := registry[name]
	if dup {
		// Indicates a development-time error
		panic("integration: duplicate suite case registered: " + name)
	}

	registry[name] = c
}

// All returns a copy of the registered cases keyed by name
func All() map[string]framework.Case {
	mu.Lock()
	defer mu.Unlock()

	out := make(map[string]framework.Case, len(registry))
	maps.Copy(out, registry)
	return out
}

// caseName builds a stable, readable name
// If the case implements Named that name is used, otherwise it is derived from the concrete type, for example "crosshost.invoke"
func caseName(c framework.Case) string {
	n, ok := c.(Named)
	if ok {
		return n.Name()
	}

	t := reflect.TypeOf(c)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	pkg := t.PkgPath()
	idx := strings.LastIndexByte(pkg, '/')
	if idx >= 0 {
		pkg = pkg[idx+1:]
	}

	return pkg + "." + t.Name()
}
