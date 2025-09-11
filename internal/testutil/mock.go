//go:build unit

package testutil

import (
	"context"
)

// MatchContextInterface returns true if the value matches the context.Context interface
// This can be used as an argument for mock.MatchedBy
func MatchContextInterface(v any) bool {
	_, ok := v.(context.Context)
	return ok
}
