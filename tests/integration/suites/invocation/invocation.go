//go:build integration

// Package invocation exercises invocation behaviors that depend on host and placement bookkeeping: the per-host limit on the number of active actors of a kind, and the turn-based concurrency that serializes calls to a single actor
package invocation

import (
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

// variants is the representative set the invocation scenarios run against
// Capacity enforcement lives in provider placement, so one variant per distinct placement implementation is enough: SQLite and Postgres each have their own SQL, and the standalone providers share one in-memory placement path that StandaloneMemory stands in for
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the invocation scenarios across the representative variants on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&capacity{kind: k, variant: v})
			suite.Register(&capacityIdle{kind: k, variant: v})
			suite.Register(&activeOnly{kind: k, variant: v})
			suite.Register(&turnBased{kind: k, variant: v})
			suite.Register(&parallel{kind: k, variant: v})

			// Two hosts share one backend, so on the local runtime only providers that coordinate across processes qualify
			if k == cluster.Remote || v.LocalMultiHost() {
				suite.Register(&crossHostSerial{kind: k, variant: v})
			}
		}
	}
}
