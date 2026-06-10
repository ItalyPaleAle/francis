//go:build integration

// Package integration is the entrypoint for the Francis integration/E2E test suite
// It is built only under the "integration" build tag
//
// Run it with:
//
//	go test -tags integration -v -count=1 -timeout 15m ./tests/integration/...
//
// Scenarios live under ./suites and self-register via init(), and are pulled in through the blank imports below
// Postgres-backed scenarios require TEST_POSTGRES_CONNSTRING and TEST_STANDALONE_POSTGRES_CONNSTRING env vars set
package integration

import (
	"testing"

	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/suite"

	// Blank-import every suite package so their init() registers the cases
	_ "github.com/italypaleale/francis/tests/integration/suites/alarminvoke"
	_ "github.com/italypaleale/francis/tests/integration/suites/alarms"
	_ "github.com/italypaleale/francis/tests/integration/suites/crosshost"
	_ "github.com/italypaleale/francis/tests/integration/suites/durability"
	_ "github.com/italypaleale/francis/tests/integration/suites/errorprop"
	_ "github.com/italypaleale/francis/tests/integration/suites/failover"
	_ "github.com/italypaleale/francis/tests/integration/suites/invocation"
	_ "github.com/italypaleale/francis/tests/integration/suites/lifecycle"
	_ "github.com/italypaleale/francis/tests/integration/suites/routing"
	_ "github.com/italypaleale/francis/tests/integration/suites/state"
	_ "github.com/italypaleale/francis/tests/integration/suites/statecrud"
	_ "github.com/italypaleale/francis/tests/integration/suites/streaming"
)

// TestIntegration runs every registered integration scenario as a subtest
func TestIntegration(t *testing.T) {
	for name, c := range suite.All() {
		t.Run(name, func(t *testing.T) {
			opts := c.Setup(t)
			framework.Run(t, opts...)
			c.Run(t)
		})
	}
}
