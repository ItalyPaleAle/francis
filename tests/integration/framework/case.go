//go:build integration

// Package framework is the core of the Francis integration/E2E test harness
//
// A test scenario implements the Case interface, returning a set of processes (hosts, a provider backend, and for the remote topology a runtime) from Setup, which the framework brings up before invoking Run for assertions
package framework

import (
	"testing"
)

// Case is implemented by every integration test scenario
//
// A concrete Case is typically a struct that, in Setup, constructs the processes that make up the test topology, stashes references to them on itself, and returns them as framework Options
// The framework then starts everything and calls Run, where the Case uses the stashed references to drive the running hosts and assert behavior
type Case interface {
	// Setup builds the test topology and returns it as framework options
	// It is called once, before Run, inside the scenario's subtest
	Setup(t *testing.T) []Option

	// Run executes assertions against the running topology created in Setup
	Run(t *testing.T)
}
