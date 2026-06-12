//go:build integration

// Package process defines the contract for a managed, in-process component of an integration test, such as an actor host or a provider backend
//
// Every moving part of a test topology is a process with a Run and Cleanup lifecycle, composed together by the framework package
package process

import (
	"testing"
)

// Interface is implemented by every managed component of an integration test
type Interface interface {
	// Run starts the process and returns only once it is ready to be used
	// It must not block for the lifetime of the process: any long-running work belongs in a background goroutine
	Run(t *testing.T)

	// Cleanup tears the process down
	// It must be idempotent and must not panic if called after a failed or partial Run
	Cleanup(t *testing.T)
}
