//go:build integration

// Package host provides in-process actor hosts as framework processes
//
// It wraps both host runtimes: a Local host embeds a provider, while a Remote host connects to a standalone runtime
// Both are built, run in a goroutine, awaited via their Ready channel, and shut down cleanly on cleanup
package host

import (
	"context"
	"testing"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework/process"
)

const (
	readinessTimeout = 30 * time.Second
	shutdownTimeout  = 45 * time.Second
	// ShutdownGrace keeps host teardown snappy in tests
	ShutdownGrace = 5 * time.Second
)

// Instance is a running actor host, regardless of whether it is local or remote
type Instance interface {
	process.Interface

	// Service returns the actor service for driving the host
	Service() *actor.Service
	// HostID returns the registered host ID, which is empty until the host has started
	HostID() string
	// Address returns the peer address the host is reachable at
	Address() string
}

// ActorReg describes an actor type to register on a host before it starts
type ActorReg struct {
	Type    string
	Factory actor.Factory
	Opts    actorcore.RegisterActorOptions
}

// waitReady blocks until the host signals readiness, failing fast if Run returns early
func waitReady(t *testing.T, address string, ready <-chan struct{}, runErrC chan error) {
	t.Helper()

	select {
	case <-ready:
		// The host registered and is ready to serve
	case err := <-runErrC:
		// Put the error back so Cleanup can observe it too
		runErrC <- err
		t.Fatalf("host %s exited during startup: %v", address, err)
	case <-time.After(readinessTimeout):
		t.Fatalf("host %s did not become ready within %s", address, readinessTimeout)
	}
}

// waitShutdown cancels the host context and waits for Run to return
// The shutdown error is drained but not asserted, matching the host package's own integration tests where graceful shutdown may surface a context error
func waitShutdown(t *testing.T, address string, runErrC chan error, cancel context.CancelFunc) {
	t.Helper()
	if cancel == nil {
		return
	}
	cancel()

	select {
	case <-runErrC:
		// Run returned, so the host has stopped
	case <-time.After(shutdownTimeout):
		t.Fatalf("host %s did not shut down within %s", address, shutdownTimeout)
	}
}
