//go:build integration

// Package host provides in-process actor hosts as framework processes
//
// It wraps both host runtimes: a Local host embeds a provider, while a Remote host connects to a standalone runtime
// Both are built, run in a goroutine, awaited via their Ready channel, and shut down cleanly on cleanup
package host

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	// Stop gracefully shuts the host down mid-test, leaving the rest of the topology running
	// It is idempotent with Cleanup, and the host can be brought back up by calling Run again on the same address
	Stop(t *testing.T)
	// WaitExit blocks until the host's Run returns on its own and reports the error it exited with
	// A host exits by itself when one of its background services fails unrecoverably, such as when its health checks stop reaching the provider, and a scenario uses this to assert the host noticed
	WaitExit(t *testing.T, timeout time.Duration) error
	// ListJobs lists an actor's jobs straight through the host, bypassing the Service guard so tests can inspect built-in actors
	ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error)
	// Halt deactivates an actor straight through the host, bypassing the Service guard so tests can halt built-in actors
	// It returns actor.ErrActorNotHosted when the actor is not active on this host, which doubles as a placement probe
	Halt(actorType string, actorID string) error
}

// ActorReg describes an actor type to register on a host before it starts
type ActorReg struct {
	Type    string
	Factory actor.Factory
	Opts    []actorcore.RegisterActorOption
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

// splitHostPort breaks a "host:port" bind address into the parts the host options take separately
func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err, "bind address %s is not valid", addr)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err, "bind address %s does not carry a numeric port", addr)

	return host, port
}

// waitExit blocks until Run returns of its own accord and reports the error it exited with
// The error is put back so a later Stop or Cleanup still observes the exit and returns immediately
func waitExit(t *testing.T, address string, runErrC chan error, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-runErrC:
		runErrC <- err
		return err
	case <-time.After(timeout):
		t.Fatalf("host %s did not exit within %s", address, timeout)
		return nil
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
