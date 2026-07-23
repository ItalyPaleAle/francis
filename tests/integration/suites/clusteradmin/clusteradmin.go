//go:build integration

// Package clusteradmin exercises cluster admission end to end against real running hosts:
//
//   - the host limit is enforced when a host registers, so a host beyond the limit fails to start with ErrClusterFull
//   - a host configured with a different limit than the rest of the cluster is rejected with ErrMaxHostsMismatch
//   - an exclusive-access lease taken through a clusteradmin.Admin evicts the running host, blocks new registrations, and re-opens the cluster once released
//
// Unlike most scenarios these build their hosts directly rather than through the shared cluster helper, because the behaviors under test are host lifecycle edges (a host that fails to join, a host that self-terminates) that the framework's managed hosts intentionally do not model
// They run on the local topology with a shared store, where each host embeds its own provider and the Admin talks to the same store directly
package clusteradmin

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/clusteradmin"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	"github.com/italypaleale/francis/tests/integration/framework/process/ports"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// stableDeadline keeps hosts comfortably healthy through a subtest, so a slow health check never prunes a host the limit test is counting
	stableDeadline = time.Minute
	// evictDeadline is the shortest deadline the store providers accept (it must exceed their 5s query timeout by at least 5s), which still yields a 5s health check interval so a locked host self-terminates within a few seconds
	evictDeadline = 12 * time.Second

	readyTimeout = 20 * time.Second
	exitTimeout  = 30 * time.Second
	// shutdownGrace keeps host teardown snappy
	shutdownGrace = 5 * time.Second
)

// variants are the shared-store providers whose store both the hosts and the Admin can open at once
// The standalone providers coordinate nothing across separate provider instances, so admission taken through a second provider would not be visible to the hosts
var variants = []provider.Variant{provider.SQLite, provider.Postgres}

func init() {
	for _, v := range variants {
		suite.Register(&clusterAdmin{variant: v})
	}
}

// clusterAdmin drives directly-built local hosts and a clusteradmin.Admin against one shared store
type clusterAdmin struct {
	variant provider.Variant
	backend provider.Backend
}

func (s *clusterAdmin) Name() string {
	return "clusteradmin/local/" + string(s.variant)
}

func (s *clusterAdmin) Setup(t *testing.T) []framework.Option {
	// Only the shared store is a managed process: the hosts and the Admin are built by Run so it can control their lifecycles precisely
	s.backend = provider.New(s.variant)
	return []framework.Option{
		framework.WithProcesses(s.backend),
	}
}

func (s *clusterAdmin) Run(t *testing.T) {
	// The host limit is enforced at registration, so a host beyond the limit fails to start
	t.Run("enforces the host limit", func(t *testing.T) {
		limit := []local.HostOption{local.WithMaxHosts(2), local.WithHostHealthCheckDeadline(stableDeadline)}

		// Two hosts fill the cluster to its limit
		h1 := s.startHost(t, limit...)
		h1.waitReady(t)
		h2 := s.startHost(t, limit...)
		h2.waitReady(t)

		// The third host exceeds the limit and its Run returns before it ever becomes ready
		h3 := s.startHost(t, limit...)
		err := h3.waitExit(t)
		require.ErrorIs(t, err, components.ErrClusterFull, "the host beyond the limit should fail to register")
	})

	// A host configured with a different limit than the cluster's effective one is rejected
	t.Run("rejects a mismatched host limit", func(t *testing.T) {
		// The first host establishes the effective limit at one
		h1 := s.startHost(t, local.WithMaxHosts(1), local.WithHostHealthCheckDeadline(stableDeadline))
		h1.waitReady(t)

		// A second host claiming a different limit is rejected while the cluster is not empty
		h2 := s.startHost(t, local.WithMaxHosts(2), local.WithHostHealthCheckDeadline(stableDeadline))
		err := h2.waitExit(t)
		require.ErrorIs(t, err, components.ErrMaxHostsMismatch, "a host with a mismatched limit should be rejected")
	})

	// An exclusive-access lease evicts the running host, blocks new registrations, and re-opens the cluster once released
	t.Run("exclusive access evicts hosts and blocks new ones", func(t *testing.T) {
		ctx := t.Context()

		// A single unlimited host, on the shortest deadline the store accepts so it notices a lock within a few seconds
		h1 := s.startHost(t, local.WithHostHealthCheckDeadline(evictDeadline))
		h1.waitReady(t)

		// Build an admin against the same store, with a matching deadline
		admin, err := clusteradmin.New(ctx, s.backend.ProviderOptions(t), clusteradmin.Options{
			HostHealthCheckDeadline: evictDeadline,
			Logger:                  slog.New(slog.DiscardHandler),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = admin.Close() })

		// Taking exclusive access with Force waits for the cluster to drain, which happens once the running host self-terminates
		_, err = admin.AcquireExclusive(ctx, clusteradmin.AcquireOptions{Force: true})
		require.NoError(t, err)

		// The evicted host's health check surfaced ErrHostUnregistered, so its Run returned
		exitErr := h1.waitExit(t)
		require.ErrorIs(t, exitErr, components.ErrHostUnregistered, "the running host should self-terminate under the lock")

		// New registrations are blocked while the lease is held
		blocked := s.startHost(t, local.WithHostHealthCheckDeadline(evictDeadline))
		blockedErr := blocked.waitExit(t)
		require.ErrorIs(t, blockedErr, components.ErrClusterLocked, "registration should be blocked while the cluster is locked")

		// Releasing re-opens the cluster
		err = admin.ReleaseExclusive(ctx)
		require.NoError(t, err)

		// A host can join again once the lease is released
		h2 := s.startHost(t, local.WithHostHealthCheckDeadline(evictDeadline))
		h2.waitReady(t)
	})
}

// hostHandle wraps a directly-built local host and the channel that receives its Run error when it exits
type hostHandle struct {
	host  *local.Host
	errCh chan error
	done  chan struct{}
}

// startHost builds a local host against the shared store, runs it in the background, and registers its teardown
// It does not wait for readiness: the caller chooses whether to expect the host to become ready or to fail
func (s *clusterAdmin) startHost(t *testing.T, extra ...local.HostOption) *hostHandle {
	t.Helper()

	// Reserve a UDP port below the ephemeral range so an outbound dial cannot later collide with it
	port := ports.Reserve(t, 1)[0]
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	// This is test-only setup code run once per host, so the small fixed base plus a few extra options is not worth preallocating
	//nolint:prealloc // test helper: tiny, one-shot slice where preallocation adds noise without measurable benefit
	opts := []local.HostOption{
		local.WithAddress(addr),
		// Every local host derives the same CA from the shared runtime PSK, so hosts sharing the store authenticate each other
		local.WithRuntimePSKs(clustersecret.RuntimePSK),
		local.WithShutdownGracePeriod(shutdownGrace),
		local.WithLogger(slog.New(slog.DiscardHandler)),
		s.backend.LocalHostOption(t),
	}
	opts = append(opts, extra...)

	h, err := local.NewHost(opts...)
	require.NoError(t, err, "failed to create local host")

	// Register a trivial actor so the host advertises at least one type when it registers
	reg := shared.CounterReg(time.Minute)
	require.NoError(t, h.RegisterActor(reg.Type, reg.Factory, reg.Opts...))

	hh := &hostHandle{
		host:  h,
		errCh: make(chan error, 1),
		done:  make(chan struct{}),
	}

	// Run the host in the background, publishing its exit error and signalling done so teardown can wait without racing the caller
	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		hh.errCh <- h.Run(runCtx)
		close(hh.done)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-hh.done:
		case <-time.After(exitTimeout):
			t.Error("host did not shut down")
		}
	})

	return hh
}

// waitReady blocks until the host registers, failing the test if it exits or times out first
func (h *hostHandle) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-h.host.Ready():
	case err := <-h.errCh:
		t.Fatalf("host exited during startup: %v", err)
	case <-time.After(readyTimeout):
		t.Fatal("host did not become ready")
	}
}

// waitExit blocks until the host's Run returns and yields its error
func (h *hostHandle) waitExit(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.errCh:
		return err
	case <-time.After(exitTimeout):
		t.Fatal("host did not exit")
		return nil
	}
}
