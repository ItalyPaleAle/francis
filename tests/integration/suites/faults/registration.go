//go:build integration

package faults

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	"github.com/italypaleale/francis/tests/integration/framework/process/ports"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// registerTimeout bounds how long a scenario waits for a host to either become ready or fail its registration
const registerTimeout = 30 * time.Second

// addressConflict starts a second host on an address a healthy host already holds, and verifies the newcomer is rejected rather than displacing it
//
// Two hosts at one address is a deployment mistake rather than a fault, but the consequence is the same class of problem as the rest of this package: if the second one were allowed to register, invocations for the first host's actors would be routed to a process that has never heard of them
// The hosts are built directly rather than through the cluster helper, which deliberately hands every host a port of its own
type addressConflict struct {
	backend provider.Backend
}

func (s *addressConflict) Name() string {
	return "faults-address-conflict/local/sqlite"
}

func (s *addressConflict) Setup(t *testing.T) []framework.Option {
	// Only the shared store is a managed process, since this scenario controls when each host starts and how it ends
	s.backend = provider.New(provider.SQLite, provider.Options{
		HostHealthCheckDeadline: healthCheckDeadline,
		HealthCheck:             healthCheckPolicy,
		QueryTimeout:            queryTimeout,
	})
	return []framework.Option{
		framework.WithProcesses(s.backend),
	}
}

func (s *addressConflict) Run(t *testing.T) {
	// One address, claimed first by a host that stays healthy for the whole scenario
	port := ports.Reserve(t, 1)[0]
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	first := s.startHost(t, addr)
	select {
	case <-first.host.Ready():
	case err := <-first.errCh:
		t.Fatalf("the first host failed to start: %v", err)
	case <-time.After(registerTimeout):
		t.Fatal("the first host did not become ready")
	}

	// A second host claiming the same address is turned away, because the address is still held by a host reporting healthy
	second := s.startHost(t, addr)
	select {
	case err := <-second.errCh:
		require.ErrorIs(t, err, components.ErrHostAlreadyRegistered, "a host claiming an address another healthy host holds should be rejected")
	case <-second.host.Ready():
		t.Fatal("the second host became ready despite claiming an address already in use")
	case <-time.After(registerTimeout):
		t.Fatal("the second host neither registered nor failed")
	}

	// The rejection left the incumbent alone: it is still the host registered at that address
	hosts, err := s.listHosts(t)
	require.NoError(t, err)
	require.Len(t, hosts, 1, "only the first host should be registered")
	require.Equal(t, addr, hosts[0].Address)
	require.Equal(t, first.host.HostID(), hosts[0].HostID, "the incumbent should keep its registration")
}

// hostRun is a host started outside the framework's process lifecycle, so the scenario can observe exactly how its Run ends
type hostRun struct {
	host  *local.Host
	errCh chan error
}

// startHost builds and starts a local host at the given address, returning once Run is under way
func (s *addressConflict) startHost(t *testing.T, addr string) *hostRun {
	t.Helper()

	h, err := local.NewHost(
		local.WithAddress(addr),
		local.WithRuntimePSKs(clustersecret.RuntimePSK),
		local.WithHostHealthCheckDeadline(healthCheckDeadline),
		local.WithHealthCheckPolicy(&healthCheckPolicy),
		local.WithProviderRequestTimeout(requestTimeout),
		local.WithLogger(slog.New(slog.DiscardHandler)),
		s.backend.LocalHostOption(t),
	)
	require.NoError(t, err, "failed to create the host")

	reg := shared.CounterReg(time.Minute)
	require.NoError(t, h.RegisterActor(reg.Type, reg.Factory, reg.Opts...))

	hr := &hostRun{host: h, errCh: make(chan error, 1)}

	// The run context is independent of the test's, so cleanup can stop the host and wait for it to finish
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hr.errCh <- h.Run(runCtx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(registerTimeout):
			t.Error("host did not shut down")
		}
	})

	return hr
}

// listHosts reads the registered hosts straight from the store, so the scenario can assert on what the rejection left behind
func (s *addressConflict) listHosts(t *testing.T) ([]components.HostInfo, error) {
	t.Helper()

	p := s.backend.NewProvider(t, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), registerTimeout)
	defer cancel()
	return p.ListHosts(ctx)
}
