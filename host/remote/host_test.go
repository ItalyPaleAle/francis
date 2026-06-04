package remote

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/runtime"
)

// testActor is a minimal actor used by the integration tests
// It echoes invocations, round-trips its state through the runtime, and optionally signals invocation, alarm, and deactivation events
type testActor struct {
	svc   *actor.Service
	id    string
	label string

	// Optional observation channels
	// A nil channel means the event is not observed
	alarmCh      chan string
	invokeCh     chan string
	deactivateCh chan string
}

func (a *testActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	signal(a.invokeCh, method)

	switch method {
	case "echo":
		var in string
		_ = data.Decode(&in)
		return a.label + "echo:" + in, nil
	case "setstate":
		return nil, a.svc.SetState(ctx, "T", a.id, "saved-value", nil)
	case "getstate":
		var out string
		err := a.svc.GetState(ctx, "T", a.id, &out)
		return out, err
	case "fail":
		return nil, errors.New("boom")
	default:
		return nil, nil
	}
}

func (a *testActor) Alarm(_ context.Context, name string, _ actor.Envelope) error {
	signal(a.alarmCh, name)
	return nil
}

// InvokeStream echoes the request body back, prefixed with the actor's label, and echoes the request content type
func (a *testActor) InvokeStream(_ context.Context, method string, reqContentType string, body io.Reader, w actor.StreamResponseWriter) error {
	signal(a.invokeCh, method)

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	w.SetContentType(reqContentType)
	_, err = w.Write([]byte(a.label + "stream:" + string(data)))
	return err
}

func (a *testActor) Deactivate(_ context.Context) error {
	signal(a.deactivateCh, a.id)
	return nil
}

// signal performs a non-blocking send so an unobserved or already-signaled channel never blocks the actor
func signal(ch chan string, v string) {
	if ch == nil {
		return
	}
	select {
	case ch <- v:
	default:
	}
}

// newRemoteHost builds a remote host pointed at the runtime, without starting it
func newRemoteHost(t *testing.T, runtimeAddr string) *Host {
	t.Helper()

	host, err := NewHost(
		WithAddress(freeUDPAddr(t)),
		WithRuntimeAddresses(runtimeAddr),
		WithServerTLSInsecureSkipTLSValidation(),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)
	return host
}

// runRemoteHost starts the host and waits until it has registered with the runtime
func runRemoteHost(t *testing.T, host *Host) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host did not connect to the runtime")
	}

	// Wait for Run to return when the test's context is canceled
	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("host did not shut down")
		}
	})
}

// startTestRuntime starts an in-memory runtime over WebTransport and returns its address and backing provider
func startTestRuntime(t *testing.T, ctx context.Context) (string, *standalone.StandaloneMemory) {
	t.Helper()

	addr := freeUDPAddr(t)

	prov, err := standalone.NewStandaloneMemory(slog.New(slog.DiscardHandler), standalone.StandaloneMemoryOptions{}, components.ProviderConfig{
		HostHealthCheckDeadline:   20 * time.Second,
		AlarmsLeaseDuration:       20 * time.Second,
		AlarmsFetchAheadInterval:  2500 * time.Millisecond,
		AlarmsFetchAheadBatchSize: 25,
	})
	require.NoError(t, err)

	rt, err := runtime.NewRuntime(prov,
		runtime.WithBind(addr),
		runtime.WithAlarmsPollInterval(300*time.Millisecond),
	)
	require.NoError(t, err)

	go func() {
		_ = rt.Run(ctx)
	}()

	return addr, prov
}

// TestHostRemoteIntegration exercises the remote host end-to-end against a real runtime over WebTransport
func TestHostRemoteIntegration(t *testing.T) {
	hostCtx, hostCancel := context.WithCancel(t.Context())
	t.Cleanup(hostCancel)

	runtimeAddr, _ := startTestRuntime(t, hostCtx)

	alarmCh := make(chan string, 1)

	host, err := NewHost(
		WithAddress(freeUDPAddr(t)),
		WithRuntimeAddresses(runtimeAddr),
		WithServerTLSInsecureSkipTLSValidation(),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)

	err = host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID, alarmCh: alarmCh}
	}, RegisterActorOptions{})
	require.NoError(t, err)

	hostErr := make(chan error, 1)
	go func() {
		hostErr <- host.Run(hostCtx)
	}()

	// Wait until the host has registered with the runtime
	select {
	case <-host.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host did not connect to the runtime")
	}
	require.NotEmpty(t, host.HostID())

	svc := host.Service()

	t.Run("invoke activates and calls the actor", func(t *testing.T) {
		res, err := svc.Invoke(t.Context(), "T", "a1", "echo", "hi")
		require.NoError(t, err)

		var out string
		require.NoError(t, res.Decode(&out))
		assert.Equal(t, "echo:hi", out)
	})

	t.Run("state round-trips through the runtime", func(t *testing.T) {
		_, err := svc.Invoke(t.Context(), "T", "a1", "setstate", nil)
		require.NoError(t, err)

		res, err := svc.Invoke(t.Context(), "T", "a1", "getstate", nil)
		require.NoError(t, err)

		var out string
		require.NoError(t, res.Decode(&out))
		assert.Equal(t, "saved-value", out)
	})

	t.Run("alarm round-trips and fires on the host", func(t *testing.T) {
		// The actor must be active so the runtime can resolve its placement when dispatching the alarm
		_, err := svc.Invoke(t.Context(), "T", "a1", "echo", "warmup")
		require.NoError(t, err)

		// An already-due alarm makes the runtime dispatch it back to this host
		err = svc.SetAlarm(t.Context(), "T", "a1", "wake", actor.AlarmProperties{
			DueTime: time.Now().Add(-time.Second),
		})
		require.NoError(t, err)

		select {
		case name := <-alarmCh:
			assert.Equal(t, "wake", name)
		case <-time.After(10 * time.Second):
			t.Fatal("alarm did not fire on the host")
		}
	})

	t.Run("deleting a missing alarm reports not found", func(t *testing.T) {
		err := svc.DeleteAlarm(t.Context(), "T", "a1", "does-not-exist")
		require.ErrorIs(t, err, actor.ErrAlarmNotFound)
	})

	// Shut down the host and confirm Run returns
	hostCancel()
	select {
	case <-hostErr:
	case <-time.After(10 * time.Second):
		t.Fatal("host did not shut down")
	}
}

// TestHostRemoteMultiHostPeerInvocation verifies a host invoking an actor the runtime places on a different host routes peer-to-peer
func TestHostRemoteMultiHostPeerInvocation(t *testing.T) {
	runtimeAddr, _ := startTestRuntime(t, t.Context())

	// Host A registers no actor types, so it can only reach actors by routing to peers
	hostA := newRemoteHost(t, runtimeAddr)

	// Host B owns actor type "T", so the runtime can only place "T" actors there
	hostB := newRemoteHost(t, runtimeAddr)
	invokeCh := make(chan string, 4)
	err := hostB.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID, label: "B:", invokeCh: invokeCh}
	}, RegisterActorOptions{})
	require.NoError(t, err)

	runRemoteHost(t, hostB)
	runRemoteHost(t, hostA)

	// Host A invokes "T", which the runtime places on host B, so the call must traverse the peer transport
	res, err := hostA.Service().Invoke(t.Context(), "T", "peer1", "echo", "hi")
	require.NoError(t, err)

	var out string
	require.NoError(t, res.Decode(&out))
	assert.Equal(t, "B:echo:hi", out, "the actor ran on host B and the result returned to host A")

	// Confirm host B actually executed the invocation
	select {
	case method := <-invokeCh:
		assert.Equal(t, "echo", method)
	case <-time.After(5 * time.Second):
		t.Fatal("host B did not execute the invocation")
	}

	// The placement host A cached must point at host B, not itself
	ap, err := hostA.lookupActor(t.Context(), ref.NewActorRef("T", "peer1"), false, false)
	require.NoError(t, err)
	assert.Equal(t, hostB.HostID(), ap.HostID)
	assert.False(t, hostA.isLocal(ap))
}

// TestHostRemoteStalePlacementReResolves verifies that a stale cached placement routing a call to the wrong host is detected by the ownership confirmation, re-resolved to the real owner, and never double-activates the actor
func TestHostRemoteStalePlacementReResolves(t *testing.T) {
	runtimeAddr, _ := startTestRuntime(t, t.Context())

	// Both hosts can run "T", so the actor could be wrongly activated on either if ownership were not confirmed
	hostA := newRemoteHost(t, runtimeAddr)
	require.NoError(t, hostA.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID, label: "A:"}
	}, RegisterActorOptions{}))

	hostB := newRemoteHost(t, runtimeAddr)
	require.NoError(t, hostB.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID, label: "B:"}
	}, RegisterActorOptions{}))

	runRemoteHost(t, hostA)
	runRemoteHost(t, hostB)

	aRef := ref.NewActorRef("T", "x")

	// Activate the actor so the runtime fixes its placement on one host
	_, err := hostA.Service().Invoke(t.Context(), "T", "x", "echo", "warmup")
	require.NoError(t, err)

	// Discover the real owner and pick the other host as the caller routing to a stale placement
	placement, err := hostA.lookupActor(t.Context(), aRef, true, false)
	require.NoError(t, err)
	caller, ownerLabel := hostB, "A:"
	if placement.HostID == hostB.HostID() {
		caller, ownerLabel = hostA, "B:"
	}

	// Poison the caller's placement cache to claim it owns the actor, simulating a stale cached placement
	caller.placementCache.Set(aRef.String(), &actorcore.Placement{
		HostID:  caller.HostID(),
		Address: caller.address,
	}, time.Minute)

	// The invocation must detect the stale local placement, re-resolve through the runtime, and run on the real owner
	res, err := caller.Service().Invoke(t.Context(), "T", "x", "echo", "hi")
	require.NoError(t, err)
	var out string
	require.NoError(t, res.Decode(&out))
	assert.Equal(t, ownerLabel+"echo:hi", out)

	// The actor must never have been activated on the non-owning caller
	_, activatedOnCaller := caller.core.Actors.Get(aRef.String())
	assert.False(t, activatedOnCaller, "the actor must not be double-activated on the non-owning caller")
}

// TestHostRemoteIdleDeactivation verifies an idle actor is deactivated and its placement is cleared at the runtime
func TestHostRemoteIdleDeactivation(t *testing.T) {
	runtimeAddr, prov := startTestRuntime(t, t.Context())

	host := newRemoteHost(t, runtimeAddr)
	deactivateCh := make(chan string, 1)
	err := host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID, deactivateCh: deactivateCh}
	}, RegisterActorOptions{
		// A short idle timeout so the actor deactivates quickly
		IdleTimeout: 250 * time.Millisecond,
	})
	require.NoError(t, err)

	runRemoteHost(t, host)

	// Activate the actor on the host
	_, err = host.Service().Invoke(t.Context(), "T", "idle1", "echo", "hi")
	require.NoError(t, err)

	// The actor goes idle and is deactivated, which calls its Deactivate method
	select {
	case <-deactivateCh:
	case <-time.After(5 * time.Second):
		t.Fatal("idle actor was not deactivated")
	}

	// Deactivation notifies the runtime, which clears the placement in the provider
	// We check the provider directly: the Deactivate signal fires before the RemoveActor round-trip completes, and an active-only Invoke would re-activate the actor through the still-warm placement cache
	aRef := ref.NewActorRef("T", "idle1")
	require.Eventually(t, func() bool {
		_, lookupErr := prov.LookupActor(t.Context(), aRef, components.LookupActorOpts{ActiveOnly: true})
		return errors.Is(lookupErr, components.ErrNoActor)
	}, 5*time.Second, 20*time.Millisecond, "the actor's placement should be cleared at the runtime after deactivation")
}

// TestHostRemoteInvokeErrors covers invocation failure modes surfaced through the remote host
func TestHostRemoteInvokeErrors(t *testing.T) {
	runtimeAddr, _ := startTestRuntime(t, t.Context())

	host := newRemoteHost(t, runtimeAddr)
	err := host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return &testActor{svc: service, id: actorID}
	}, RegisterActorOptions{})
	require.NoError(t, err)

	runRemoteHost(t, host)

	svc := host.Service()

	t.Run("unsupported actor type", func(t *testing.T) {
		_, err := svc.Invoke(t.Context(), "Unsupported", "a1", "echo", "hi")
		require.ErrorIs(t, err, actor.ErrActorTypeUnsupported)
	})

	t.Run("active-only invoke of an inactive actor", func(t *testing.T) {
		_, err := host.Invoke(t.Context(), "T", "never-active", "echo", "hi", actor.WithInvokeActiveOnly())
		require.ErrorIs(t, err, actor.ErrActorNotActive)
	})

	t.Run("error returned by the actor is propagated", func(t *testing.T) {
		_, err := svc.Invoke(t.Context(), "T", "a1", "fail", nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "boom")
	})
}

// TestHostRemoteStreamInvocation exercises streamed invocation locally and across hosts
func TestHostRemoteStreamInvocation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runtimeAddr, _ := startTestRuntime(t, ctx)

	t.Run("same-host stream", func(t *testing.T) {
		host := newRemoteHost(t, runtimeAddr)
		err := host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
			return &testActor{svc: service, id: actorID}
		}, RegisterActorOptions{})
		require.NoError(t, err)
		runRemoteHost(t, host)

		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		ct, resp, err := host.Service().InvokeStream(reqCtx, "T", "s1", "echo", "text/plain", strings.NewReader("hi"))
		require.NoError(t, err)
		defer resp.Close()

		assert.Equal(t, "text/plain", ct)
		got, err := io.ReadAll(resp)
		require.NoError(t, err)
		assert.Equal(t, "stream:hi", string(got))
	})

	t.Run("cross-host peer stream", func(t *testing.T) {
		hostA := newRemoteHost(t, runtimeAddr)
		hostB := newRemoteHost(t, runtimeAddr)
		err := hostB.RegisterActor("S", func(actorID string, service *actor.Service) actor.Actor {
			return &testActor{svc: service, id: actorID, label: "B:"}
		}, RegisterActorOptions{})
		require.NoError(t, err)

		runRemoteHost(t, hostB)
		runRemoteHost(t, hostA)

		reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
		defer reqCancel()

		// Host A streams to "S", which the runtime places on host B, so it traverses the peer transport
		ct, resp, err := hostA.Service().InvokeStream(reqCtx, "S", "s1", "echo", "application/test", strings.NewReader("ping"))
		require.NoError(t, err)
		defer resp.Close()

		assert.Equal(t, "application/test", ct)
		got, err := io.ReadAll(resp)
		require.NoError(t, err)
		assert.Equal(t, "B:stream:ping", string(got))
	})
}
