package remote

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/runtime"
)

// testActor is a minimal actor used by the integration test
// It echoes invocations, round-trips its state through the runtime, and signals when its alarm fires
type testActor struct {
	svc     *actor.Service
	id      string
	alarmCh chan string
}

func (a *testActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case "echo":
		var in string
		_ = data.Decode(&in)
		return "echo:" + in, nil
	case "setstate":
		return nil, a.svc.SetState(ctx, "T", a.id, "saved-value", nil)
	case "getstate":
		var out string
		err := a.svc.GetState(ctx, "T", a.id, &out)
		return out, err
	default:
		return nil, nil
	}
}

func (a *testActor) Alarm(_ context.Context, name string, _ actor.Envelope) error {
	a.alarmCh <- name
	return nil
}

// startTestRuntime starts an in-memory runtime over WebTransport and returns its address
func startTestRuntime(t *testing.T, ctx context.Context) string {
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

	return addr
}

// TestHostRemoteIntegration exercises the remote host end-to-end against a real runtime over WebTransport
func TestHostRemoteIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runtimeAddr := startTestRuntime(t, ctx)

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
		hostErr <- host.Run(ctx)
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
		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		res, err := svc.Invoke(reqCtx, "T", "a1", "echo", "hi")
		require.NoError(t, err)

		var out string
		require.NoError(t, res.Decode(&out))
		assert.Equal(t, "echo:hi", out)
	})

	t.Run("state round-trips through the runtime", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		_, err := svc.Invoke(reqCtx, "T", "a1", "setstate", nil)
		require.NoError(t, err)

		res, err := svc.Invoke(reqCtx, "T", "a1", "getstate", nil)
		require.NoError(t, err)

		var out string
		require.NoError(t, res.Decode(&out))
		assert.Equal(t, "saved-value", out)
	})

	t.Run("alarm round-trips and fires on the host", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		// The actor must be active so the runtime can resolve its placement when dispatching the alarm
		_, err := svc.Invoke(reqCtx, "T", "a1", "echo", "warmup")
		require.NoError(t, err)

		// An already-due alarm makes the runtime dispatch it back to this host
		err = svc.SetAlarm(reqCtx, "T", "a1", "wake", actor.AlarmProperties{
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
		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		err := svc.DeleteAlarm(reqCtx, "T", "a1", "does-not-exist")
		require.ErrorIs(t, err, actor.ErrAlarmNotFound)
	})

	// Shut down the host and confirm Run returns
	cancel()
	select {
	case <-hostErr:
	case <-time.After(10 * time.Second):
		t.Fatal("host did not shut down")
	}
}
