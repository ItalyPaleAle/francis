package local

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/ref"
)

// localTestRuntimePSK is the shared runtime PSK the local integration tests derive their cluster CA from
var localTestRuntimePSK = []byte("local-test-runtime-psk-0123456789")

// runLocalHost starts the host and waits until it has registered with the provider, cleaning it up when the test ends
func runLocalHost(t *testing.T, host *Host) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
	case runErr := <-errCh:
		t.Fatalf("host stopped before becoming ready: %v", runErr)
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}

	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("host did not shut down")
		}
	})
}

// localFreeUDPAddr returns a localhost address with a currently-free UDP port
func localFreeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pc.LocalAddr().String()
	require.NoError(t, pc.Close())
	return addr
}

// smokeActor is a minimal actor that echoes the decoded request back as the result
type smokeActor struct{}

func (smokeActor) Invoke(_ context.Context, method string, data actor.Envelope) (any, error) {
	var s string
	if data != nil {
		_ = data.Decode(&s)
	}
	return method + ":" + s, nil
}

// TestHostLocalMultiHostPeerInvocation verifies a local host invoking an actor the provider places on another host routes peer-to-peer over WebTransport
func TestHostLocalMultiHostPeerInvocation(t *testing.T) {
	// Both hosts share one SQLite database so they see the same placement table
	dbPath := filepath.Join(t.TempDir(), "shared.db")

	newSharedHost := func(register bool) *Host {
		host, err := NewHost(
			WithAddress(localFreeUDPAddr(t)),
			WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
			// Both hosts share the same runtime PSK so they derive the same CA and authenticate each other with mTLS
			WithRuntimePSKs(localTestRuntimePSK),
			WithLogger(slog.New(slog.DiscardHandler)))
		require.NoError(t, err)
		if register {
			err = host.RegisterActor("S", func(actorID string, service *actor.Service) actor.Actor {
				return smokeActor{}
			})
			require.NoError(t, err)
		}
		return host
	}

	// Host B owns actor type "S"
	// Host A registers nothing, so it can only reach the actor by routing to a peer
	hostB := newSharedHost(true)
	hostA := newSharedHost(false)

	runLocalHost(t, hostB)
	runLocalHost(t, hostA)

	// Host A invokes "S", which the provider places on host B, so the call must traverse the peer transport
	res, err := hostA.Service().Invoke(t.Context(), "S", "x", "echo", "hi")
	require.NoError(t, err)
	var out string
	require.NoError(t, res.Decode(&out))
	assert.Equal(t, "echo:hi", out)

	// The placement host A resolved must point at host B, not itself
	ap, err := hostA.lookupActor(t.Context(), ref.NewActorRef("S", "x"), true, false)
	require.NoError(t, err)
	assert.Equal(t, hostB.HostID(), ap.HostID)
	assert.False(t, hostA.isLocal(ap))
}

// TestHostLocalInvocationSmoke runs a real local host over WebTransport and confirms an actor invocation routes through the shared messaging path
func TestHostLocalInvocationSmoke(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "smoke.db")

	host, err := NewHost(
		WithAddress(localFreeUDPAddr(t)),
		WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
		WithRuntimePSKs(localTestRuntimePSK),
		WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, err)

	err = host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return smokeActor{}
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(ctx)
	}()

	// Wait until the host has registered with the provider
	select {
	case <-host.Ready():
	case runErr := <-errCh:
		t.Fatalf("host stopped before becoming ready: %v", runErr)
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}

	// Invoke an actor on the same host: this exercises the provider-backed resolver, the ownership confirmation, and local activation
	res, err := host.Service().Invoke(t.Context(), "T", "a1", "echo", "hi")
	require.NoError(t, err)
	var out string
	require.NoError(t, res.Decode(&out))
	assert.Equal(t, "echo:hi", out) // method "echo" + ":" + arg "hi"

	// Shut down the host and confirm Run returns
	cancel()
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("host did not shut down")
	}
}

// slowDeactivateActor is an actor whose deactivation takes longer than the host's health check deadline
type slowDeactivateActor struct {
	smokeActor

	started chan struct{}
	delay   time.Duration
}

func (a slowDeactivateActor) Deactivate(ctx context.Context) error {
	close(a.started)
	time.Sleep(a.delay)
	return nil
}

// TestHostLocalKeepsHealthChecksWhileActorsHalt verifies a host stays registered while its actors take longer to halt than the health check deadline
// If the registration expired meanwhile, the provider would let another host activate actors that are still halting here
func TestHostLocalKeepsHealthChecksWhileActorsHalt(t *testing.T) {
	// Use the normal SQLite request timeout while ensuring actor shutdown still outlasts the health deadline
	const deadline = 2 * sqlite.DefaultTimeout
	dbPath := filepath.Join(t.TempDir(), "health.db")

	h, err := NewHost(
		WithAddress(localFreeUDPAddr(t)),
		WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
		WithRuntimePSKs(localTestRuntimePSK),
		WithHostHealthCheckDeadline(deadline),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)
	started := make(chan struct{})
	err = h.RegisterActor("Slow", func(actorID string, service *actor.Service) actor.Actor {
		return slowDeactivateActor{started: started, delay: 2 * deadline}
	})
	require.NoError(t, err)

	// Run the host and activate the actor
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.Run(ctx)
	}()
	select {
	case <-h.Ready():
	case runErr := <-errCh:
		t.Fatalf("host stopped before becoming ready: %v", runErr)
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}
	hostID := h.HostID()
	_, err = h.Service().Invoke(t.Context(), "Slow", "a1", "echo", "hi")
	require.NoError(t, err)

	// A second provider on the same database watches the registration from outside the host
	watcherCfg := components.NewProviderConfig()
	watcherCfg.HostHealthCheckDeadline = deadline
	watcher, err := sqlite.NewSQLiteProvider(slog.New(slog.DiscardHandler), sqlite.SQLiteProviderOptions{ConnectionString: dbPath}, watcherCfg)
	require.NoError(t, err)
	err = watcher.Init(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = watcher.Close()
	})

	// Stop the host, which starts halting the actor
	cancel()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the actor was not deactivated")
	}

	// Well past the deadline, the host is still registered, because its health checks keep running while the actor halts
	time.Sleep(deadline + deadline/2)
	_, err = watcher.GetHostDetails(t.Context(), hostID)
	require.NoError(t, err, "the host's registration expired while its actor was still halting")

	select {
	case err = <-errCh:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("host did not stop")
	}
}
