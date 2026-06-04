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
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/ref"
)

// runLocalHost starts the host and waits until it has registered with the provider, cleaning it up when the test ends
func runLocalHost(t *testing.T, host *Host) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
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
	const sharedKey = "multihost-shared-key-1234567890"

	newSharedHost := func(register bool) *Host {
		host, err := NewHost(
			WithAddress(localFreeUDPAddr(t)),
			WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
			WithPeerAuthenticationSharedKey(sharedKey),
			WithServerTLSInsecureSkipTLSValidation(),
			WithLogger(slog.New(slog.DiscardHandler)),
		)
		require.NoError(t, err)
		if register {
			err = host.RegisterActor("S", func(actorID string, service *actor.Service) actor.Actor {
				return smokeActor{}
			}, RegisterActorOptions{})
			require.NoError(t, err)
		}
		return host
	}

	// Host B owns actor type "S"; host A registers nothing, so it can only reach the actor by routing to a peer
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
		WithPeerAuthenticationSharedKey("smoke-shared-key-0123456789"),
		WithServerTLSInsecureSkipTLSValidation(),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)

	err = host.RegisterActor("T", func(actorID string, service *actor.Service) actor.Actor {
		return smokeActor{}
	}, RegisterActorOptions{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(ctx)
	}()

	// Wait until the host has registered with the provider
	select {
	case <-host.Ready():
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
