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
)

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
	require.Eventually(t, func() bool {
		return host.HostID() != ""
	}, 15*time.Second, 20*time.Millisecond, "host did not register")

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
