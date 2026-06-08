package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/channelbind"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// freeUDPAddr returns a localhost address with a currently-free UDP port
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pc.LocalAddr().String()
	require.NoError(t, pc.Close())
	return addr
}

// runRuntimeServer starts a Runtime over WebTransport on a free port and returns it, its provider, and its address
func runRuntimeServer(t *testing.T) (*Runtime, *standalone.StandaloneMemory, string) {
	t.Helper()

	addr := freeUDPAddr(t)
	prov, err := standalone.NewStandaloneMemory(slog.New(slog.DiscardHandler), standalone.StandaloneMemoryOptions{}, components.ProviderConfig{
		HostHealthCheckDeadline:   20 * time.Second,
		AlarmsLeaseDuration:       20 * time.Second,
		AlarmsFetchAheadInterval:  2500 * time.Millisecond,
		AlarmsFetchAheadBatchSize: 25,
	})
	require.NoError(t, err)

	// A long poll interval keeps the alarm fetcher quiet, since these tests only exercise the session lifecycle
	rt, err := NewRuntime(prov, append([]RuntimeOption{WithBind(addr), WithAlarmsPollInterval(time.Hour)}, baseRuntimeOpts()...)...)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() {
		_ = rt.Run(ctx)
	}()

	return rt, prov, addr
}

// dialRuntime dials the runtime's WebTransport endpoint, retrying until the server is accepting connections
func dialRuntime(t *testing.T, ctx context.Context, addr string) *webtransport.Session {
	t.Helper()

	// The host verifies the runtime, so a test dialer skips verification and presents no client certificate, exercising the bootstrap path
	cliTLS := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{http3.NextProtoH3},
		InsecureSkipVerify: true,
	}
	dialer := wt.NewDialer(cliTLS)
	t.Cleanup(func() {
		_ = dialer.Close()
	})

	url := "https://" + addr + protocol.RuntimeConnectPath
	deadline := time.Now().Add(15 * time.Second)
	for {
		dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
		// The returned session owns the underlying stream, so the HTTP response body must not be closed here
		rsp, session, derr := dialer.Dial(dctx, url, nil) //nolint:bodyclose
		dcancel()
		if derr == nil && rsp.StatusCode >= 200 && rsp.StatusCode < 300 {
			return session
		}
		if session != nil {
			_ = session.CloseWithError(0, "")
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("could not dial runtime at %s: %v", addr, derr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// registerOnSession runs the PSK bootstrap handshake on the session's first stream and returns the runtime's response
func registerOnSession(t *testing.T, ctx context.Context, session *webtransport.Session) protocol.RegisterHostResponse {
	t.Helper()

	stream, err := session.OpenStreamSync(ctx)
	require.NoError(t, err)
	defer stream.Close()

	// Run the PSK challenge-response, bound to this session's channel binding
	psk, err := bootstrapauth.NewPSK(testHostPSK)
	require.NoError(t, err)
	cb, err := channelbind.Export(session)
	require.NoError(t, err)
	clientNonce, err := bootstrapauth.Nonce()
	require.NoError(t, err)

	begin, err := protocol.NewRequest(protocol.KindRegisterHostAuth, protocol.RegisterAuthBeginRequest{
		Method:      bootstrapauth.MethodPSK,
		ClientNonce: clientNonce,
	})
	require.NoError(t, err)
	challengeEnv, err := protocol.RoundTrip(ctx, stream, begin)
	require.NoError(t, err)
	_, isErr := challengeEnv.AsError()
	require.False(t, isErr)
	var challenge protocol.RegisterAuthChallengeResponse
	require.NoError(t, challengeEnv.DecodePayload(&challenge))
	require.True(t, psk.VerifyServerProof(cb, clientNonce, challenge.ServerNonce, challenge.ServerProof))

	// Generate a workload key the runtime signs into a certificate
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	proof := psk.ClientProof(cb, clientNonce, challenge.ServerNonce)

	req, err := protocol.NewRequest(protocol.KindRegisterHost, protocol.RegisterHostRequest{
		Address:        "10.9.0.1:1",
		ActorTypes:     []protocol.ActorHostType{{ActorType: "T", IdleTimeoutMs: 60000}},
		Auth:           protocol.RegisterAuth{Method: bootstrapauth.MethodPSK, Proof: proof},
		WorkloadPubKey: pub,
	})
	require.NoError(t, err)

	resp, err := protocol.RoundTrip(ctx, stream, req)
	require.NoError(t, err)
	perr, isErr := resp.AsError()
	require.False(t, isErr, "registration should succeed: %v", perr)

	var out protocol.RegisterHostResponse
	err = resp.DecodePayload(&out)
	require.NoError(t, err)
	return out
}

// unregisterOnSession sends a graceful UnregisterHost on a new stream, stamped with the host identity the runtime assigned
func unregisterOnSession(t *testing.T, ctx context.Context, session *webtransport.Session, hostID string, sessionID string) {
	t.Helper()

	stream, err := session.OpenStreamSync(ctx)
	require.NoError(t, err)
	defer stream.Close()

	req, err := protocol.NewRequest(protocol.KindUnregisterHost, protocol.UnregisterHostRequest{})
	require.NoError(t, err)
	req.HostID = hostID
	req.SessionID = sessionID

	resp, err := protocol.RoundTrip(ctx, stream, req)
	require.NoError(t, err)
	_, isErr := resp.AsError()
	require.False(t, isErr)
	assert.Equal(t, protocol.KindUnregisterHostResponse, resp.Kind)
}

func TestServeSessionRegistersAndRemovesHostOnUngracefulDisconnect(t *testing.T) {
	rt, prov, addr := runRuntimeServer(t)

	session := dialRuntime(t, t.Context(), addr)
	reg := registerOnSession(t, t.Context(), session)
	require.NotEmpty(t, reg.HostID)

	// serveSession registered the host in the manager as part of the handshake
	_, ok := rt.hosts.Get(reg.HostID)
	require.True(t, ok, "the host should be tracked after registration")

	// Closing the session without unregistering is an ungraceful disconnect
	err := session.CloseWithError(0, "")
	require.NoError(t, err)

	// serveSession's teardown removes the host from the manager
	require.Eventually(t, func() bool {
		_, ok := rt.hosts.Get(reg.HostID)
		return !ok
	}, 10*time.Second, 50*time.Millisecond, "the host should be removed from the manager on disconnect")

	// An ungraceful disconnect leaves the provider registration to the health deadline, so the host is still eligible for placement
	_, err = prov.LookupActor(t.Context(), ref.NewActorRef("T", "x"), components.LookupActorOpts{})
	require.NoError(t, err, "an ungraceful disconnect must not unregister the host from the provider")
}

func TestServeSessionUnregistersDrainedHostFromProvider(t *testing.T) {
	rt, prov, addr := runRuntimeServer(t)

	session := dialRuntime(t, t.Context(), addr)
	reg := registerOnSession(t, t.Context(), session)
	require.NotEmpty(t, reg.HostID)

	// A graceful UnregisterHost marks the host draining without removing it yet
	unregisterOnSession(t, t.Context(), session, reg.HostID, reg.SessionID)

	// Closing the session after unregistering signals the host is completely done
	err := session.CloseWithError(0, "")
	require.NoError(t, err)

	// serveSession's teardown removes the host from the manager
	require.Eventually(t, func() bool {
		_, ok := rt.hosts.Get(reg.HostID)
		return !ok
	}, 10*time.Second, 50*time.Millisecond, "the host should be removed from the manager on disconnect")

	// Because the host drained gracefully, its session closing unregisters it from the provider, so no host is left to place "T" actors
	require.Eventually(t, func() bool {
		_, rErr := prov.LookupActor(t.Context(), ref.NewActorRef("T", "x"), components.LookupActorOpts{})
		return errors.Is(rErr, components.ErrNoHost)
	}, 10*time.Second, 50*time.Millisecond, "a gracefully drained host should be unregistered from the provider once its session closes")
}

func TestServeSessionRejectsNonRegistrationFirstMessage(t *testing.T) {
	rt, _, addr := runRuntimeServer(t)

	session := dialRuntime(t, t.Context(), addr)

	// The first stream of a session must carry the registration handshake, anything else fails it
	stream, err := session.OpenStreamSync(t.Context())
	require.NoError(t, err)
	req, err := protocol.NewRequest(protocol.KindHealthCheck, protocol.HealthCheckRequest{})
	require.NoError(t, err)
	resp, err := protocol.RoundTrip(t.Context(), stream, req)
	_ = stream.Close()

	// The runtime replies with a bad-request error before closing the session
	// The close may race the reply, so only assert the code when a reply arrives
	if err == nil {
		perr, isErr := resp.AsError()
		require.True(t, isErr)
		assert.Equal(t, protocol.ErrCodeBadRequest, perr.Code)
	}

	// No host was ever registered, and serveSession closes the session after the failed handshake
	assert.Zero(t, rt.hosts.Count(), "a failed handshake must not register a host")
	require.Eventually(t, func() bool {
		select {
		case <-session.Context().Done():
			return true
		default:
			return false
		}
	}, 10*time.Second, 50*time.Millisecond, "the runtime should close a session whose first message is not a registration")
}

func TestHandleHostDisconnectGracefulUnregisters(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.0.0.21:1", protocol.ActorHostType{ActorType: "T"})

	aref := ref.NewActorRef("T", "a1")
	_, err := prov.LookupActor(t.Context(), aref, components.LookupActorOpts{})
	require.NoError(t, err)

	// A host that drained gracefully is completely done once its session closes
	c.setDraining()
	rt.handleHostDisconnect(c)

	// It is gone from the host manager and unregistered from the provider, taking its active-actor placements with it
	_, ok := rt.hosts.Get(c.hostID)
	assert.False(t, ok)
	_, err = prov.LookupActor(t.Context(), aref, components.LookupActorOpts{ActiveOnly: true})
	require.ErrorIs(t, err, components.ErrNoActor, "a gracefully drained host must be unregistered, clearing its placements")
}

func TestHandleHostDisconnectUngracefulKeepsRegistration(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.0.0.22:1", protocol.ActorHostType{ActorType: "T"})

	aref := ref.NewActorRef("T", "a1")
	_, err := prov.LookupActor(t.Context(), aref, components.LookupActorOpts{})
	require.NoError(t, err)

	// An ungraceful disconnect (the host never drained) leaves the provider record to the health deadline, since the host may reconnect and reattach
	rt.handleHostDisconnect(c)

	// The host is still removed from the in-memory manager, but its provider registration and active actor survive
	_, ok := rt.hosts.Get(c.hostID)
	assert.False(t, ok, "the host is removed from the in-memory manager")
	_, err = prov.LookupActor(t.Context(), aref, components.LookupActorOpts{
		ActiveOnly: true,
	})
	require.NoError(t, err, "an ungraceful disconnect must not unregister the host from the provider")
}

func TestHandleHostDisconnectIgnoresSupersededSession(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.0.0.23:1", protocol.ActorHostType{ActorType: "T"})

	// A newer session for the same host supersedes the original, so the original conn (session s1) is now stale
	newer := &hostConn{
		hostID:    c.hostID,
		sessionID: "s2",
		address:   c.address,
	}
	newer.actorTypes.Store(c.actorTypes.Load())
	rt.hosts.Register(newer)

	aref := ref.NewActorRef("T", "a1")
	_, err := prov.LookupActor(t.Context(), aref, components.LookupActorOpts{})
	require.NoError(t, err)

	// Even though the stale conn was draining, tearing it down must not evict the newer session or touch the provider
	c.setDraining()
	rt.handleHostDisconnect(c)

	// The newer session is still tracked
	got, ok := rt.hosts.Get(c.hostID)
	require.True(t, ok, "the newer session must remain tracked")
	assert.Equal(t, "s2", got.sessionID)

	// The provider registration is untouched, since the stale teardown did not own the current session
	_, err = prov.LookupActor(t.Context(), aref, components.LookupActorOpts{
		ActiveOnly: true,
	})
	require.NoError(t, err, "a superseded session's teardown must not unregister the host")
}
