package remote

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
	"github.com/italypaleale/francis/runtime"
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

// testBootstrap returns a fresh holder, the host bootstrap PSK, and the runtime client TLS config bound to that holder
func testBootstrap(t *testing.T) (*certholder.Holder, *bootstrapauth.PSK, *tls.Config) {
	t.Helper()
	holder := certholder.New(nil, nil)
	psk, err := bootstrapauth.NewPSK(testHostPSK)
	require.NoError(t, err)
	return holder, psk, hosttls.RuntimeClientTLSConfig(holder)
}

// testRuntimeServerTLS builds a server TLS config presenting a runtime certificate signed by the test CA
func testRuntimeServerTLS(t *testing.T) *tls.Config {
	t.Helper()
	cas, err := ca.CABundle([][]byte{testRuntimePSK})
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := cas[0].IssueWorkloadCert(ca.RuntimeURI("test"), pub, time.Hour)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}},
	}
}

// TestRuntimeClientIntegration exercises the host-to-runtime client against a real runtime over WebTransport
func TestRuntimeClientIntegration(t *testing.T) {
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
		runtime.WithRuntimePSKs(testRuntimePSK),
		runtime.WithHostBootstrapPSK(testHostPSK),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	rtErr := make(chan error, 1)
	go func() {
		rtErr <- rt.Run(ctx)
	}()

	// The client bootstraps with the host PSK, then reconnects over mTLS using the issued certificate
	holder, psk, clientTLS := testBootstrap(t)

	alarmCh := make(chan protocol.ExecuteAlarmRequest, 1)
	rc := newRuntimeClient(runtimeClientConfig{
		addresses:    []string{addr},
		peerAddress:  "127.0.0.1:7000",
		actorTypes:   []protocol.ActorHostType{{ActorType: "T", IdleTimeoutMs: 60000}},
		tlsConfig:    clientTLS,
		holder:       holder,
		bootstrapPSK: psk,
		minBackoff:   50 * time.Millisecond,
		log:          slog.New(slog.DiscardHandler),
		handlers: runtimeHandlers{
			executeAlarm: func(_ context.Context, req protocol.ExecuteAlarmRequest) (protocol.ExecuteAlarmResponse, *protocol.Error) {
				select {
				case alarmCh <- req:
				default:
				}
				return protocol.ExecuteAlarmResponse{ExecutionTimeUnixMs: time.Now().UnixMilli()}, nil
			},
		},
	})
	go func() {
		_ = rc.Run(ctx)
	}()

	// Wait until the client has registered with the runtime
	select {
	case <-rc.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("client did not connect to the runtime")
	}
	require.NotEmpty(t, rc.HostID())

	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	// State round-trips through the runtime
	require.NoError(t, rc.SetState(reqCtx, protocol.SetStateRequest{
		ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		Data:     []byte("hello"),
	}))
	st, err := rc.GetState(reqCtx, protocol.GetStateRequest{ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"}})
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), st.Data)

	// A lookup resolves to this host's peer address
	lk, err := rc.LookupActor(reqCtx, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
	require.NoError(t, err)
	assert.Equal(t, rc.HostID(), lk.HostID)
	assert.Equal(t, "127.0.0.1:7000", lk.Address)

	// Setting an already-due alarm makes the runtime dispatch ExecuteAlarm back to this host
	require.NoError(t, rc.SetAlarm(reqCtx, protocol.SetAlarmRequest{
		AlarmRef:        protocol.AlarmRef{ActorType: "T", ActorID: "a1", Name: "wake"},
		AlarmProperties: protocol.AlarmProperties{DueTimeUnixMs: time.Now().Add(-time.Second).UnixMilli()},
	}))

	select {
	case got := <-alarmCh:
		assert.Equal(t, "a1", got.ActorID)
		assert.Equal(t, "wake", got.Name)
	case <-time.After(10 * time.Second):
		t.Fatal("runtime did not dispatch the due alarm to the host")
	}

	// A missing-state lookup surfaces the structured protocol error
	_, err = rc.GetState(reqCtx, protocol.GetStateRequest{ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "missing"}})
	require.ErrorIs(t, err, protocol.NewError(protocol.ErrCodeStateNotFound, ""))

	// Shut down and confirm the runtime stops cleanly
	cancel()
	select {
	case <-rtErr:
	case <-time.After(10 * time.Second):
		t.Fatal("runtime did not shut down")
	}
}

// startRejectingRuntime starts a minimal WebTransport server that rejects every registration with a permanent protocol-version error
// It returns the address the client should dial
func startRejectingRuntime(t *testing.T) string {
	t.Helper()

	addr := freeUDPAddr(t)
	serverTLS := testRuntimeServerTLS(t)

	mux := http.NewServeMux()
	wtServer := wt.NewServer(addr, serverTLS, mux)
	mux.HandleFunc(protocol.RuntimeConnectPath, func(w http.ResponseWriter, r *http.Request) {
		session, uErr := wtServer.Upgrade(w, r)
		if uErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// The first stream carries the registration handshake - read it and reject it
		stream, sErr := session.AcceptStream(r.Context())
		if sErr != nil {
			return
		}
		req, rErr := protocol.ReadMessageWithTimeout(stream, 5*time.Second)
		if rErr != nil {
			_ = stream.Close()
			return
		}
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeProtocolVersion, "unsupported protocol version")))
		_ = stream.Close()

		// Hold the session open until the client tears it down after receiving the rejection
		<-session.Context().Done()
	})

	go func() {
		_ = wtServer.ListenAndServe()
	}()
	t.Cleanup(func() {
		_ = wtServer.Close()
	})

	return addr
}

// TestRuntimeClientCanReconnect verifies the reconnect-vs-bootstrap selection: only a certificate with enough lifetime left reconnects over mTLS
// An expired or soon-to-expire certificate must fall back to bootstrap, so an outage longer than the cert lifetime can never strand the host on a doomed mTLS reconnect loop
func TestRuntimeClientCanReconnect(t *testing.T) {
	now := time.Now()
	fakeClock := clocktesting.NewFakeClock(now)

	newClient := func(cert *tls.Certificate) *runtimeClient {
		holder := certholder.New(nil, nil)
		if cert != nil {
			holder.SetCertificate(cert)
		}
		return newRuntimeClient(runtimeClientConfig{
			holder: holder,
			clock:  fakeClock,
			log:    slog.New(slog.DiscardHandler),
		})
	}

	certExpiringAt := func(notAfter time.Time) *tls.Certificate {
		return &tls.Certificate{Leaf: &x509.Certificate{NotAfter: notAfter}}
	}

	t.Run("no certificate bootstraps", func(t *testing.T) {
		ok := newClient(nil).canReconnect()
		assert.False(t, ok)
	})

	t.Run("certificate without a parsed leaf bootstraps", func(t *testing.T) {
		ok := newClient(&tls.Certificate{}).canReconnect()
		assert.False(t, ok)
	})

	t.Run("certificate with ample lifetime reconnects", func(t *testing.T) {
		ok := newClient(certExpiringAt(now.Add(time.Hour))).canReconnect()
		assert.True(t, ok)
	})

	t.Run("expired certificate bootstraps", func(t *testing.T) {
		ok := newClient(certExpiringAt(now.Add(-time.Minute))).canReconnect()
		assert.False(t, ok)
	})

	t.Run("certificate within the reconnect margin bootstraps", func(t *testing.T) {
		ok := newClient(certExpiringAt(now.Add(reconnectCertMargin / 2))).canReconnect()
		assert.False(t, ok)
	})
}

// TestRuntimeClientFailsFastOnPermanentRegistrationRejection verifies that a permanent registration rejection stops the reconnect loop instead of spinning forever
func TestRuntimeClientFailsFastOnPermanentRegistrationRejection(t *testing.T) {
	addr := startRejectingRuntime(t)

	holder, psk, clientTLS := testBootstrap(t)

	rc := newRuntimeClient(runtimeClientConfig{
		addresses:    []string{addr},
		peerAddress:  "127.0.0.1:7001",
		actorTypes:   []protocol.ActorHostType{{ActorType: "T", IdleTimeoutMs: 60000}},
		tlsConfig:    clientTLS,
		holder:       holder,
		bootstrapPSK: psk,
		minBackoff:   50 * time.Millisecond,
		log:          slog.New(slog.DiscardHandler),
	})

	runErr := make(chan error, 1)
	go func() {
		runErr <- rc.Run(t.Context())
	}()

	// A permanent rejection must stop Run quickly rather than reconnecting forever
	select {
	case rErr := <-runErr:
		require.Error(t, rErr)
		assert.ErrorIs(t, rErr, errFatalRegistration)
		assert.True(t, isProtocolErrorCode(rErr, protocol.ErrCodeProtocolVersion), "the fatal error should carry the protocol-version code")
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not fail fast on a permanent registration rejection")
	}

	// Ready never closes, because the host never successfully registered
	select {
	case <-rc.Ready():
		t.Fatal("Ready must not close when registration is permanently rejected")
	default:
	}
}
