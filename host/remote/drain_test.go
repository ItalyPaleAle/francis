package remote

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/webtransport-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// scriptedRuntime is a fake runtime that accepts mTLS reconnects, answers host requests, and lets a test send requests to the connected host
type scriptedRuntime struct {
	addr string

	registrations atomic.Int32
	unregisters   atomic.Int32
	healthChecks  atomic.Int32

	// healthDeadlineMs is the health check deadline the runtime advertises, 60s unless a test lowers it before the host connects
	healthDeadlineMs atomic.Int64

	// sessionCh receives each session once it has registered
	sessionCh chan *webtransport.Session
}

// startScriptedRuntime starts a scriptedRuntime that reattaches the host as host-1 on every registration
func startScriptedRuntime(t *testing.T) *scriptedRuntime {
	t.Helper()

	rt := &scriptedRuntime{
		addr:      freeUDPAddr(t),
		sessionCh: make(chan *webtransport.Session, 8),
	}
	rt.healthDeadlineMs.Store(60_000)

	mux := http.NewServeMux()
	wtServer := wt.NewServer(rt.addr, testRuntimeServerTLS(t), mux)
	mux.HandleFunc(protocol.RuntimeConnectPath, func(w http.ResponseWriter, r *http.Request) {
		session, rErr := wtServer.Upgrade(w, r)
		if rErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// The first stream carries the registration, which always reattaches
		stream, rErr := session.AcceptStream(r.Context())
		if rErr != nil {
			return
		}
		req, rErr := protocol.ReadMessageWithTimeout(stream, 5*time.Second)
		if rErr != nil {
			_ = stream.Close()
			return
		}
		rt.registrations.Add(1)
		resp, _ := req.ReplyWith(protocol.KindRegisterHostResponse, protocol.RegisterHostResponse{
			HostID:                "host-1",
			SessionID:             "session-1",
			Reattached:            true,
			HealthCheckDeadlineMs: rt.healthDeadlineMs.Load(),
		})
		_ = protocol.WriteMessage(stream, resp)
		_ = stream.Close()

		rt.sessionCh <- session

		// Answer every later host request, recording unregistrations
		for {
			s, rErr := session.AcceptStream(session.Context())
			if rErr != nil {
				return
			}
			go func() {
				defer s.Close()
				in, rErr := protocol.ReadMessageWithTimeout(s, 5*time.Second)
				if rErr != nil {
					return
				}
				switch in.Kind {
				case protocol.KindUnregisterHost:
					rt.unregisters.Add(1)
					_ = protocol.WriteMessage(s, in.Reply(protocol.KindUnregisterHostResponse, nil))
				case protocol.KindHealthCheck:
					rt.healthChecks.Add(1)
					_ = protocol.WriteMessage(s, in.Reply(protocol.KindHealthCheckResponse, nil))
				default:
					_ = protocol.WriteMessage(s, in.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "unexpected kind")))
				}
			}()
		}
	})

	go func() {
		_ = wtServer.ListenAndServe()
	}()
	t.Cleanup(func() {
		_ = wtServer.Close()
	})

	return rt
}

// send sends one request to the host over the session and returns the reply
func (rt *scriptedRuntime) send(t *testing.T, session *webtransport.Session, kind string, payload any) *protocol.Envelope {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req, err := protocol.NewRequest(kind, payload)
	require.NoError(t, err)
	req.HostID = "host-1"
	req.SessionID = "session-1"

	stream, err := session.OpenStreamSync(ctx)
	require.NoError(t, err)
	defer wt.CloseStream(stream)

	resp, err := protocol.RoundTrip(ctx, stream, req)
	require.NoError(t, err)
	return resp
}

// newReconnectingClient returns a runtime client holding a valid host-1 certificate, so it reconnects over mTLS without bootstrapping
func newReconnectingClient(t *testing.T, addr string, cfg runtimeClientConfig) *runtimeClient {
	t.Helper()

	cas, err := ca.CABundle([][]byte{testRuntimePSK})
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := cas[0].IssueWorkloadCert(ca.HostURI("host-1"), pub, time.Hour)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	holder := certholder.New(&tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}, ca.NewCertPool(cas))
	cfg.addresses = []string{addr}
	cfg.peerAddress = "127.0.0.1:7010"
	cfg.actorTypes = []protocol.ActorHostType{{ActorType: "T", IdleTimeoutMs: 60000}}
	cfg.tlsConfig = hosttls.RuntimeClientTLSConfig(holder)
	cfg.holder = holder
	cfg.minBackoff = 20 * time.Millisecond
	cfg.maxBackoff = 50 * time.Millisecond
	cfg.requestTimeout = 2 * time.Second
	cfg.log = slog.New(slog.DiscardHandler)

	rc := newRuntimeClient(cfg)
	rc.hostID = "host-1"
	return rc
}

func waitSession(t *testing.T, rt *scriptedRuntime) *webtransport.Session {
	t.Helper()
	select {
	case s := <-rt.sessionCh:
		return s
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register with the fake runtime")
		return nil
	}
}

func waitRun(t *testing.T, runErr <-chan error) error {
	t.Helper()
	select {
	case err := <-runErr:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

type gracefulRemoteJob struct {
	run func(context.Context) error
}

func (j gracefulRemoteJob) Job(ctx context.Context, _ string, _ actor.Envelope) error {
	return j.run(ctx)
}

func TestRemoteDrainPreservesRunningJobGrace(t *testing.T) {
	for _, timeout := range []int64{0, 2000} {
		t.Run(time.Duration(timeout*int64(time.Millisecond)).String(), func(t *testing.T) {
			rt := startScriptedRuntime(t)
			started := make(chan struct{})
			halting := make(chan error, 1)
			release := make(chan struct{})
			defer close(release)

			h := &Host{clock: &clock.RealClock{}, log: slog.New(slog.DiscardHandler)}
			h.core = actorcore.NewManager(actorcore.Options{
				RemoveActor:         func(context.Context, ref.ActorRef) error { return nil },
				ShutdownGracePeriod: time.Hour,
			})

			err := h.core.RegisterActor("T", func(string, *actor.Service) actor.Actor {
				return gracefulRemoteJob{run: func(ctx context.Context) error {
					close(started)
					select {
					case <-actor.HaltingFromContext(ctx):
						halting <- ctx.Err()
					case <-ctx.Done():
						halting <- ctx.Err()
						return ctx.Err()
					}
					select {
					case <-release:
						return nil
					case <-t.Context().Done():
						return context.Canceled
					}
				}}
			}, actorcore.RegisterActorOptions{})
			require.NoError(t, err)

			h.core.Start()
			defer h.core.Close()

			rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{
				handlers: runtimeHandlers{executeAlarm: h.executeAlarm},
				onDrain:  h.core.DrainAll,
			})

			runErr := make(chan error, 1)
			go func() {
				runErr <- rc.Run(t.Context())
			}()

			session := waitSession(t, rt)
			jobReq, err := protocol.NewRequest(protocol.KindExecuteAlarm, protocol.ExecuteAlarmRequest{
				ActorType: "T", ActorID: "one", Name: "job", Kind: string(components.AlarmKindJob), JobMethod: "wait",
			})
			require.NoError(t, err)

			jobReq.HostID = "host-1"
			jobReq.SessionID = "session-1"
			jobCtx, cancelJob := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelJob()

			type jobResult struct {
				response *protocol.Envelope
				err      error
			}
			jobResp := make(chan jobResult, 1)

			go func() {
				stream, err := session.OpenStreamSync(jobCtx)
				if err != nil {
					jobResp <- jobResult{err: err}
					return
				}
				defer wt.CloseStream(stream)
				resp, err := protocol.RoundTrip(jobCtx, stream, jobReq)
				jobResp <- jobResult{response: resp, err: err}
			}()

			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("remote job did not start")
			}

			resp := rt.send(t, session, protocol.KindHostDrain, protocol.HostDrainRequest{TimeoutMs: timeout})
			require.Equal(t, protocol.KindHostDrainResponse, resp.Kind)
			select {
			case err = <-halting:
				require.NoError(t, err, "stopping admission must preserve the job context during grace")
			case <-time.After(5 * time.Second):
				t.Fatal("job did not receive the halt signal")
			}

			release <- struct{}{}

			select {
			case result := <-jobResp:
				require.NoError(t, result.err)
				require.Equal(t, protocol.KindExecuteAlarmResponse, result.response.Kind)
			case <-time.After(5 * time.Second):
				t.Fatal("job response was lost during teardown")
			}

			err = waitRun(t, runErr)
			require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		})
	}
}

func TestRemoteDrainCancelsIdleStreamReads(t *testing.T) {
	rt := startScriptedRuntime(t)
	rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{})
	runErr := make(chan error, 1)

	go func() {
		runErr <- rc.Run(t.Context())
	}()

	session := waitSession(t, rt)

	// Leave a request frame incomplete so the host has an admitted stream waiting for bytes
	stream, err := session.OpenStreamSync(t.Context())
	require.NoError(t, err)
	defer wt.CloseStream(stream)

	_, err = stream.Write([]byte{0})
	require.NoError(t, err)

	resp := rt.send(t, session, protocol.KindHostDrain, protocol.HostDrainRequest{TimeoutMs: 1000})
	require.Equal(t, protocol.KindHostDrainResponse, resp.Kind)

	select {
	case err = <-runErr:
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)
	case <-time.After(5 * time.Second):
		t.Fatal("an idle inbound stream held the drain open")
	}
}

func TestRuntimeClientAdministrativeDrain(t *testing.T) {
	t.Run("drain acknowledges, tears down, and ends Run", func(t *testing.T) {
		rt := startScriptedRuntime(t)

		var (
			drainStarted atomic.Bool
			drainTimeout atomic.Int64
		)
		onDrainCalled := make(chan struct{})
		rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{
			onDrainStart: func() { drainStarted.Store(true) },
			onDrain: func(timeout time.Duration) {
				drainTimeout.Store(int64(timeout))
				close(onDrainCalled)
			},
		})

		runErr := make(chan error, 1)
		go func() {
			runErr <- rc.Run(t.Context())
		}()
		session := waitSession(t, rt)

		// The drain is acknowledged before the teardown starts
		resp := rt.send(t, session, protocol.KindHostDrain, protocol.HostDrainRequest{TimeoutMs: 1500, Reason: "test"})
		perr, isErr := resp.AsError()
		require.False(t, isErr, "unexpected error %v", perr)
		require.Equal(t, protocol.KindHostDrainResponse, resp.Kind)
		var ack protocol.HostDrainResponse
		err := resp.DecodePayload(&ack)
		require.NoError(t, err)
		assert.False(t, ack.AlreadyDraining)

		// The graceful sequence runs, bounded by the requested timeout, and Run reports the drain
		err = waitRun(t, runErr)
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		select {
		case <-onDrainCalled:
		default:
			t.Fatal("onDrain was not called")
		}
		assert.True(t, drainStarted.Load())
		assert.Equal(t, 1500*time.Millisecond, time.Duration(drainTimeout.Load()))
		assert.Equal(t, int32(1), rt.unregisters.Load())

		// The host never registers again, and a later Run refuses to start
		err = rc.Run(t.Context())
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, int32(1), rt.registrations.Load())
	})

	t.Run("session dropping mid-drain does not reconnect", func(t *testing.T) {
		rt := startScriptedRuntime(t)

		onDrainEntered := make(chan struct{})
		releaseDrain := make(chan struct{})
		rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{
			onDrain: func(time.Duration) {
				close(onDrainEntered)
				<-releaseDrain
			},
		})

		runErr := make(chan error, 1)
		go func() {
			runErr <- rc.Run(t.Context())
		}()
		session := waitSession(t, rt)

		resp := rt.send(t, session, protocol.KindHostDrain, protocol.HostDrainRequest{Reason: "test"})
		require.Equal(t, protocol.KindHostDrainResponse, resp.Kind)

		// Drop the session while local actors are still draining
		select {
		case <-onDrainEntered:
		case <-time.After(10 * time.Second):
			t.Fatal("onDrain was not called")
		}
		_ = session.CloseWithError(0, "runtime went away")

		// Give a reconnect loop time to misbehave before the drain completes
		time.Sleep(200 * time.Millisecond)
		close(releaseDrain)

		err := waitRun(t, runErr)
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, int32(1), rt.registrations.Load())
	})

	t.Run("health checks keep running while actors drain", func(t *testing.T) {
		rt := startScriptedRuntime(t)
		rt.healthDeadlineMs.Store(1500)

		// Halting the actors takes longer than the health check deadline
		var before, after int32
		rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{
			onDrain: func(time.Duration) {
				before = rt.healthChecks.Load()
				time.Sleep(2 * time.Second)
				after = rt.healthChecks.Load()
			},
		})

		runErr := make(chan error, 1)
		go func() {
			runErr <- rc.Run(t.Context())
		}()
		session := waitSession(t, rt)
		resp := rt.send(t, session, protocol.KindHostDrain, protocol.HostDrainRequest{Reason: "test"})
		require.Equal(t, protocol.KindHostDrainResponse, resp.Kind)

		// The runtime kept receiving health checks, so the registration did not expire while the actors halted
		err := waitRun(t, runErr)
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		assert.Greater(t, after, before, "no health check was sent while the actors drained")
	})

	t.Run("context cancellation is not a drain", func(t *testing.T) {
		rt := startScriptedRuntime(t)

		drainTimeout := make(chan time.Duration, 1)
		rc := newReconnectingClient(t, rt.addr, runtimeClientConfig{
			onDrain: func(timeout time.Duration) { drainTimeout <- timeout },
		})

		ctx, cancel := context.WithCancel(t.Context())
		runErr := make(chan error, 1)
		go func() {
			runErr <- rc.Run(ctx)
		}()
		waitSession(t, rt)
		<-rc.Ready()

		cancel()
		err := waitRun(t, runErr)
		require.NoError(t, err)
		select {
		case timeout := <-drainTimeout:
			assert.Equal(t, time.Duration(0), timeout, "a shutdown is not bounded")
		default:
			t.Fatal("onDrain was not called")
		}
		assert.Equal(t, int32(1), rt.unregisters.Load())
	})
}

func TestDispatchInboundDrain(t *testing.T) {
	identity := sessionIdentity{hostID: "host-1", sessionID: "session-1"}
	drain := func(t *testing.T, rc *runtimeClient) protocol.HostDrainResponse {
		t.Helper()
		req, err := protocol.NewRequest(protocol.KindHostDrain, protocol.HostDrainRequest{TimeoutMs: 250})
		require.NoError(t, err)
		resp := rc.dispatchInbound(t.Context(), req, identity)
		perr, isErr := resp.AsError()
		require.False(t, isErr, "unexpected error %v", perr)
		var out protocol.HostDrainResponse
		err = resp.DecodePayload(&out)
		require.NoError(t, err)
		return out
	}

	t.Run("second drain reports already draining", func(t *testing.T) {
		rc := newRuntimeClient(runtimeClientConfig{addresses: []string{"127.0.0.1:1"}})
		require.True(t, rc.drain.Start(), "a drain is only accepted while the client runs")
		assert.False(t, drain(t, rc).AlreadyDraining)
		assert.True(t, drain(t, rc).AlreadyDraining)

		// The accepted drain keeps its timeout for the teardown
		teardown, timeout := rc.beginTeardown(false)
		assert.True(t, teardown)
		assert.Equal(t, 250*time.Millisecond, timeout)
	})

	t.Run("drain during shutdown reports already draining", func(t *testing.T) {
		rc := newRuntimeClient(runtimeClientConfig{addresses: []string{"127.0.0.1:1"}})
		require.True(t, rc.drain.Start(), "a drain is only accepted while the client runs")
		teardown, timeout := rc.beginTeardown(true)
		assert.True(t, teardown)
		assert.Zero(t, timeout)
		assert.True(t, drain(t, rc).AlreadyDraining)
		assert.False(t, rc.drain.Accepted())
	})

	t.Run("a client that is not running refuses a drain", func(t *testing.T) {
		rc := newRuntimeClient(runtimeClientConfig{addresses: []string{"127.0.0.1:1"}})
		req, err := protocol.NewRequest(protocol.KindHostDrain, protocol.HostDrainRequest{})
		require.NoError(t, err)
		perr, isErr := rc.dispatchInbound(t.Context(), req, identity).AsError()
		require.True(t, isErr)
		assert.Equal(t, protocol.ErrCodeHostUnavailable, perr.Code)
		assert.False(t, rc.drain.Accepted())
	})

	t.Run("a new run after a shutdown accepts a drain again", func(t *testing.T) {
		rc := newRuntimeClient(runtimeClientConfig{addresses: []string{"127.0.0.1:1"}})
		require.True(t, rc.drain.Start())
		teardown, _ := rc.beginTeardown(true)
		require.True(t, teardown)
		rc.drain.Stopped()

		// The shutdown belonged to the previous run, so the next one can be drained
		require.True(t, rc.drain.Start())
		assert.False(t, drain(t, rc).AlreadyDraining)
	})

	t.Run("no drain and no shutdown means no teardown", func(t *testing.T) {
		rc := newRuntimeClient(runtimeClientConfig{addresses: []string{"127.0.0.1:1"}})
		require.True(t, rc.drain.Start(), "a drain is only accepted while the client runs")
		teardown, _ := rc.beginTeardown(false)
		assert.False(t, teardown)
	})
}

func TestDispatchInboundSnapshot(t *testing.T) {
	var got protocol.HostSnapshotRequest
	rc := newRuntimeClient(runtimeClientConfig{
		addresses: []string{"127.0.0.1:1"},
		handlers: runtimeHandlers{
			snapshot: func(_ context.Context, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, *protocol.Error) {
				got = req
				return protocol.HostSnapshotResponse{HostID: "host-1", ActiveCount: 3}, nil
			},
		},
	})

	req, err := protocol.NewRequest(protocol.KindHostSnapshot, protocol.HostSnapshotRequest{ActorType: "T", Limit: 10, SkipActivations: true})
	require.NoError(t, err)
	resp := rc.dispatchInbound(t.Context(), req, sessionIdentity{hostID: "host-1", sessionID: "s"})
	perr, isErr := resp.AsError()
	require.False(t, isErr, "unexpected error %v", perr)
	require.Equal(t, protocol.KindHostSnapshotResponse, resp.Kind)

	var out protocol.HostSnapshotResponse
	err = resp.DecodePayload(&out)
	require.NoError(t, err)
	assert.Equal(t, "host-1", out.HostID)
	assert.Equal(t, 3, out.ActiveCount)
	assert.Equal(t, protocol.HostSnapshotRequest{ActorType: "T", Limit: 10, SkipActivations: true}, got)
}
