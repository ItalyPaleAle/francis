package peer

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/hosttls"
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

// echoHandler returns the request argument as the result, or a structured error for the "boom" method
func echoHandler(_ context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	if req.Method == "boom" {
		return protocol.InvokeActorResponse{}, protocol.NewError(protocol.ErrCodeInvokeFailed, "actor failed")
	}
	return protocol.InvokeActorResponse{Data: req.Data}, nil
}

func TestPeerServerHandleObject(t *testing.T) {
	newReq := func(t *testing.T, payload protocol.InvokeActorRequest) *protocol.Envelope {
		t.Helper()
		env, err := protocol.NewRequest(protocol.KindInvokeActor, payload)
		require.NoError(t, err)
		return env
	}

	t.Run("object invocation returns the result", func(t *testing.T) {
		ps := NewServer(ServerConfig{
			HostID:  func() string { return "host-b" },
			Handler: echoHandler,
		})
		env := newReq(t, protocol.InvokeActorRequest{
			ActorType: "T",
			ActorID:   "a1",
			Method:    "echo",
			Mode:      protocol.InvocationModeObject,
			Data:      []byte("arg"),
		})
		resp := ps.handleObject(t.Context(), env, protocol.InvokeActorRequest{
			Method: "echo",
			Data:   []byte("arg"),
		})
		require.Equal(t, protocol.KindInvokeActorResponse, resp.Kind)

		var out protocol.InvokeActorResponse
		err := resp.DecodePayload(&out)
		require.NoError(t, err)
		assert.Equal(t, []byte("arg"), out.Data)
	})

	t.Run("handler error is relayed", func(t *testing.T) {
		ps := NewServer(ServerConfig{
			HostID:  func() string { return "host-b" },
			Handler: echoHandler,
		})
		env := protocol.NewEnvelope(protocol.KindInvokeActor, nil)
		resp := ps.handleObject(t.Context(), env, protocol.InvokeActorRequest{Method: "boom"})
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeFailed, perr.Code)
	})

	t.Run("object invocation is unsupported when no handler is registered", func(t *testing.T) {
		ps := NewServer(ServerConfig{
			HostID: func() string { return "host-b" },
		})
		env := protocol.NewEnvelope(protocol.KindInvokeActor, nil)
		resp := ps.handleObject(t.Context(), env, protocol.InvokeActorRequest{})
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeModeUnsupported, perr.Code)
	})
}

func TestPeerInvocationIntegration(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	ps := NewServer(ServerConfig{
		Bind:      addr,
		TLSConfig: srvTLS,
		HostID:    func() string { return "host-b" },
		Handler:   echoHandler,
		Log:       slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	arg, err := protocol.Marshal("hello peer")
	require.NoError(t, err)

	// Retry the first invocation until the server is accepting connections
	var (
		out  protocol.InvokeActorResponse
		perr *protocol.Error
	)
	deadline := time.Now().Add(10 * time.Second)
	for {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		out, perr = pc.InvokeObject(reqCtx, addr, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "echo",
			Data:         arg,
		})
		reqCancel()
		if perr == nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Nil(t, perr, "invocation should succeed once the peer server is up")

	var got string
	require.NoError(t, protocol.Unmarshal(out.Data, &got))
	assert.Equal(t, "hello peer", got)

	// An invocation aimed at a stale placement is rejected with a retryable host mismatch
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	_, perr = pc.InvokeObject(reqCtx, addr, protocol.InvokeActorRequest{
		TargetHostID: "stale-host",
		ActorType:    "T",
		ActorID:      "a1",
		Method:       "echo",
		Data:         arg,
	})
	reqCancel()
	require.NotNil(t, perr)
	assert.Equal(t, protocol.ErrCodeHostMismatch, perr.Code)
	assert.True(t, perr.Retryable())
}

func TestPeerInvocationRejectedWhenDraining(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	// The server flips into draining mid-test so we can prove both that it serves normally and that it then rejects new invocations
	var draining atomic.Bool
	ps := NewServer(ServerConfig{
		Bind:          addr,
		TLSConfig:     srvTLS,
		HostID:        func() string { return "host-b" },
		Handler:       echoHandler,
		StreamHandler: echoStreamHandler,
		Draining:      draining.Load,
		Log:           slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	req := protocol.InvokeActorRequest{
		TargetHostID: "host-b",
		ActorType:    "T",
		ActorID:      "a1",
		Method:       "echo",
		Data:         []byte("arg"),
	}

	// Retry the first invocation until the server is accepting connections, while it is not yet draining
	var perr *protocol.Error
	deadline := time.Now().Add(10 * time.Second)
	for {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, perr = pc.InvokeObject(reqCtx, addr, req)
		reqCancel()
		if perr == nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Nil(t, perr, "invocation should succeed before the host starts draining")

	// Once draining, a new object invocation is rejected with a retryable draining error before reaching the handler
	draining.Store(true)
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	_, perr = pc.InvokeObject(reqCtx, addr, req)
	reqCancel()
	require.NotNil(t, perr, "a draining host must reject the object invocation")
	assert.Equal(t, protocol.ErrCodeHostDraining, perr.Code)
	assert.True(t, perr.Retryable())

	// A stream invocation is rejected the same way
	streamReq := req
	streamReq.Mode = protocol.InvocationModeStream
	reqCtx, reqCancel = context.WithTimeout(ctx, 2*time.Second)
	_, _, perr = pc.InvokeStream(reqCtx, addr, streamReq, bytes.NewReader([]byte("body")))
	reqCancel()
	require.NotNil(t, perr, "a draining host must reject the stream invocation")
	assert.Equal(t, protocol.ErrCodeHostDraining, perr.Code)
	assert.True(t, perr.Retryable())
}

func TestPeerInFlightInvocationCompletesDuringDrain(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	// The "block" method holds the handler open until released, so a test can flip draining while the invocation is in flight
	var draining atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
		if req.Method == "block" {
			close(started)
			<-release
		}
		return protocol.InvokeActorResponse{Data: req.Data}, nil
	}

	ps := NewServer(ServerConfig{
		Bind:      addr,
		TLSConfig: srvTLS,
		HostID:    func() string { return "host-b" },
		Handler:   handler,
		Draining:  draining.Load,
		Log:       slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	baseReq := protocol.InvokeActorRequest{
		TargetHostID: "host-b",
		ActorType:    "T",
		ActorID:      "a1",
		Mode:         protocol.InvocationModeObject,
	}

	// Warm up with a quick echo, retrying until the server is accepting connections, while it is not yet draining
	var perr *protocol.Error
	deadline := time.Now().Add(10 * time.Second)
	for {
		warmReq := baseReq
		warmReq.Method = "echo"
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, perr = pc.InvokeObject(reqCtx, addr, warmReq)
		reqCancel()
		if perr == nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Nil(t, perr, "warmup invocation should succeed before draining")

	// Start an invocation that blocks in the handler
	// It passes the draining gate while draining is still false
	type result struct {
		resp protocol.InvokeActorResponse
		perr *protocol.Error
	}
	inflight := make(chan result, 1)
	go func() {
		req := baseReq
		req.Method = "block"
		req.Data = []byte("payload")
		resp, perr := pc.InvokeObject(ctx, addr, req)
		inflight <- result{resp: resp, perr: perr}
	}()

	// Wait until the in-flight handler is running, then flip into draining
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight invocation did not start")
	}
	draining.Store(true)

	// A new invocation arriving during the drain is rejected
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	newReq := baseReq
	newReq.Method = "echo"
	_, perr = pc.InvokeObject(reqCtx, addr, newReq)
	reqCancel()
	require.NotNil(t, perr, "a new invocation must be rejected while draining")
	assert.Equal(t, protocol.ErrCodeHostDraining, perr.Code)

	// The in-flight invocation must still complete normally once released, despite draining
	close(release)
	select {
	case res := <-inflight:
		require.Nil(t, res.perr, "an in-flight invocation must finish normally during the drain window")
		assert.Equal(t, []byte("payload"), res.resp.Data)
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight invocation did not complete")
	}
}

// echoStreamHandler reads the entire request body and writes it back as the response body
func echoStreamHandler(_ context.Context, _ protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error {
	data, err := io.ReadAll(body)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInvokeFailed, "failed to read body: %v", err)
	}

	w.SetContentType("application/test")
	_, err = w.Write(data)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInvokeFailed, "failed to write response: %v", err)
	}

	return nil
}

func TestPeerStreamInvocationIntegration(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	ps := NewServer(ServerConfig{
		Bind:          addr,
		TLSConfig:     srvTLS,
		HostID:        func() string { return "host-b" },
		StreamHandler: echoStreamHandler,
		Log:           slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	payload := []byte("a streamed request body that travels as raw bytes")

	// Retry the first invocation until the server is accepting connections
	var (
		contentType string
		respBody    io.ReadCloser
		perr        *protocol.Error
	)
	deadline := time.Now().Add(10 * time.Second)
	for {
		req := protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "stream",
			ContentType:  "application/test",
		}
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		contentType, respBody, perr = pc.InvokeStream(reqCtx, addr, req, bytes.NewReader(payload))
		reqCancel()
		if perr == nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Nil(t, perr, "stream invocation should succeed once the peer server is up")
	require.NotNil(t, respBody)
	defer respBody.Close()

	assert.Equal(t, "application/test", contentType)
	got, err := io.ReadAll(respBody)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestPeerStreamInvocationUnsupported(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	// A server with only an object handler does not support stream invocation
	ps := NewServer(ServerConfig{
		Bind:      addr,
		TLSConfig: srvTLS,
		HostID:    func() string { return "host-b" },
		Handler:   echoHandler,
		Log:       slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	var perr *protocol.Error
	deadline := time.Now().Add(10 * time.Second)
	for {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		req := protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "stream",
		}
		_, _, perr = pc.InvokeStream(reqCtx, addr, req, bytes.NewReader([]byte("body")))
		reqCancel()
		// Retry only transport failures while the server is starting
		// A structured reply ends the loop
		if perr == nil || perr.Code != protocol.ErrCodeRetryLater || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotNil(t, perr)
	assert.Equal(t, protocol.ErrCodeInvokeModeUnsupported, perr.Code)
}

// partialThenFailStreamHandler writes part of the response body and then fails, simulating an actor whose stream invocation fails after it has started writing
// The sleep gives the written bytes time to reach the caller so the test deterministically exercises the post-flush failure path
func partialThenFailStreamHandler(_ context.Context, _ protocol.InvokeActorRequest, _ io.Reader, w actor.StreamResponseWriter) *protocol.Error {
	w.SetContentType("application/test")
	_, err := w.Write([]byte("partial-but-incomplete-response-body"))
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInvokeFailed, "failed to write response: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	return protocol.NewError(protocol.ErrCodeInvokeFailed, "boom after the body started")
}

// TestPeerStreamInvocationMidStreamFailure verifies that a mid-stream actor failure is surfaced to the caller as a read error rather than a clean, truncated success
func TestPeerStreamInvocationMidStreamFailure(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	ps := NewServer(ServerConfig{
		Bind:          addr,
		TLSConfig:     srvTLS,
		HostID:        func() string { return "host-b" },
		StreamHandler: partialThenFailStreamHandler,
		Log:           slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	// Retry until the server accepts the invocation and hands back a response body
	var (
		respBody io.ReadCloser
		perr     *protocol.Error
	)
	deadline := time.Now().Add(10 * time.Second)
	for {
		req := protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "stream",
		}
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, respBody, perr = pc.InvokeStream(reqCtx, addr, req, bytes.NewReader([]byte("request")))
		reqCancel()
		if perr == nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Nil(t, perr, "the metadata frame is sent before the failure, so the invocation call itself succeeds")
	require.NotNil(t, respBody)
	defer respBody.Close()

	// Reading the body must fail rather than return a clean EOF, so the caller never mistakes the truncated body for a complete response
	_, readErr := io.ReadAll(respBody)
	require.Error(t, readErr, "a mid-stream failure must surface as a read error, not a clean EOF")
	assert.ErrorIs(t, readErr, ErrStreamReset)
}
