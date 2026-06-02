package remote

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/protocol"
)

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
		ps := newPeerServer(peerServerConfig{hostID: func() string { return "host-b" }, handler: echoHandler})
		resp := ps.handleObject(t.Context(), newReq(t, protocol.InvokeActorRequest{
			ActorType: "T",
			ActorID:   "a1",
			Method:    "echo",
			Mode:      protocol.InvocationModeObject,
			Data:      []byte("arg"),
		}), protocol.InvokeActorRequest{Method: "echo", Data: []byte("arg")})
		require.Equal(t, protocol.KindInvokeActorResponse, resp.Kind)
		var out protocol.InvokeActorResponse
		require.NoError(t, resp.DecodePayload(&out))
		assert.Equal(t, []byte("arg"), out.Data)
	})

	t.Run("handler error is relayed", func(t *testing.T) {
		ps := newPeerServer(peerServerConfig{hostID: func() string { return "host-b" }, handler: echoHandler})
		resp := ps.handleObject(t.Context(), protocol.NewEnvelope(protocol.KindInvokeActor, nil), protocol.InvokeActorRequest{Method: "boom"})
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeFailed, perr.Code)
	})

	t.Run("object invocation is unsupported when no handler is registered", func(t *testing.T) {
		ps := newPeerServer(peerServerConfig{hostID: func() string { return "host-b" }})
		resp := ps.handleObject(t.Context(), protocol.NewEnvelope(protocol.KindInvokeActor, nil), protocol.InvokeActorRequest{})
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeModeUnsupported, perr.Code)
	})
}

func TestPeerInvocationIntegration(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	ps := newPeerServer(peerServerConfig{
		bind:      addr,
		tlsConfig: srvTLS,
		hostID:    func() string { return "host-b" },
		handler:   echoHandler,
		log:       slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{InsecureSkipTLSValidation: true}.GetTLSConfig()
	require.NoError(t, err)
	pc := newPeerClient(cliTLS, 5*time.Second, slog.New(slog.DiscardHandler))
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

// echoStreamHandler reads the entire request body and returns it as the response body
func echoStreamHandler(_ context.Context, _ protocol.InvokeActorRequest, body io.Reader) (string, io.Reader, *protocol.Error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", nil, protocol.NewErrorf(protocol.ErrCodeInvokeFailed, "failed to read body: %v", err)
	}
	return "application/test", bytes.NewReader(data), nil
}

func TestPeerStreamInvocationIntegration(t *testing.T) {
	addr := freeUDPAddr(t)

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)

	ps := newPeerServer(peerServerConfig{
		bind:          addr,
		tlsConfig:     srvTLS,
		hostID:        func() string { return "host-b" },
		streamHandler: echoStreamHandler,
		log:           slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{InsecureSkipTLSValidation: true}.GetTLSConfig()
	require.NoError(t, err)
	pc := newPeerClient(cliTLS, 5*time.Second, slog.New(slog.DiscardHandler))
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
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		contentType, respBody, perr = pc.InvokeStream(reqCtx, addr, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "stream",
			ContentType:  "application/test",
		}, bytes.NewReader(payload))
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
	ps := newPeerServer(peerServerConfig{
		bind:      addr,
		tlsConfig: srvTLS,
		hostID:    func() string { return "host-b" },
		handler:   echoHandler,
		log:       slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = ps.Run(ctx)
	}()

	_, cliTLS, err := hosttls.HostTLSOptions{InsecureSkipTLSValidation: true}.GetTLSConfig()
	require.NoError(t, err)
	pc := newPeerClient(cliTLS, 5*time.Second, slog.New(slog.DiscardHandler))
	defer pc.Close()

	var perr *protocol.Error
	deadline := time.Now().Add(10 * time.Second)
	for {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, _, perr = pc.InvokeStream(reqCtx, addr, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "stream",
		}, bytes.NewReader([]byte("body")))
		reqCancel()
		// Retry only transport failures while the server is starting; a structured reply ends the loop
		if perr == nil || perr.Code != protocol.ErrCodeRetryLater || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotNil(t, perr)
	assert.Equal(t, protocol.ErrCodeInvokeModeUnsupported, perr.Code)
}
