package remote

import (
	"context"
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

func TestPeerServerHandleInvoke(t *testing.T) {
	ps := newPeerServer(peerServerConfig{
		hostID:  func() string { return "host-b" },
		handler: echoHandler,
	})

	newReq := func(t *testing.T, payload protocol.InvokeActorRequest) *protocol.Envelope {
		t.Helper()
		env, err := protocol.NewRequest(protocol.KindInvokeActor, payload)
		require.NoError(t, err)
		return env
	}

	t.Run("object invocation returns the result", func(t *testing.T) {
		resp := ps.handleInvoke(t.Context(), newReq(t, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Method:       "echo",
			Mode:         protocol.InvocationModeObject,
			Data:         []byte("arg"),
		}))
		require.Equal(t, protocol.KindInvokeActorResponse, resp.Kind)
		var out protocol.InvokeActorResponse
		require.NoError(t, resp.DecodePayload(&out))
		assert.Equal(t, []byte("arg"), out.Data)
	})

	t.Run("invocation for a different host is a mismatch", func(t *testing.T) {
		resp := ps.handleInvoke(t.Context(), newReq(t, protocol.InvokeActorRequest{
			TargetHostID: "host-other",
			ActorType:    "T",
			ActorID:      "a1",
			Mode:         protocol.InvocationModeObject,
		}))
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeHostMismatch, perr.Code)
		assert.True(t, perr.Retryable())
	})

	t.Run("stream invocation is unsupported", func(t *testing.T) {
		resp := ps.handleInvoke(t.Context(), newReq(t, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			ActorType:    "T",
			ActorID:      "a1",
			Mode:         protocol.InvocationModeStream,
		}))
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeModeUnsupported, perr.Code)
	})

	t.Run("handler error is relayed", func(t *testing.T) {
		resp := ps.handleInvoke(t.Context(), newReq(t, protocol.InvokeActorRequest{
			TargetHostID: "host-b",
			Method:       "boom",
			Mode:         protocol.InvocationModeObject,
		}))
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeInvokeFailed, perr.Code)
	})

	t.Run("non-invocation kind is rejected", func(t *testing.T) {
		resp := ps.handleInvoke(t.Context(), protocol.NewEnvelope(protocol.KindHealthCheck, nil))
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeBadRequest, perr.Code)
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
	go func() { _ = ps.Run(ctx) }()

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
