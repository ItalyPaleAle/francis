package local

import (
	"context"
	"io"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/protocol"
)

// Invoke performs the synchronous invocation of an actor running anywhere in the cluster.
func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, optsFn ...actor.InvokeOption) (actor.Envelope, error) {
	return h.core.InvokeActor(ctx, h.resolver, h.peerClient, actorType, actorID, method, data, false, optsFn)
}

// InvokeStream performs a streamed invocation of an actor running anywhere in the cluster.
func (h *Host) InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, optsFn ...actor.InvokeOption) (string, io.ReadCloser, error) {
	return h.core.InvokeActorStream(ctx, h.resolver, h.peerClient, actorType, actorID, method, reqContentType, body, false, optsFn)
}

// Peek performs the synchronous, read-only invocation of an actor running anywhere in the cluster.
func (h *Host) Peek(ctx context.Context, actorType string, actorID string, method string, data any, optsFn ...actor.InvokeOption) (actor.Envelope, error) {
	return h.core.InvokeActor(ctx, h.resolver, h.peerClient, actorType, actorID, method, data, true, optsFn)
}

// PeekStream performs a streamed, read-only invocation of an actor running anywhere in the cluster.
func (h *Host) PeekStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, optsFn ...actor.InvokeOption) (string, io.ReadCloser, error) {
	return h.core.InvokeActorStream(ctx, h.resolver, h.peerClient, actorType, actorID, method, reqContentType, body, true, optsFn)
}

// peerInvokeObject executes an object invocation for an actor owned by this host, on behalf of a peer caller
func (h *Host) peerInvokeObject(ctx context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	return h.core.PeerInvokeObject(ctx, h.resolver, req)
}

// peerInvokeStream executes a streamed invocation for an actor owned by this host, on behalf of a peer caller
func (h *Host) peerInvokeStream(ctx context.Context, req protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error {
	return h.core.PeerInvokeStream(ctx, h.resolver, req, body, w)
}
