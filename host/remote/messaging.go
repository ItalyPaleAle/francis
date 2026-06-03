package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/types"
	"github.com/italypaleale/francis/protocol"
)

// Invoke performs the synchronous invocation of an actor running anywhere in the cluster.
func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, optsFn ...actor.InvokeOption) (actor.Envelope, error) {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	// Resolve the placement and invoke, using the cache on the first attempt
	ap, err := h.lookupActor(ctx, aRef, false, opts.ActiveOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	env, retry, err := h.doInvoke(ctx, aRef, ap, method, data, opts.ActiveOnly)
	if !retry {
		return env, err
	}

	// The placement was stale: drop it, re-resolve with a fresh lookup, and try once more
	h.invalidatePlacement(aRef)
	ap, err = h.lookupActor(ctx, aRef, true, opts.ActiveOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	env, _, err = h.doInvoke(ctx, aRef, ap, method, data, opts.ActiveOnly)
	return env, err
}

// doInvoke dispatches a single invocation attempt to the local actor or a peer, depending on the placement
// The bool return is true when the attempt failed in a way that warrants re-resolving the placement and retrying
func (h *Host) doInvoke(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
	if h.isLocal(ap) {
		return h.doInvokeLocal(ctx, aRef, method, data, activeOnly)
	}
	return h.doInvokeRemote(ctx, aRef, ap, method, data, activeOnly)
}

// doInvokeLocal invokes an actor owned by this host
// The bool return is true when an active-only invocation found the actor inactive, so the caller can re-resolve in case it is active elsewhere
func (h *Host) doInvokeLocal(ctx context.Context, aRef ref.ActorRef, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
	invoke := func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		// The actor must implement the Invoke method to be called this way
		obj, ok := act.Instance.(actor.ActorInvoke)
		if !ok {
			return nil, errActorMethodUnsupported
		}

		// Invoke the actor
		// We wrap the data in an envelope as the receiver expects
		res, err := obj.Invoke(invokeCtx, method, actorcore.NewObjectEnvelope(data))
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return res, nil
	}

	// An active-only invocation must not activate the actor
	// If it is not active here, re-resolve in case it moved
	var (
		res any
		err error
	)
	if activeOnly {
		res, err = h.core.LockAndInvokeActive(ctx, aRef, invoke)
		if errors.Is(err, actor.ErrActorNotActive) {
			return nil, true, err
		}
	} else {
		res, err = h.core.LockAndInvoke(ctx, aRef, invoke)
	}
	if err != nil {
		return nil, false, err
	}

	// If there's a response, wrap it in an envelope
	if res != nil {
		return actorcore.NewObjectEnvelope(res), false, nil
	}

	return nil, false, nil
}

// doInvokeRemote invokes an actor owned by a peer host over the peer transport
// The bool return is true when the placement looks stale and the caller should re-resolve and retry
func (h *Host) doInvokeRemote(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
	// Encode the argument as MessagePack for the request body
	var argData []byte
	if data != nil {
		b, err := msgpack.Marshal(data)
		if err != nil {
			return nil, false, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}
		argData = b
	}

	// Invoke the actor on the peer that owns it
	resp, perr := h.peerClient.InvokeObject(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    aRef.ActorType,
		ActorID:      aRef.ActorID,
		Method:       method,
		Data:         argData,
		ActiveOnly:   activeOnly,
	})
	if perr != nil {
		// Re-resolve and retry on a stale placement, a halted actor, a transport failure, or an active-only miss that may be active elsewhere
		retry := perr.Retryable() || perr.Code == protocol.ErrCodeActorNotActive
		return nil, retry, protocolErrorToActor(perr)
	}

	// Decode the result into an envelope
	if len(resp.Data) == 0 {
		return nil, false, nil
	}
	var out any
	err := msgpack.Unmarshal(resp.Data, &out)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode response body using msgpack: %w", err)
	}

	return actorcore.NewObjectEnvelope(out), false, nil
}

// InvokeStream performs a streamed invocation of an actor running anywhere in the cluster.
// Stale-placement and not-active errors are returned to the caller rather than retried, because the request body is a one-shot reader that cannot be replayed.
func (h *Host) InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, optsFn ...actor.InvokeOption) (string, io.ReadCloser, error) {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	// Resolve the placement and invoke
	ap, err := h.lookupActor(ctx, aRef, false, opts.ActiveOnly)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	return h.doInvokeStream(ctx, aRef, ap, method, reqContentType, body, opts.ActiveOnly)
}

// doInvokeStream dispatches a streamed invocation to the local actor or a peer, depending on the placement
func (h *Host) doInvokeStream(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	if h.isLocal(ap) {
		return h.doInvokeStreamLocal(ctx, aRef, method, reqContentType, body, activeOnly)
	}

	ct, resp, err := h.doInvokeStreamRemote(ctx, aRef, ap, method, reqContentType, body, activeOnly)

	// On a stale-placement or not-active error, drop the cached placement so the next call re-resolves
	// We do not retry the current call because the request body has already been consumed
	switch {
	case errors.Is(err, actor.ErrActorNotHosted), errors.Is(err, actor.ErrActorHalted), errors.Is(err, actor.ErrActorNotActive):
		h.invalidatePlacement(aRef)
	}

	return ct, resp, err
}

// doInvokeStreamLocal streams an invocation to an actor owned by this host
func (h *Host) doInvokeStreamLocal(ctx context.Context, aRef ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	return h.core.LockAndStream(ctx, aRef, activeOnly, func(invokeCtx context.Context, act *actorcore.ActiveActor, w actor.StreamResponseWriter) error {
		// The actor must implement the InvokeStream method to be called this way
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return errActorMethodUnsupported
		}

		// Stream the invocation; the actor reads body and writes the response to w
		return obj.InvokeStream(invokeCtx, method, reqContentType, body, w)
	})
}

// doInvokeStreamRemote streams an invocation to an actor owned by a peer host over the peer transport
func (h *Host) doInvokeStreamRemote(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	ct, resp, perr := h.peerClient.InvokeStream(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    aRef.ActorType,
		ActorID:      aRef.ActorID,
		Method:       method,
		ContentType:  reqContentType,
		ActiveOnly:   activeOnly,
	}, body)
	if perr != nil {
		return "", nil, protocolErrorToActor(perr)
	}

	return ct, resp, nil
}

// peerInvokeStream executes a streamed invocation for an actor owned by this host, on behalf of a peer caller
func (h *Host) peerInvokeStream(ctx context.Context, req protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error {
	aRef := ref.NewActorRef(req.ActorType, req.ActorID)

	invoke := func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		// The actor must implement the InvokeStream method to be called this way
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return nil, errActorMethodUnsupported
		}

		// Stream the invocation; the actor reads body and writes the response to w, which emits the response frame on its first write
		return nil, obj.InvokeStream(invokeCtx, req.Method, req.ContentType, body, w)
	}

	// An active-only invocation must not activate the actor here; this host is authoritative about whether it is active
	var err error
	if req.ActiveOnly {
		_, err = h.core.LockAndInvokeActive(ctx, aRef, invoke)
	} else {
		_, err = h.core.LockAndInvoke(ctx, aRef, invoke)
	}
	if err != nil {
		return invokeErrorToProtocol(err)
	}

	return nil
}

// peerInvokeObject executes an object invocation for an actor owned by this host, on behalf of a peer caller
func (h *Host) peerInvokeObject(ctx context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	aRef := ref.NewActorRef(req.ActorType, req.ActorID)

	invoke := func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		// The actor must implement the Invoke method to be called this way
		obj, ok := act.Instance.(actor.ActorInvoke)
		if !ok {
			return nil, errActorMethodUnsupported
		}

		// Decode the argument straight off the wire: the MessagePack decoder satisfies the Envelope interface
		var data actor.Envelope
		if len(req.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(req.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		// Invoke the actor
		r, invokeErr := obj.Invoke(invokeCtx, req.Method, data)
		if invokeErr != nil {
			return nil, fmt.Errorf("error from actor: %w", invokeErr)
		}

		return r, nil
	}

	// An active-only invocation must not activate the actor here; this host is authoritative about whether it is active
	var (
		res any
		err error
	)
	if req.ActiveOnly {
		res, err = h.core.LockAndInvokeActive(ctx, aRef, invoke)
	} else {
		res, err = h.core.LockAndInvoke(ctx, aRef, invoke)
	}
	if err != nil {
		return protocol.InvokeActorResponse{}, invokeErrorToProtocol(err)
	}

	// Encode the result for the response body
	if res == nil {
		return protocol.InvokeActorResponse{NoContent: true}, nil
	}
	outData, err := msgpack.Marshal(res)
	if err != nil {
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to serialize response using msgpack: %v", err)
	}

	return protocol.InvokeActorResponse{Data: outData}, nil
}
