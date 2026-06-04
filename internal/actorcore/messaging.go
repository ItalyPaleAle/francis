package actorcore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// Placement is the resolved location of an actor
type Placement struct {
	// HostID is the stable identity of the host that owns the actor
	HostID string
	// Address is the peer address of the owning host, including the port
	Address string
}

// PlacementResolver resolves where an actor is placed
// Local mode resolves through the provider, while remote mode resolves through the runtime
type PlacementResolver interface {
	// Resolve returns the placement of an actor for a caller, allocating a new placement on any eligible host when needed
	// skipCache bypasses any local placement cache
	// activeOnly resolves only an already-active actor and never allocates
	Resolve(ctx context.Context, r ref.ActorRef, skipCache bool, activeOnly bool) (*Placement, error)

	// ConfirmLocal authoritatively claims an actor for this host, used on a local invocation before the actor is activated
	// It returns nil if the actor is, or can be, placed on this host, and actor.ErrActorNotHosted if it is owned elsewhere
	// This keeps placement provider-authoritative even when the caller routed here from a stale cached placement
	ConfirmLocal(ctx context.Context, r ref.ActorRef) error

	// Invalidate drops any cached placement for an actor so the next resolution re-resolves it
	Invalidate(r ref.ActorRef)

	// IsLocal reports whether a placement points at this host
	IsLocal(p *Placement) bool
}

// PeerInvoker invokes actors owned by other hosts over the peer transport
// It is satisfied by the WebTransport peer client
type PeerInvoker interface {
	// InvokeObject performs an object invocation against the actor on the peer at address
	InvokeObject(ctx context.Context, address string, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error)
	// InvokeStream performs a stream invocation against the actor on the peer at address, streaming the request body from body
	InvokeStream(ctx context.Context, address string, req protocol.InvokeActorRequest, body io.Reader) (string, io.ReadCloser, *protocol.Error)
}

// Invoke resolves an actor's placement and performs an object invocation, retrying once on a stale placement
func (m *Manager) Invoke(parentCtx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, method string, data any, activeOnly bool) (actor.Envelope, error) {
	// Resolve the placement and invoke, using the cache on the first attempt
	ap, err := resolver.Resolve(parentCtx, r, false, activeOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	env, retry, err := m.doInvokeObject(parentCtx, resolver, peer, r, ap, method, data, activeOnly)
	if !retry {
		return env, err
	}

	// The placement was stale: drop it, re-resolve with a fresh lookup, and try once more
	resolver.Invalidate(r)
	ap, err = resolver.Resolve(parentCtx, r, true, activeOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	env, _, err = m.doInvokeObject(parentCtx, resolver, peer, r, ap, method, data, activeOnly)
	return env, err
}

// doInvokeObject dispatches a single object invocation attempt to the local actor or a peer, depending on the placement
// The bool return is true when the attempt failed in a way that warrants re-resolving the placement and retrying
func (m *Manager) doInvokeObject(ctx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
	if resolver.IsLocal(ap) {
		return m.invokeLocalObject(ctx, resolver, r, method, data, activeOnly)
	}
	return m.invokePeerObject(ctx, peer, r, ap, method, data, activeOnly)
}

// invokeLocalObject invokes an actor owned by this host
// The bool return is true when the placement looks stale (the actor is inactive or owned elsewhere) so the caller can re-resolve
func (m *Manager) invokeLocalObject(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
	invoke := func(invokeCtx context.Context, act *ActiveActor) (any, error) {
		// The actor must implement the Invoke method to be called this way
		obj, ok := act.Instance.(actor.ActorInvoke)
		if !ok {
			return nil, ErrActorMethodUnsupported
		}

		// Invoke the actor, wrapping the data in an envelope as the receiver expects
		res, err := obj.Invoke(invokeCtx, method, NewObjectEnvelope(data))
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}
		return res, nil
	}

	res, err := m.lockAndInvokeLocal(ctx, resolver, r, activeOnly, invoke)
	if err != nil {
		// An inactive actor or a placement owned elsewhere means the actor may be active on another host - re-resolve
		retry := errors.Is(err, actor.ErrActorNotActive) || errors.Is(err, actor.ErrActorNotHosted)
		return nil, retry, err
	}

	// If there's a response, wrap it in an envelope
	if res != nil {
		return NewObjectEnvelope(res), false, nil
	}
	return nil, false, nil
}

// invokePeerObject invokes an actor owned by a peer host over the peer transport
// The bool return is true when the placement looks stale and the caller should re-resolve and retry
func (m *Manager) invokePeerObject(ctx context.Context, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, data any, activeOnly bool) (actor.Envelope, bool, error) {
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
	resp, perr := peer.InvokeObject(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    r.ActorType,
		ActorID:      r.ActorID,
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
	return NewObjectEnvelope(out), false, nil
}

// InvokeStream resolves an actor's placement and performs a streamed invocation
// Stale-placement and not-active errors are returned to the caller rather than retried, because the request body is a one-shot reader that cannot be replayed
func (m *Manager) InvokeStream(parentCtx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	// Resolve the placement and invoke
	ap, err := resolver.Resolve(parentCtx, r, false, activeOnly)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	if resolver.IsLocal(ap) {
		return m.invokeLocalStream(parentCtx, resolver, r, method, reqContentType, body, activeOnly)
	}

	// Stream to the peer that owns the actor
	ct, resp, retry, err := m.invokePeerStream(parentCtx, peer, r, ap, method, reqContentType, body, activeOnly)

	// On a stale or unavailable placement, drop the cached entry so the next call re-resolves
	// We do not retry the current call because the request body has already been consumed
	if retry {
		resolver.Invalidate(r)
	}
	return ct, resp, err
}

// invokeLocalStream streams an invocation to an actor owned by this host, bridging the actor's writer to a reader for the caller
func (m *Manager) invokeLocalStream(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	// Claim the actor for this host before activating it, unless this is an active-only invocation
	if !activeOnly {
		err := m.claimLocal(ctx, resolver, r)
		if err != nil {
			return "", nil, err
		}
	}

	return m.LockAndStream(ctx, r, activeOnly, func(invokeCtx context.Context, act *ActiveActor, w actor.StreamResponseWriter) error {
		// The actor must implement the InvokeStream method to be called this way
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return ErrActorMethodUnsupported
		}

		// Stream the invocation
		return obj.InvokeStream(invokeCtx, method, reqContentType, body, w)
	})
}

// invokePeerStream streams an invocation to an actor owned by a peer host over the peer transport
// The bool return is true when the placement looks stale or unavailable and the caller should re-resolve
func (m *Manager) invokePeerStream(ctx context.Context, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, bool, error) {
	ct, resp, perr := peer.InvokeStream(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    r.ActorType,
		ActorID:      r.ActorID,
		Method:       method,
		ContentType:  reqContentType,
		ActiveOnly:   activeOnly,
	}, body)
	if perr != nil {
		retry := perr.Retryable() || perr.Code == protocol.ErrCodeActorNotActive
		return "", nil, retry, protocolErrorToActor(perr)
	}

	return ct, resp, false, nil
}

// PeerInvokeObject executes an object invocation for an actor owned by this host, on behalf of a peer caller
func (m *Manager) PeerInvokeObject(ctx context.Context, resolver PlacementResolver, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	r := ref.NewActorRef(req.ActorType, req.ActorID)

	invoke := func(invokeCtx context.Context, act *ActiveActor) (any, error) {
		// The actor must implement the Invoke method to be called this way
		obj, ok := act.Instance.(actor.ActorInvoke)
		if !ok {
			return nil, ErrActorMethodUnsupported
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
		res, invokeErr := obj.Invoke(invokeCtx, req.Method, data)
		if invokeErr != nil {
			return nil, fmt.Errorf("error from actor: %w", invokeErr)
		}
		return res, nil
	}

	res, err := m.lockAndInvokeLocal(ctx, resolver, r, req.ActiveOnly, invoke)
	if err != nil {
		return protocol.InvokeActorResponse{}, InvokeErrorToProtocol(err)
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

// PeerInvokeStream executes a streamed invocation for an actor owned by this host, on behalf of a peer caller
// It reads the request body from body and writes the response through w, which emits the response frame on its first write
func (m *Manager) PeerInvokeStream(ctx context.Context, resolver PlacementResolver, req protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error {
	r := ref.NewActorRef(req.ActorType, req.ActorID)

	// Claim the actor for this host before activating it, unless this is an active-only invocation
	if !req.ActiveOnly {
		err := m.claimLocal(ctx, resolver, r)
		if err != nil {
			return InvokeErrorToProtocol(err)
		}
	}

	invoke := func(invokeCtx context.Context, act *ActiveActor) (any, error) {
		// The actor must implement the InvokeStream method to be called this way
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return nil, ErrActorMethodUnsupported
		}

		// Stream the invocation
		return nil, obj.InvokeStream(invokeCtx, req.Method, req.ContentType, body, w)
	}

	// An active-only invocation must not activate the actor here
	// This host is authoritative about whether it is active
	var err error
	if req.ActiveOnly {
		_, err = m.LockAndInvokeActive(ctx, r, invoke)
	} else {
		_, err = m.LockAndInvoke(ctx, r, invoke)
	}
	if err != nil {
		return InvokeErrorToProtocol(err)
	}

	return nil
}

// lockAndInvokeLocal runs fn against a local actor, claiming the actor for this host first unless the invocation is active-only
func (m *Manager) lockAndInvokeLocal(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, activeOnly bool, fn func(ctx context.Context, act *ActiveActor) (any, error)) (any, error) {
	// An active-only invocation is authoritative locally: only invoke if already active here, never activate or claim
	if activeOnly {
		return m.LockAndInvokeActive(ctx, r, fn)
	}

	// Authoritatively claim the actor for this host, then activate and invoke it
	err := m.claimLocal(ctx, resolver, r)
	if err != nil {
		return nil, err
	}

	return m.LockAndInvoke(ctx, r, fn)
}

// claimLocal authoritatively claims an actor for this host before it is activated
// An actor already active here is, by definition, ours and needs no claim, which keeps warm invocations free of a placement lookup
func (m *Manager) claimLocal(ctx context.Context, resolver PlacementResolver, r ref.ActorRef) error {
	_, active := m.Actors.Get(r.String())
	if active {
		return nil
	}

	return resolver.ConfirmLocal(ctx, r)
}
