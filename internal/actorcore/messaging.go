package actorcore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
	"github.com/italypaleale/francis/internal/types"
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
// readOnly requests a Peek rather than an Invoke: the actor is called through its ActorPeek interface, under the shared (read) lock
func (m *Manager) Invoke(parentCtx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, method string, data any, activeOnly bool, readOnly bool) (env actor.Envelope, err error) {
	// Span the whole logical invocation, including placement resolution and the single stale-placement retry
	ctx, span := tracing.Start(parentCtx, "actor.invoke",
		trace.WithAttributes(
			tracing.ActorType(r.ActorType),
			tracing.ActorID(r.ActorID),
			tracing.ActorMethod(method),
		),
	)
	defer func() {
		tracing.End(span, err)
	}()

	// Resolve the placement and invoke, using the cache on the first attempt
	ap, err := resolver.Resolve(ctx, r, false, activeOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	// Generate a stable request ID once so both the first attempt and any retry carry the same ID, allowing the owning host to coalesce them if the first is still in flight
	requestUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate request ID: %w", err)
	}
	requestID := requestUUID.String()
	span.SetAttributes(tracing.RequestID(requestID))

	env, retry, retryAfter, err := m.doInvokeObject(ctx, resolver, peer, r, ap, method, data, activeOnly, readOnly, requestID)
	if !retry {
		return env, err
	}

	// The placement was stale, so drop it before re-resolving
	resolver.Invalidate(r)

	// If there's a retry-after, honor it (for example from a host that is draining or at capacity)
	waitErr := m.waitRetryAfter(ctx, retryAfter)
	if waitErr != nil {
		return nil, waitErr
	}

	// Re-resolve with a fresh lookup and try once more
	ap, err = resolver.Resolve(ctx, r, true, activeOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	env, _, _, err = m.doInvokeObject(ctx, resolver, peer, r, ap, method, data, activeOnly, readOnly, requestID)
	return env, err
}

// maxInvokeRetryDelay caps how long a single object-invocation retry waits on a retry-after hint, so a large hint cannot stall the call
const maxInvokeRetryDelay = time.Second

// waitRetryAfter sleeps for the retry-after hint before the single invocation retry, capped and interruptible by the context
func (m *Manager) waitRetryAfter(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	d = min(d, maxInvokeRetryDelay)

	t := m.clock.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// doInvokeObject dispatches a single object invocation attempt to the local actor or a peer, depending on the placement
// The bool return is true when the attempt failed in a way that warrants re-resolving the placement and retrying, and the duration carries any retry-after hint to wait before that retry
func (m *Manager) doInvokeObject(ctx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, data any, activeOnly bool, readOnly bool, requestID string) (actor.Envelope, bool, time.Duration, error) {
	if resolver.IsLocal(ap) {
		return m.invokeLocalObject(ctx, resolver, r, method, data, activeOnly, readOnly, requestID)
	}

	return m.invokePeerObject(ctx, peer, r, ap, method, data, activeOnly, readOnly, requestID)
}

// invokeLocalObject invokes an actor owned by this host
// The bool return is true when the placement looks stale (the actor is inactive or owned elsewhere) so the caller can re-resolve
// A local miss carries no retry-after hint
func (m *Manager) invokeLocalObject(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, method string, data any, activeOnly bool, readOnly bool, requestID string) (actor.Envelope, bool, time.Duration, error) {
	invoke := func(invokeCtx context.Context, act *ActiveActor) (any, error) {
		// Stamp the request ID into the context so the actor can detect duplicates
		invokeCtx = actor.WithRequestID(invokeCtx, requestID)

		var (
			res any
			err error
		)
		switch {
		case readOnly:
			// The actor must implement the Peek method to be called this way
			obj, ok := act.Instance.(actor.ActorPeek)
			if !ok {
				return nil, ErrActorMethodUnsupported
			}

			// Mark the context read-only so the client rejects state-mutating calls made from within the handler
			invokeCtx = types.WithReadOnly(invokeCtx)
			res, err = obj.Peek(invokeCtx, method, NewObjectEnvelope(data))
		case method == ref.MethodBootstrap:
			// The reserved bootstrap lifecycle drives the actor's optional ActorBootstrapper hook rather than its Invoke handler
			// An actor that does not implement it has no durable work to set up, so this is a no-op
			// The bootstrap payload is delivered like any Invoke argument, wrapped in an envelope
			bs, ok := act.Instance.(actor.ActorBootstrapper)
			if ok {
				err = bs.Bootstrap(invokeCtx, NewObjectEnvelope(data))
			}
		default:
			// The actor must implement the Invoke method to be called this way
			obj, ok := act.Instance.(actor.ActorInvoke)
			if !ok {
				return nil, ErrActorMethodUnsupported
			}

			res, err = obj.Invoke(invokeCtx, method, NewObjectEnvelope(data))
		}
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}
		return res, nil
	}

	res, err := m.lockAndInvokeLocal(ctx, resolver, r, activeOnly, readOnly, invoke)
	if err != nil {
		// A halted, inactive, or elsewhere-owned actor means the actor may now be active on another host, so re-resolve and retry
		// ErrActorHalted is included because the manager removes the actor from its map after halting, so a re-resolve activates a fresh instance
		retry := errors.Is(err, actor.ErrActorNotActive) ||
			errors.Is(err, actor.ErrActorNotHosted) ||
			errors.Is(err, actor.ErrActorHalted)
		return nil, retry, 0, err
	}

	// If there's a response, wrap it in an envelope
	if res != nil {
		return NewObjectEnvelope(res), false, 0, nil
	}
	return nil, false, 0, nil
}

// invokePeerObject invokes an actor owned by a peer host over the peer transport
// The bool return is true when the placement looks stale and the caller should re-resolve and retry, and the duration carries the peer's retry-after hint
func (m *Manager) invokePeerObject(ctx context.Context, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, data any, activeOnly bool, readOnly bool, requestID string) (actor.Envelope, bool, time.Duration, error) {
	// Encode the argument as MessagePack for the request body
	var (
		argData []byte
		err     error
	)

	if data != nil {
		argData, err = msgpack.Marshal(data)
		if err != nil {
			return nil, false, 0, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}
	}

	// Invoke the actor on the peer that owns it, carrying the request ID so the peer can coalesce a concurrent retry
	resp, perr := peer.InvokeObject(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    r.ActorType,
		ActorID:      r.ActorID,
		Method:       method,
		Data:         argData,
		ActiveOnly:   activeOnly,
		ReadOnly:     readOnly,
		RequestID:    requestID,
	})
	if perr != nil {
		// Re-resolve and retry on a stale placement, a halted actor, a transport failure, or an active-only miss that may be active elsewhere
		retry := perr.Retryable() || perr.Code == protocol.ErrCodeActorNotActive
		// Carry the retry-after hint so the caller waits a beat before re-resolving
		retryAfter, _ := perr.RetryAfter()
		return nil, retry, retryAfter, protocolErrorToActor(perr)
	}

	// Decode the result into an envelope
	if len(resp.Data) == 0 {
		return nil, false, 0, nil
	}

	var out any
	err = msgpack.Unmarshal(resp.Data, &out)
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to decode response body using msgpack: %w", err)
	}

	return NewObjectEnvelope(out), false, 0, nil
}

// InvokeStream resolves an actor's placement and performs a streamed invocation
// Stale-placement and not-active errors are returned to the caller rather than retried, because the request body is a one-shot reader that cannot be replayed
// readOnly requests a PeekStream rather than an InvokeStream: the actor is called through its ActorPeekStream interface, under the shared (read) lock for the whole call
func (m *Manager) InvokeStream(parentCtx context.Context, resolver PlacementResolver, peer PeerInvoker, r ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool, readOnly bool) (string, io.ReadCloser, error) {
	// Span the invocation setup: the span ends when the response metadata is ready, before the caller streams the body
	ctx, span := tracing.Start(parentCtx, "actor.invoke.stream",
		trace.WithAttributes(
			tracing.ActorType(r.ActorType),
			tracing.ActorID(r.ActorID),
			tracing.ActorMethod(method),
		),
	)
	defer span.End()

	// Resolve the placement and invoke
	ap, err := resolver.Resolve(ctx, r, false, activeOnly)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	if resolver.IsLocal(ap) {
		ct, resp, err := m.invokeLocalStream(ctx, resolver, r, method, reqContentType, body, activeOnly, readOnly)
		if err != nil && (errors.Is(err, actor.ErrActorNotActive) || errors.Is(err, actor.ErrActorNotHosted)) {
			// A stale cached placement routed us here, but the actor is inactive or owned elsewhere: drop the entry so the next call re-resolves
			// We do not retry the current call because the request body has already been consumed, mirroring the object path's stale-local handling
			resolver.Invalidate(r)
		}

		return ct, resp, err
	}

	// Stream to the peer that owns the actor
	ct, resp, retry, err := m.invokePeerStream(ctx, peer, r, ap, method, reqContentType, body, activeOnly, readOnly)

	// On a stale or unavailable placement, drop the cached entry so the next call re-resolves
	// We do not retry the current call because the request body has already been consumed
	if retry {
		resolver.Invalidate(r)
	}
	return ct, resp, err
}

// invokeLocalStream streams an invocation to an actor owned by this host, bridging the actor's writer to a reader for the caller
func (m *Manager) invokeLocalStream(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool, readOnly bool) (string, io.ReadCloser, error) {
	// Claim the actor for this host before activating it, unless this is an active-only invocation
	if !activeOnly {
		err := m.claimLocal(ctx, resolver, r)
		if err != nil {
			return "", nil, err
		}
	}

	return m.LockAndStream(ctx, r, activeOnly, readOnly, func(invokeCtx context.Context, act *ActiveActor, w actor.StreamResponseWriter) error {
		if readOnly {
			// The actor must implement the PeekStream method to be called this way
			obj, ok := act.Instance.(actor.ActorPeekStream)
			if !ok {
				return ErrActorMethodUnsupported
			}

			// Mark the context read-only so the client rejects state-mutating calls made from within the handler
			invokeCtx = types.WithReadOnly(invokeCtx)
			return obj.PeekStream(invokeCtx, method, reqContentType, body, w)
		}

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
func (m *Manager) invokePeerStream(ctx context.Context, peer PeerInvoker, r ref.ActorRef, ap *Placement, method string, reqContentType string, body io.Reader, activeOnly bool, readOnly bool) (string, io.ReadCloser, bool, error) {
	ct, resp, perr := peer.InvokeStream(ctx, ap.Address, protocol.InvokeActorRequest{
		TargetHostID: ap.HostID,
		ActorType:    r.ActorType,
		ActorID:      r.ActorID,
		Method:       method,
		ContentType:  reqContentType,
		ActiveOnly:   activeOnly,
		ReadOnly:     readOnly,
	}, body)
	if perr != nil {
		retry := perr.Retryable() || perr.Code == protocol.ErrCodeActorNotActive
		return "", nil, retry, protocolErrorToActor(perr)
	}

	return ct, resp, false, nil
}

// PeerInvokeObject executes an object invocation for an actor owned by this host, on behalf of a peer caller
func (m *Manager) PeerInvokeObject(ctx context.Context, resolver PlacementResolver, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	// If the caller provided a request ID, coalesce any concurrent duplicate into the in-flight execution so the actor runs at most once even when the caller retried after losing the response
	if req.RequestID != "" {
		type dedupResult struct {
			resp protocol.InvokeActorResponse
			perr *protocol.Error
		}
		val, _, _ := m.inflightDedup.Do(req.RequestID, func() (any, error) {
			resp, perr := m.peerInvokeObjectCore(ctx, resolver, req)
			return dedupResult{resp: resp, perr: perr}, nil
		})
		res, _ := val.(dedupResult)
		return res.resp, res.perr
	}

	return m.peerInvokeObjectCore(ctx, resolver, req)
}

// peerInvokeObjectCore runs the actor invocation for PeerInvokeObject
func (m *Manager) peerInvokeObjectCore(ctx context.Context, resolver PlacementResolver, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	r := ref.NewActorRef(req.ActorType, req.ActorID)

	invoke := func(invokeCtx context.Context, act *ActiveActor) (rRes any, rErr error) {
		// Decode the argument straight off the wire: the MessagePack decoder satisfies the Envelope interface
		var data actor.Envelope
		if len(req.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(req.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		// Stamp the request ID into the context so the actor can detect duplicates
		invokeCtx = actor.WithRequestID(invokeCtx, req.RequestID)

		switch {
		case req.ReadOnly:
			// The actor must implement the Peek method to be called this way
			obj, ok := act.Instance.(actor.ActorPeek)
			if !ok {
				return nil, ErrActorMethodUnsupported
			}

			// Mark the context read-only so the client rejects state-mutating calls made from within the handler
			invokeCtx = types.WithReadOnly(invokeCtx)
			rRes, rErr = obj.Peek(invokeCtx, req.Method, data)
		case req.Method == ref.MethodBootstrap:
			// The reserved bootstrap lifecycle drives the actor's optional ActorBootstrapper hook rather than its Invoke handler
			// An actor that does not implement it has no durable work to set up, so this is a no-op
			// The decoded envelope is forwarded so Bootstrap can access BootstrapData when remote hosts trigger it
			bs, ok := act.Instance.(actor.ActorBootstrapper)
			if ok {
				rErr = bs.Bootstrap(invokeCtx, data)
			}
		default:
			// The actor must implement the Invoke method to be called this way
			obj, ok := act.Instance.(actor.ActorInvoke)
			if !ok {
				return nil, ErrActorMethodUnsupported
			}

			rRes, rErr = obj.Invoke(invokeCtx, req.Method, data)
		}
		if rErr != nil {
			return nil, fmt.Errorf("error from actor: %w", rErr)
		}
		return rRes, nil
	}

	res, err := m.lockAndInvokeLocal(ctx, resolver, r, req.ActiveOnly, req.ReadOnly, invoke)
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
		if req.ReadOnly {
			// The actor must implement the PeekStream method to be called this way
			obj, ok := act.Instance.(actor.ActorPeekStream)
			if !ok {
				return nil, ErrActorMethodUnsupported
			}

			// Mark the context read-only so the client rejects state-mutating calls made from within the handler
			invokeCtx = types.WithReadOnly(invokeCtx)
			return nil, obj.PeekStream(invokeCtx, req.Method, req.ContentType, body, w)
		}

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
	switch {
	case req.ActiveOnly && req.ReadOnly:
		_, err = m.LockAndPeekActive(ctx, r, invoke)
	case req.ActiveOnly:
		_, err = m.LockAndInvokeActive(ctx, r, invoke)
	case req.ReadOnly:
		_, err = m.LockAndPeek(ctx, r, invoke)
	default:
		_, err = m.LockAndInvoke(ctx, r, invoke)
	}
	if err != nil {
		return InvokeErrorToProtocol(err)
	}

	return nil
}

// lockAndInvokeLocal runs fn against a local actor, claiming the actor for this host first unless the invocation is active-only
// readOnly selects the shared-lock LockAndPeek(Active) path instead of the exclusive-lock LockAndInvoke(Active) path
func (m *Manager) lockAndInvokeLocal(ctx context.Context, resolver PlacementResolver, r ref.ActorRef, activeOnly bool, readOnly bool, fn func(ctx context.Context, act *ActiveActor) (any, error)) (any, error) {
	// An active-only invocation is authoritative locally: only invoke if already active here, never activate or claim
	if activeOnly {
		if readOnly {
			return m.LockAndPeekActive(ctx, r, fn)
		}

		return m.LockAndInvokeActive(ctx, r, fn)
	}

	// Authoritatively claim the actor for this host, then activate and invoke it
	err := m.claimLocal(ctx, resolver, r)
	if err != nil {
		return nil, err
	}

	if readOnly {
		return m.LockAndPeek(ctx, r, fn)
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
