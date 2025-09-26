package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/activeactor"
	"github.com/italypaleale/francis/internal/objectenvelope"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/types"
)

const (
	userAgentValue     = "actors/v1"
	contentTypeMsgpack = "application/vnd.msgpack"
	headerXHostID      = "X-Host-Id"
	headerContentType  = "Content-Type"
	headerUserAgent    = "User-Agent"
)

// Invoke performs the synchronous invocation of an actor running anywhere.
func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, optsFn ...actor.InvokeOption) (actor.Envelope, error) {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	// Look up the actor
	ap, err := h.lookupActor(ctx, aRef, false, opts.ActiveOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	// Check if we're invoking a remote host
	if !h.isLocal(ap) {
		return h.doInvokeRemote(ctx, aRef, ap, method, data)
	}

	// TODO: Handle ErrActorHalted: retry (after new lookup without cache): actor could be on a separate host
	return h.doInvokeLocal(ctx, ref.NewActorRef(actorType, actorID), method, data)
}

// InvokeLocal performs the synchronous invocation of an actor running on the current node.
// Returns ErrActorNotHosted if the actor is active on a different node.
// Returns ErrActorHalted if the actor is being halted (callers should retry after a delay).
func (h *Host) InvokeLocal(ctx context.Context, actorType string, actorID string, method string, data any, optsFn ...actor.InvokeOption) (actor.Envelope, error) {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	// Look up the actor
	ap, err := h.lookupActor(ctx, aRef, false, opts.ActiveOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	// Cannot invoke remote actors in this method
	if !h.isLocal(ap) {
		return nil, actor.ErrActorNotHosted
	}

	return h.doInvokeLocal(ctx, ref.NewActorRef(actorType, actorID), method, data)
}

func (h *Host) doInvokeLocal(ctx context.Context, ref ref.ActorRef, method string, data any) (actor.Envelope, error) {
	res, err := h.lockAndInvokeFn(ctx, ref, func(ctx context.Context, act *activeactor.Instance) (any, error) {
		obj, ok := act.Instance().(actor.ActorInvoke)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Invoke method", act.ActorType())
		}

		// Invoke the actor
		// We wrap the data in an envelope as the receiver expects
		res, err := obj.Invoke(ctx, method, objectenvelope.NewEnvelope(data))
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return res, nil
	})
	if err != nil {
		return nil, err
	}

	// If there's a response, wrap in an envelope
	if res != nil {
		return objectenvelope.NewEnvelope(res), nil
	}

	return nil, nil
}

func (h *Host) doInvokeRemote(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, data any) (actor.Envelope, error) {
	// Request URL
	u := url.URL{}
	u.Scheme = "https"
	u.Host = ap.Address
	u.Path = "/v1/invoke/" + aRef.String() + "/" + method

	// Encode the body using msgpack
	var br io.ReadCloser
	if data != nil {
		var bw *io.PipeWriter
		br, bw = io.Pipe()
		go func() {
			enc := msgpack.GetEncoder()
			defer msgpack.PutEncoder(enc)
			enc.Reset(bw)
			be := enc.Encode(data)
			if be != nil {
				bw.CloseWithError(be)
			}
		}()
	}

	// TODO: retry in case of errors?
	// TODO: Set timeout in context
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), br)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(headerUserAgent, userAgentValue)
	req.Header.Set(headerXHostID, ap.HostID)

	if br != nil {
		// Set the content type for msgpack if we have a body
		req.Header.Set(headerContentType, contentTypeMsgpack)
	}

	// Peer authenticator may modify the request header
	err = h.peerAuth.UpdateRequest(req)
	if err != nil {
		return nil, fmt.Errorf("peer authenticator failed to modify the request: %w", err)
	}

	res, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()

	dec := msgpack.GetDecoder()
	dec.Reset(res.Body)
	defer msgpack.PutDecoder(dec)

	// Handle API errors
	// The response must have status 2XX in case of success
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		// Parse the error in the body
		// First, check if it's encoded with msgpack
		if res.Header.Get(headerContentType) == contentTypeMsgpack {
			var apiErr apiError
			err = dec.Decode(&apiErr)
			if err != nil {
				return nil, fmt.Errorf("request failed with status code %d and failed to read msgpack response body with error: %w", res.StatusCode, err)
			}
			// TODO: Check API errors
			// Some errors to consider include "req_invoke_hostid_mismatch" (re-fetch cached address and retry)
			return nil, fmt.Errorf("request failed with status code %d and API error: %w", res.StatusCode, apiErr)
		}

		// Return the error as-is
		var body []byte
		body, err = io.ReadAll(res.Body)
		if err != nil {
			return nil, fmt.Errorf("request failed with status code %d and failed to read raw response body with error: %w", res.StatusCode, err)
		}

		return nil, fmt.Errorf("request failed with status code %d and raw error: %s", res.StatusCode, string(body))
	}

	// Parse the response body unless the status code is 204 No Content
	if res.StatusCode != http.StatusNoContent {
		var out any
		err = dec.Decode(&out)
		if err != nil {
			return nil, fmt.Errorf("failed to decode response body using msgpack: %w", err)
		}

		return objectenvelope.NewEnvelope(out), nil
	}

	return nil, nil
}

func (h *Host) lockAndInvokeFn(parentCtx context.Context, ref ref.ActorRef, fn func(context.Context, *activeactor.Instance) (any, error)) (any, error) {
	// Get the actor, which may create it
	act, err := h.getOrCreateActor(ref)
	if err != nil {
		return nil, err
	}

	// Create a context for this request, which allows us to stop it in-flight if needed
	ctx, cancel := context.WithCancelCause(parentCtx)
	defer cancel(nil)

	// Acquire a lock for turn-based concurrency
	haltCh, err := act.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for actor: %w", err)
	}
	defer act.Unlock()

	go func() {
		select {
		case <-haltCh:
			// The actor is being halted, so we need to cancel the context
			t := h.clock.NewTimer(h.shutdownGracePeriod)
			select {
			case <-t.C():
				// Graceful timeout has passed: forcefully cancel the context
				cancel(actor.ErrActorHalted)
				return
			case <-ctx.Done():
				// The method is returning (either fn() is done, or context was canceled)
				if !t.Stop() {
					<-t.C()
				}
				return
			}
		case <-ctx.Done():
			// The method is returning (either fn() is done, or context was canceled)
			return
		}
	}()

	return fn(ctx, act)
}

func (h *Host) getOrCreateActor(ref ref.ActorRef) (*activeactor.Instance, error) {
	// Get the factory function
	fn, err := h.createActorFn(ref)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(ref.String(), fn)

	return actor, nil
}

func (h *Host) createActorFn(ref ref.ActorRef) (func() *activeactor.Instance, error) {
	// We don't need a locking mechanism here as these maps are "locked" after the service has started
	factoryFn := h.actorFactories[ref.ActorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	idleTimeout := h.actorsConfig[ref.ActorType].IdleTimeout
	return func() *activeactor.Instance {
		instance := factoryFn(ref.ActorID, h.service)
		return activeactor.NewInstance(ref, instance, idleTimeout, h.idleActorProcessor, h.clock)
	}, nil
}
