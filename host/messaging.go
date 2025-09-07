package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
)

const (
	userAgentValue      = "actors/v1"
	contentTypeMsgpack  = "application/vnd.msgpack"
	headerHostID        = "X-Host-ID"
	headerContentType   = "Content-Type"
	headerContentLength = "Content-Length"
	headerUserAgent     = "User-Agent"
)

// Invoke performs the synchronous invocation of an actor running anywhere.
func (h *Host) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, out any) error {
	return h.doInvoke(ctx, ref.NewActorRef(actorType, actorID), method, data, out, false)
}

// InvokeLocal performs the synchronous invocation of an actor running on the current node.
// It returns ErrActorNotHosted if the actor is active on a different node.
func (h *Host) InvokeLocal(ctx context.Context, actorType string, actorID string, method string, data any, out any) error {
	return h.doInvoke(ctx, ref.NewActorRef(actorType, actorID), method, data, out, true)
}

func (h *Host) doInvoke(ctx context.Context, aRef ref.ActorRef, method string, data any, out any, local bool) error {
	// Look up the actor
	lar, err := h.actorProvider.LookupActor(ctx, aRef, components.LookupActorOpts{})
	if err != nil {
		return fmt.Errorf("failed to look up actor '%s': %w", aRef, err)
	}

	// If the host ID is different from the current, the invocation is for a remote actor
	if lar.HostID != h.hostID {
		if local {
			// Caller wanted to invoke a local actor only
			return actor.ErrActorNotHosted
		}

		// Invoke the remote actor
		return h.doInvokeRemote(ctx, aRef, lar, method, data, out)
	}

	// TODO: Handle ErrActorHalted:
	// - If local=false, retry: actor could be on a separate host
	// - If local=true, return ErrActorNotHosted
	res, err := h.doInvokeLocal(ctx, aRef, method, data)
	if err != nil {
		return err
	}

	// Assign res to out if not nil
	if out != nil {
		outVal := reflect.ValueOf(out)
		if outVal.IsNil() {
			// Caller doesn't care about return value
			return nil
		}
		if outVal.Kind() != reflect.Ptr {
			return errors.New("parameter out must be a non-nil pointer")
		}
		resVal := reflect.ValueOf(res)
		if !resVal.Type().AssignableTo(outVal.Elem().Type()) {
			return fmt.Errorf("cannot assign result of type %T to out of type %T", res, out)
		}
		outVal.Elem().Set(resVal)
	}

	return nil
}

func (h *Host) doInvokeRemote(ctx context.Context, aRef ref.ActorRef, lar components.LookupActorRes, method string, data any, out any) error {
	// Request URL
	u := url.URL{}
	u.Scheme = "https"
	u.Host = lar.Address
	u.Path = "/v1/invoke/" + aRef.String() + "/" + method

	// Encode the body using msgpack
	var br *io.PipeReader
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
		return fmt.Errorf("failed to create request: %w", err)
	}

	// TODO: Auth
	req.Header.Set(headerUserAgent, userAgentValue)
	req.Header.Set(headerHostID, lar.HostID)

	res, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
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
				return fmt.Errorf("request failed with status code %d and failed to read msgpack response body with error: %w", res.StatusCode, err)
			}
			// TODO: Check API errors
			// Some errors to consider include "req_invoke_hostid_mismatch" (re-fetch cached address and retry)
			return fmt.Errorf("request failed with status code %d and API error: %w", res.StatusCode, apiErr)
		}

		// Return the error as-is
		var body []byte
		body, err = io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("request failed with status code %d and failed to read raw response body with error: %w", res.StatusCode, err)
		}

		return fmt.Errorf("request failed with status code %d and raw error: %s", res.StatusCode, string(body))
	}

	// Parse the response body unless the status code is 204 No Content
	if res.StatusCode != http.StatusNoContent {
		err = dec.Decode(&out)
		if err != nil {
			return fmt.Errorf("failed to decode response body into output object: %w", err)
		}
	}

	return nil
}

func (h *Host) doInvokeLocal(ctx context.Context, ref ref.ActorRef, method string, data any) (any, error) {
	return h.lockAndInvokeFn(ctx, ref, func(ctx context.Context, act *activeActor) (any, error) {
		obj, ok := act.instance.(actor.ActorInvoke)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Invoke method", act.ActorType())
		}

		// Invoke the actor
		res, err := obj.Invoke(ctx, method, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return res, nil
	})
}

func (h *Host) lockAndInvokeFn(parentCtx context.Context, ref ref.ActorRef, fn func(context.Context, *activeActor) (any, error)) (any, error) {
	// Get the actor, which may create it
	act, err := h.getOrCreateActor(ref)
	if err != nil {
		return nil, err
	}

	// Create a context for this request, which allows us to stop it in-flight if needed
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

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
				cancel()
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

func (h *Host) executeAlarm(ctx context.Context, ref ref.AlarmRef, data any) error {
	_, err := h.lockAndInvokeFn(ctx, ref.ActorRef(), func(ctx context.Context, act *activeActor) (any, error) {
		obj, ok := act.instance.(actor.ActorAlarm)
		if !ok {
			return nil, fmt.Errorf("actor of type '%s' does not implement the Alarm method", act.ActorType())
		}

		// Invoke the actor
		err := obj.Alarm(ctx, ref.Name, data)
		if err != nil {
			return nil, fmt.Errorf("error from actor: %w", err)
		}

		return nil, nil
	})
	return err
}

func (h *Host) getOrCreateActor(ref ref.ActorRef) (*activeActor, error) {
	// Get the factory function
	fn, err := h.createActorFn(ref)
	if err != nil {
		return nil, err
	}

	// Get (or create) the actor
	actor, _ := h.actors.GetOrCompute(ref.String(), fn)

	return actor, nil
}

func (h *Host) createActorFn(ref ref.ActorRef) (func() *activeActor, error) {
	// We don't need a locking mechanism here as these maps are "locked" after the service has started
	factoryFn := h.actorFactories[ref.ActorType]
	if factoryFn == nil {
		return nil, errors.New("unsupported actor type")
	}

	idleTimeout := h.actorsConfig[ref.ActorType].IdleTimeout
	return func() *activeActor {
		instance := factoryFn(ref.ActorID, h.service)
		return newActiveActor(ref, instance, idleTimeout, h.idleActorProcessor, h.clock)
	}, nil
}
