package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/italypaleale/go-kit/utils"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/types"
)

// errActorStreamUnsupported is returned when an actor does not implement the InvokeStream method
var errActorStreamUnsupported = errors.New("actor does not implement the InvokeStream method")

// InvokeStream performs a streamed invocation of an actor running anywhere.
// Stale-placement and not-active errors are returned to the caller rather than retried, because the request body is a one-shot reader that cannot be replayed.
func (h *Host) InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, optsFn ...actor.InvokeOption) (string, io.ReadCloser, error) {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	// Resolve the placement
	ap, err := h.lookupActor(ctx, aRef, false, opts.ActiveOnly)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up actor: %w", err)
	}

	// Invoke locally if the actor is on this host
	if h.isLocal(ap) {
		return h.doInvokeStreamLocal(ctx, aRef, method, reqContentType, body, opts.ActiveOnly)
	}

	// Otherwise stream to the owning host over HTTP
	ct, resp, err := h.doInvokeStreamRemote(ctx, aRef, ap, method, reqContentType, body, opts.ActiveOnly)

	// On a stale-placement or not-active error, drop the cached placement so the next call re-resolves
	// We do not retry the current call because the request body has already been consumed
	switch {
	case errors.Is(err, actor.ErrActorNotHosted), errors.Is(err, actor.ErrActorHalted), errors.Is(err, actor.ErrActorNotActive):
		h.placementCache.Delete(aRef.String())
	}

	return ct, resp, err
}

// doInvokeStreamLocal streams an invocation to an actor owned by this host, bridging the actor's writer to a reader for the caller
func (h *Host) doInvokeStreamLocal(ctx context.Context, aRef ref.ActorRef, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	return h.core.LockAndStream(ctx, aRef, activeOnly, func(invokeCtx context.Context, act *actorcore.ActiveActor, w actor.StreamResponseWriter) error {
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return errActorStreamUnsupported
		}

		// Stream the invocation; the actor reads body and writes the response to w
		return obj.InvokeStream(invokeCtx, method, reqContentType, body, w)
	})
}

// doInvokeStreamRemote streams an invocation to an actor owned by a remote host over HTTP
func (h *Host) doInvokeStreamRemote(ctx context.Context, aRef ref.ActorRef, ap *actorPlacement, method string, reqContentType string, body io.Reader, activeOnly bool) (string, io.ReadCloser, error) {
	// Request URL
	u := url.URL{
		Scheme: "https",
		Host:   ap.Address,
		Path:   "/v1/invoke-stream/" + aRef.String() + "/" + method,
	}

	// The request body streams directly to the owning host
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(headerUserAgent, userAgentValue)
	req.Header.Set(headerXHostID, ap.HostID)
	if reqContentType != "" {
		req.Header.Set(headerContentType, reqContentType)
	}

	// An active-only invocation must be enforced by the owning host, which is authoritative about whether the actor is active
	if activeOnly {
		req.Header.Set(headerActiveOnly, "1")
	}

	// Peer authenticator may modify the request header
	err = h.peerAuth.UpdateRequest(req)
	if err != nil {
		return "", nil, fmt.Errorf("peer authenticator failed to modify the request: %w", err)
	}

	res, err := h.client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("request failed: %w", err)
	}

	// On a non-2XX status, parse and map the error, then release the body
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		defer res.Body.Close()
		return "", nil, h.streamResponseError(res)
	}

	// On success the response body is the streamed response, which the caller reads and closes
	return res.Header.Get(headerContentType), res.Body, nil
}

// streamResponseError maps a non-2XX stream response to an error, translating the well-known API errors to their public actor equivalents
func (h *Host) streamResponseError(res *http.Response) error {
	if res.Header.Get(headerContentType) == contentTypeMsgpack {
		var apiErr apiError
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		dec.Reset(res.Body)
		err := dec.Decode(&apiErr)
		if err != nil {
			return fmt.Errorf("request failed with status code %d and failed to read msgpack response body with error: %w", res.StatusCode, err)
		}

		switch apiErr.Code {
		case errApiActorNotActive.Code:
			return actor.ErrActorNotActive
		case errApiActorNotHosted.Code, errApiReqInvokeHostIdMismatch.Code:
			return actor.ErrActorNotHosted
		case errApiActorHalted.Code:
			return actor.ErrActorHalted
		}

		return fmt.Errorf("request failed with status code %d and API error: %w", res.StatusCode, apiErr)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		// Couldn't read the body
		return fmt.Errorf("request failed with status code %d and failed to read raw response body with error: %w", res.StatusCode, err)
	}

	// Error with body
	return fmt.Errorf("request failed with status code %d and raw error: %s", res.StatusCode, string(body))
}

// handleStreamRequest is the handler for POST /v1/invoke-stream/{actorType}/{actorID}/{method}
func (h *Host) handleStreamRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Validate the request is for the correct host
	reqHostID := r.Header.Get(headerXHostID)
	if reqHostID == "" {
		errApiReqInvokeHostIdEmpty.WriteResponse(w)
		return
	} else if reqHostID != h.hostID {
		errApiReqInvokeHostIdMismatch.WriteResponse(w)
		return
	}

	// Authorize the request
	ok, err := h.peerAuth.ValidateIncomingRequest(r)
	if err != nil {
		errInternal.WriteResponse(w)
		return
	}
	if !ok {
		errApiReqInvokeUnauthorized.WriteResponse(w)
		return
	}

	// A caller may request active-only invocation, which must not activate the actor on this host
	invokeOpts := make([]actor.InvokeOption, 0, 1)
	if utils.IsTruthy(r.Header.Get(headerActiveOnly)) {
		invokeOpts = append(invokeOpts, actor.WithInvokeActiveOnly())
	}

	// Run the actor locally, writing the response straight to the HTTP response writer
	actorType := r.PathValue("actorType")
	sw := &httpStreamWriter{w: w}
	err = h.invokeStreamLocal(r.Context(), actorType, r.PathValue("actorID"), r.PathValue("method"), r.Header.Get(headerContentType), r.Body, sw, invokeOpts...)

	// If the response has already started, the status line is sent, so a late failure can only truncate the body
	if err != nil && sw.flushed {
		h.log.ErrorContext(r.Context(), "Stream actor returned an error after the response started", slog.Any("error", err))
		return
	}

	switch {
	case err == nil:
		// Nothing written means an empty response
		if !sw.flushed {
			w.WriteHeader(http.StatusNoContent)
		}
	case errors.Is(err, actor.ErrActorNotHosted):
		errApiActorNotHosted.WriteResponse(w)
	case errors.Is(err, actor.ErrActorNotActive):
		errApiActorNotActive.WriteResponse(w)
	case errors.Is(err, actor.ErrActorHalted):
		dt := h.deactivationTimeoutForActorType(actorType).Milliseconds()
		errApiActorHalted.
			Clone(withMetadata(map[string]string{
				errMetadataActorDeactivationTimeout: strconv.FormatInt(dt, 10),
			})).
			WriteResponse(w)
	case errors.Is(err, errActorStreamUnsupported):
		errApiInvokeModeUnsupported.WriteResponse(w)
	default:
		errApiInvokeFailed.
			Clone(withInnerError(err)).
			WriteResponse(w)
	}
}

// invokeStreamLocal runs a streamed invocation against an actor on the current node, writing the response to sw.
// It mirrors InvokeLocal: for an active-only invocation the local active-actor table is authoritative, otherwise it confirms the actor is placed here before activating it.
func (h *Host) invokeStreamLocal(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, sw actor.StreamResponseWriter, optsFn ...actor.InvokeOption) error {
	opts := &types.InvokeOpts{}
	for _, fn := range optsFn {
		fn(opts)
	}

	aRef := ref.NewActorRef(actorType, actorID)

	invoke := func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		obj, ok := act.Instance.(actor.ActorStream)
		if !ok {
			return nil, errActorStreamUnsupported
		}
		return nil, obj.InvokeStream(invokeCtx, method, reqContentType, body, sw)
	}

	// For an active-only invocation the local active-actor table is authoritative
	if opts.ActiveOnly {
		_, err := h.core.LockAndInvokeActive(ctx, aRef, invoke)
		return err
	}

	// Confirm the actor is placed on this host before activating it
	ap, err := h.lookupActor(ctx, aRef, false, false)
	if err != nil {
		return fmt.Errorf("failed to look up actor: %w", err)
	}
	if !h.isLocal(ap) {
		return actor.ErrActorNotHosted
	}

	_, err = h.core.LockAndInvoke(ctx, aRef, invoke)
	return err
}

// httpStreamWriter is a StreamResponseWriter that writes a streamed response to an HTTP response writer
type httpStreamWriter struct {
	w           http.ResponseWriter
	contentType string
	flushed     bool
}

// SetContentType records the content type, which is written as a header on the first Write
func (sw *httpStreamWriter) SetContentType(contentType string) {
	if !sw.flushed {
		sw.contentType = contentType
	}
}

// Write sends the response headers on the first call, then writes the body bytes
func (sw *httpStreamWriter) Write(p []byte) (int, error) {
	if !sw.flushed {
		sw.flushed = true
		if sw.contentType != "" {
			sw.w.Header().Set(headerContentType, sw.contentType)
		}
		sw.w.WriteHeader(http.StatusOK)
	}

	return sw.w.Write(p)
}
