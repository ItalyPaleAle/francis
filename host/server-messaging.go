package host

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/actors/actor"
)

// Handler for POST /v1/invoke/{actorType}/{actorID}/{method}
func (h *Host) handleMessageRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Validate the request is for the correct host
	// It can happen that clients make calls to incorrect hosts if the app just (re-)started and their cached data is stale
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

	// Read the request body
	var reqData any
	ct := r.Header.Get(headerContentType)
	switch ct {
	case contentTypeMsgpack:
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		dec.Reset(r.Body)
		err = dec.Decode(&reqData)
		if err != nil {
			errApiReqInvokeBody.
				Clone(withInnerError(err)).
				WriteResponse(w)
			return
		}
	case "":
		// Ignore the body if the content type is unsupported
	default:
		errApiReqInvokeContentType.WriteResponse(w)
		return
	}

	// Invoke the actor
	actorType := r.PathValue("actorType")
	outData, err := h.InvokeLocal(r.Context(), actorType, r.PathValue("actorID"), r.PathValue("method"), reqData)
	switch {
	case errors.Is(err, actor.ErrActorNotHosted):
		errApiActorNotHosted.WriteResponse(w)
		return
	case errors.Is(err, actor.ErrActorHalted):
		// Get the deactivation timeout for the actor type (in ms), and include the ActorDeactivationTimeout metadata key to aid the caller in deciding how long to wait
		dt := h.deactivationTimeoutForActorType(actorType).Milliseconds()
		errApiActorHalted.
			Clone(withMetadata(map[string]string{
				errMetadataActorDeactivationTimeout: strconv.FormatInt(dt, 10),
			})).
			WriteResponse(w)
		return
	case err != nil:
		errApiInvokeFailed.
			Clone(withInnerError(err)).
			WriteResponse(w)
		return
	}

	// If there's no output data, respond with 204 No Content
	if outData == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Respond with the body
	// Set the content type for msgpack if we have a body
	w.Header().Set(headerContentType, contentTypeMsgpack)
	w.WriteHeader(http.StatusOK)
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(w)
	err = enc.Encode(outData)
	if err != nil {
		// At this point all we can do is log the error
		h.log.ErrorContext(r.Context(), "Error writing response body", slog.Any("error", err))
		return
	}
}
