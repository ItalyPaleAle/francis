package host

import (
	"fmt"
	"net/http"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	errMetadataActorDeactivationTimeout = "ActorDeactivationTimeout"
)

var (
	errApiReqInvokeHostIdEmpty    = newApiError("req_invoke_hostid_empty", http.StatusBadRequest, headerXHostID+" header is missing in the request")
	errApiReqInvokeHostIdMismatch = newApiError("req_invoke_hostid_mismatch", http.StatusConflict, "Request is for a different host ID")
	errApiReqInvokeBody           = newApiError("req_invoke_body", http.StatusBadRequest, "Failed to parse request body")
	errApiReqInvokeContentType    = newApiError("req_invoke_content_type", http.StatusBadRequest, "Unsupported content type")
	errApiActorNotHosted          = newApiError("actor_not_hosted", http.StatusNotFound, "Actor is not active on the current host")
	errApiActorHalted             = newApiError("actor_halted", http.StatusServiceUnavailable, "Actor is halted")
	errApiInvokeFailed            = newApiError("invoke_failed", http.StatusInternalServerError, "Actor invocation failed")
)

type apiError struct {
	Code       string
	Message    string
	InnerError error
	Metadata   map[string]string

	httpStatus int
}

func newApiError(code string, httpStatus int, message string) *apiError {
	return &apiError{
		Code:    code,
		Message: message,

		httpStatus: httpStatus,
	}
}

func (e apiError) WriteResponse(w http.ResponseWriter) {
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(w)

	w.Header().Add(headerContentType, contentTypeMsgpack)

	w.WriteHeader(e.httpStatus)

	// Ignore errors here
	_ = enc.Encode(e)
}

// Clone returns a cloned error with the data appended
func (e apiError) Clone(with ...func(*apiError)) *apiError {
	cloned := &apiError{
		Code:    e.Code,
		Message: e.Message,

		httpStatus: e.httpStatus,
	}

	for _, w := range with {
		w(cloned)
	}

	return cloned
}

func withInnerError(innerError error) func(*apiError) {
	return func(e *apiError) {
		e.InnerError = innerError
	}
}

func withMetadata(metadata map[string]string) func(*apiError) {
	return func(e *apiError) {
		e.Metadata = metadata
	}
}

// Error implements the error interface
func (e apiError) Error() string {
	return fmt.Sprintf("API error (%s): %s", e.Code, e.Message)
}

// Is allows comparing API errors by looking at their status codes
func (e apiError) Is(target error) bool {
	targetApiError, ok := target.(apiError)
	if !ok {
		return false
	}

	return targetApiError.Code == e.Code
}
