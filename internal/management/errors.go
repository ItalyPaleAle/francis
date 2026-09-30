package management

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/italypaleale/francis/protocol"
)

// Error codes returned in the body of error responses
const (
	CodeBadRequest           = "badRequest"
	CodeUnauthorized         = "unauthorized"
	CodeForbidden            = "forbidden"
	CodeNotFound             = "notFound"
	CodeNotApplicable        = "notApplicable"
	CodeMethodNotAllowed     = "methodNotAllowed"
	CodeExclusiveLeaseHeld   = "exclusiveLeaseHeld"
	CodeLastServer           = "lastServer"
	CodeHostUnavailable      = "hostUnavailable"
	CodeHostReattached       = "hostReattached"
	CodeNoHostsReachable     = "noHostsReachable"
	CodeEventHistoryDisabled = "eventHistoryDisabled"
	CodePayloadTooLarge      = "payloadTooLarge"
	CodeStateNotDecodable    = "stateNotDecodable"
	CodeTimeout              = "timeout"
	CodeInternal             = "internal"
)

// Errors a Backend returns, which the handlers map to responses
var (
	// ErrNotApplicable is returned for an operation that does not exist in the backend's topology
	ErrNotApplicable = errors.New("not applicable in this topology")
	// ErrHostUnavailable is returned when the host, or the runtime owning its session, cannot be reached
	ErrHostUnavailable = errors.New("host is unavailable")
	// ErrHostReattached is returned when the host's session changed before it received the request
	ErrHostReattached = errors.New("host session changed before the request was delivered")
)

// apiError is the body of an error response
type apiError struct {
	status int

	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"requestId,omitempty"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *apiError) Error() string {
	return e.Code + ": " + e.Message
}

func newAPIError(status int, code string, msg string) *apiError {
	return &apiError{status: status, Code: code, Message: msg}
}

func newAPIErrorf(status int, code string, format string, args ...any) *apiError {
	return newAPIError(status, code, fmt.Sprintf(format, args...))
}

func errBadRequest(format string, args ...any) *apiError {
	return newAPIErrorf(http.StatusBadRequest, CodeBadRequest, format, args...)
}

func errNotFound(format string, args ...any) *apiError {
	return newAPIErrorf(http.StatusNotFound, CodeNotFound, format, args...)
}

func (e *apiError) retryable() *apiError {
	e.Retryable = true
	return e
}

func (e *apiError) withDetails(details map[string]any) *apiError {
	e.Details = details
	return e
}

// backendError maps an error returned by a Backend to a response, or returns nil when it is not one of the known errors
func backendError(err error) *apiError {
	switch {
	case errors.Is(err, ErrNotApplicable):
		return newAPIError(http.StatusNotFound, CodeNotApplicable, "this resource is not available in this topology")
	case errors.Is(err, ErrHostReattached):
		return newAPIError(http.StatusConflict, CodeHostReattached, "the host kept reconnecting before it received the request; send it again").retryable()
	case errors.Is(err, ErrHostUnavailable):
		return newAPIErrorf(http.StatusServiceUnavailable, CodeHostUnavailable, "the host could not be reached: %v", err).retryable()
	default:
		return nil
	}
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// writeError writes an error response, stamped with the request ID
func writeError(w http.ResponseWriter, r *http.Request, e *apiError) {
	e.RequestID = requestIDFromContext(r.Context())
	writeJSON(w, e.status, e)
}

// FromProtocolError maps the protocol errors that mean a host could not be reached to ErrHostReattached and ErrHostUnavailable, and returns any other error as-is
// Backends use it for the errors of requests sent to hosts and runtime replicas
func FromProtocolError(err error) error {
	var perr *protocol.Error
	if !errors.As(err, &perr) {
		return err
	}

	switch perr.Code {
	case protocol.ErrCodeHostReattached:
		return fmt.Errorf("%w: %s", ErrHostReattached, perr.Message)
	case protocol.ErrCodeHostUnavailable, protocol.ErrCodeHostMismatch, protocol.ErrCodeTransportFailure:
		return fmt.Errorf("%w: %s", ErrHostUnavailable, perr.Message)
	default:
		return perr
	}
}
