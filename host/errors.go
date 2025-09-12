package host

import (
	"fmt"
	"net/http"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

type apiError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func newApiError(httpStatus int, code string, message string) *apiError {
	return &apiError{
		HTTPStatus: httpStatus,
		Code:       code,
		Message:    message,
	}
}

func newApiErrorf(httpStatus int, code string, message string, args ...any) *apiError {
	return &apiError{
		HTTPStatus: httpStatus,
		Code:       code,
		Message:    fmt.Sprintf(message, args...),
	}
}

func (e apiError) WriteResponse(w http.ResponseWriter) {
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(w)

	w.Header().Add(headerContentType, contentTypeMsgpack)

	w.WriteHeader(e.HTTPStatus)

	// Ignore errors here
	_ = enc.Encode(e)
}

// Error implements the error interface
func (e apiError) Error() string {
	return fmt.Sprintf("API error (%s): %s", e.Code, e.Message)
}
