package host

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

var (
	ErrActorHalted    = errors.New("actor is halted")
	ErrActorNotHosted = errors.New("actor is not active on the current host")
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
	b := &bytes.Buffer{}
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(b)

	w.Header().Add(headerContentType, contentTypeMsgpack)
	w.Header().Add(headerContentLength, strconv.Itoa(b.Len()))

	w.WriteHeader(e.HTTPStatus)

	// Ignore errors here
	_, _ = io.Copy(w, b)
}

// Error implements the error interface
func (e apiError) Error() string {
	return fmt.Sprintf("API error (%s): %s", e.Code, e.Message)
}
