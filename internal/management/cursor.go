package management

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/italypaleale/francis/components"
)

// encodeCursor renders a pagination position as an opaque token
func encodeCursor(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		// Cursors are small structs of strings, numbers and UUIDs, which always marshal
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeCursor parses a token produced by encodeCursor
// An empty token leaves dst unchanged
func decodeCursor(token string, dst any) *apiError {
	if token == "" {
		return nil
	}

	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return errBadRequest("invalid cursor")
	}
	err = json.Unmarshal(data, dst)
	if err != nil {
		return errBadRequest("invalid cursor")
	}

	return nil
}

// pageParams reads the cursor and limit query parameters
func pageParams(r *http.Request, cursor any) (limit int, apiErr *apiError) {
	q := r.URL.Query()

	apiErr = decodeCursor(q.Get("cursor"), cursor)
	if apiErr != nil {
		return 0, apiErr
	}

	limitStr := q.Get("limit")
	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n < 1 {
			return 0, errBadRequest("limit must be a positive integer")
		}
		if n > components.MaxManagementListLimit {
			return 0, errBadRequest("limit must not exceed %d", components.MaxManagementListLimit)
		}
		limit = n
	}

	return components.EffectiveListLimit(limit), nil
}

// page is the envelope of every paginated response
type page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}

func newPage[T any](items []T, next string) page[T] {
	if items == nil {
		items = []T{}
	}
	return page[T]{Items: items, NextCursor: next}
}
