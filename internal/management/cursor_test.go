package management

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Run("string cursor", func(t *testing.T) {
		in := placementsCursor{Type: "my/type?", ID: "id with spaces & ünicode"}
		token := encodeCursor(in)
		assert.NotContains(t, token, "=")
		assert.NotContains(t, token, "+")
		assert.NotContains(t, token, "/")

		var out placementsCursor
		apiErr := decodeCursor(token, &out)
		require.Nil(t, apiErr)
		assert.Equal(t, in, out)
	})

	t.Run("numeric cursor", func(t *testing.T) {
		in := eventsCursor{After: 1<<62 + 7}
		var out eventsCursor
		apiErr := decodeCursor(encodeCursor(in), &out)
		require.Nil(t, apiErr)
		assert.Equal(t, in, out)
	})

	t.Run("empty token leaves the destination unchanged", func(t *testing.T) {
		out := hostsCursor{After: "keep"}
		apiErr := decodeCursor("", &out)
		require.Nil(t, apiErr)
		assert.Equal(t, "keep", out.After)
	})
}

func TestDecodeCursorInvalid(t *testing.T) {
	tests := map[string]string{
		"not base64":          "!!!not-base64!!!",
		"padded base64":       base64.URLEncoding.EncodeToString([]byte(`{"a":"xy"}`)),
		"base64 but not JSON": base64.RawURLEncoding.EncodeToString([]byte("not json")),
		"wrong JSON type":     base64.RawURLEncoding.EncodeToString([]byte(`{"a":5}`)),
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			var out hostsCursor
			apiErr := decodeCursor(token, &out)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.status)
			assert.Equal(t, CodeBadRequest, apiErr.Code)
			assert.Equal(t, "invalid cursor", apiErr.Message)
		})
	}
}

func TestPageParams(t *testing.T) {
	req := func(q url.Values) *http.Request {
		return httptest.NewRequest(http.MethodGet, "/x?"+q.Encode(), nil)
	}

	t.Run("defaults", func(t *testing.T) {
		var c hostsCursor
		limit, apiErr := pageParams(req(nil), &c)
		require.Nil(t, apiErr)
		assert.Equal(t, components.DefaultManagementListLimit, limit)
		assert.Empty(t, c.After)
	})

	t.Run("limit and cursor", func(t *testing.T) {
		var c hostsCursor
		limit, apiErr := pageParams(req(url.Values{"limit": {"7"}, "cursor": {encodeCursor(hostsCursor{After: "h3"})}}), &c)
		require.Nil(t, apiErr)
		assert.Equal(t, 7, limit)
		assert.Equal(t, "h3", c.After)
	})

	t.Run("limit bounds", func(t *testing.T) {
		for _, n := range []int{1, components.MaxManagementListLimit} {
			var c hostsCursor
			limit, apiErr := pageParams(req(url.Values{"limit": {strconv.Itoa(n)}}), &c)
			require.Nil(t, apiErr)
			assert.Equal(t, n, limit)
		}
	})

	invalid := map[string]string{
		"zero":         "0",
		"negative":     "-1",
		"not a number": "ten",
		"float":        "1.5",
		"over the max": strconv.Itoa(components.MaxManagementListLimit + 1),
	}
	for name, v := range invalid {
		t.Run("invalid limit "+name, func(t *testing.T) {
			var c hostsCursor
			_, apiErr := pageParams(req(url.Values{"limit": {v}}), &c)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.status)
		})
	}

	t.Run("invalid cursor", func(t *testing.T) {
		var c hostsCursor
		_, apiErr := pageParams(req(url.Values{"cursor": {"%%%"}}), &c)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.status)
		assert.Equal(t, "invalid cursor", apiErr.Message)
	})
}

func TestNewPage(t *testing.T) {
	p := newPage[int](nil, "")
	assert.NotNil(t, p.Items)
	assert.Empty(t, p.Items)
}
