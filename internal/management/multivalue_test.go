package management

import (
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeValues serves fetchValue calls from fixed lists, where an item's position is its index in its value's list, and records each call
type fakeValues struct {
	lists map[string][]string
	calls []string
}

func (f *fakeValues) fetch(value string, after int, limit int) ([]string, int, bool, error) {
	f.calls = append(f.calls, value)
	list := f.lists[value]
	end := min(after+limit, len(list))
	return list[after:end], end, end < len(list), nil
}

func TestPageAcrossValues(t *testing.T) {
	values := []string{"a", "b", "c"}
	lists := map[string][]string{
		"a": {"a1", "a2"},
		"b": {"b1", "b2", "b3"},
		"c": {"c1"},
	}

	t.Run("continues in the next value when one runs out", func(t *testing.T) {
		f := &fakeValues{lists: lists}
		page, err := pageAcrossValues(values, 0, 0, 3, f.fetch)
		require.NoError(t, err)
		assert.Equal(t, []string{"a1", "a2", "b1"}, page.Items)
		assert.True(t, page.HasMore)
		assert.Equal(t, 1, page.Group)
		assert.Equal(t, 1, page.After)
	})

	t.Run("resumes in the value the cursor stopped in", func(t *testing.T) {
		f := &fakeValues{lists: lists}
		page, err := pageAcrossValues(values, 1, 1, 10, f.fetch)
		require.NoError(t, err)
		assert.Equal(t, []string{"b2", "b3", "c1"}, page.Items)
		assert.False(t, page.HasMore)
		assert.Equal(t, []string{"b", "c"}, f.calls)
	})

	t.Run("a page that fills up where a value runs out continues in the next value with items", func(t *testing.T) {
		f := &fakeValues{lists: map[string][]string{"a": {"a1", "a2"}, "b": {}, "c": {"c1"}}}
		page, err := pageAcrossValues(values, 0, 0, 2, f.fetch)
		require.NoError(t, err)
		assert.Equal(t, []string{"a1", "a2"}, page.Items)
		assert.True(t, page.HasMore)
		assert.Equal(t, 2, page.Group)
		assert.Equal(t, 0, page.After)
	})

	t.Run("a page that fills up where the last values are empty is the last page", func(t *testing.T) {
		f := &fakeValues{lists: map[string][]string{"a": {"a1", "a2"}, "b": {}, "c": {}}}
		page, err := pageAcrossValues(values, 0, 0, 2, f.fetch)
		require.NoError(t, err)
		assert.Equal(t, []string{"a1", "a2"}, page.Items)
		assert.False(t, page.HasMore)
	})

	t.Run("a single value lists as before", func(t *testing.T) {
		f := &fakeValues{lists: map[string][]string{"": {"x1", "x2", "x3"}}}
		page, err := pageAcrossValues([]string{""}, 0, 0, 2, f.fetch)
		require.NoError(t, err)
		assert.Equal(t, []string{"x1", "x2"}, page.Items)
		assert.True(t, page.HasMore)
		assert.Equal(t, 0, page.Group)
		assert.Equal(t, 2, page.After)
	})

	t.Run("returns the error of a fetch", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := pageAcrossValues(values, 0, 0, 2, func(string, int, int) ([]string, int, bool, error) {
			return nil, 0, false, boom
		})
		require.ErrorIs(t, err, boom)
	})
}

func TestQueryValues(t *testing.T) {
	order := inOrder([]string{"pending", "active", "dead"})

	t.Run("without the parameter lists everything", func(t *testing.T) {
		assert.Equal(t, []string{""}, queryValues(url.Values{}, "status", order))
		assert.Equal(t, []string{""}, queryValues(url.Values{"status": {""}}, "status", order))
	})

	t.Run("sorts and deduplicates repeated values", func(t *testing.T) {
		q := url.Values{"status": {"dead", "pending", "dead", ""}}
		assert.Equal(t, []string{"pending", "dead"}, queryValues(q, "status", order))
	})

	t.Run("sorts other values with the comparison given", func(t *testing.T) {
		q := url.Values{"type": {"user", "cart"}}
		assert.Equal(t, []string{"cart", "user"}, queryValues(q, "type", strings.Compare))
	})

	t.Run("the group of a cursor must point at a value", func(t *testing.T) {
		values := []string{"a", "b"}
		assert.True(t, validGroup(1, values))
		assert.False(t, validGroup(2, values))
		assert.False(t, validGroup(-1, values))
	})
}
