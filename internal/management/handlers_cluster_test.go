package management

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountPages(t *testing.T) {
	// pages returns a fetch function serving total items in pages of size, with the cursor being the number of items read so far
	pages := func(total int, size int) func(after int) (int, bool, int, error) {
		return func(after int) (int, bool, int, error) {
			n := min(size, total-after)
			return n, after+n < total, after + n, nil
		}
	}

	t.Run("counts every page", func(t *testing.T) {
		res, err := countPages(pages(2500, 1000))
		require.NoError(t, err)
		assert.Equal(t, boundedCountJSON{Count: 2500}, res)
	})

	t.Run("stops at the cap", func(t *testing.T) {
		res, err := countPages(pages(summaryCountCap*2, 1000))
		require.NoError(t, err)
		assert.Equal(t, boundedCountJSON{Count: summaryCountCap, Truncated: true}, res)
	})

	t.Run("a count that ends exactly at the cap is not truncated", func(t *testing.T) {
		res, err := countPages(pages(summaryCountCap, 1000))
		require.NoError(t, err)
		assert.Equal(t, boundedCountJSON{Count: summaryCountCap}, res)
	})

	t.Run("returns the error of a page", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := countPages(func(after int) (int, bool, int, error) {
			return 0, false, after, boom
		})
		require.ErrorIs(t, err, boom)
	})
}
