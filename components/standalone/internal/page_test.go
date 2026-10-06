package internal

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageSelector(t *testing.T) {
	t.Run("keeps the smallest items in order", func(t *testing.T) {
		// Compare with sorting every item, over sizes below, at, and above the number of items, with duplicates
		// #nosec G404 -- a seeded generator keeps the test reproducible, and nothing here needs unpredictable values
		rnd := rand.New(rand.NewPCG(1, 2))
		for _, count := range []int{0, 1, 2, 7, 100, 1000} {
			items := make([]int, count)
			for i := range items {
				items[i] = rnd.IntN(count/2 + 1)
			}
			sorted := slices.Clone(items)
			slices.Sort(sorted)

			for _, size := range []int{1, 2, 10, count, count + 5} {
				sel := newPageSelector(size, cmp.Compare[int])
				for _, v := range items {
					sel.Add(v)
				}
				assert.Equalf(t, sorted[:min(size, count)], sel.Sorted(), "%d items, size %d", count, size)
			}
		}
	})

	t.Run("a size that is not positive keeps nothing", func(t *testing.T) {
		sel := newPageSelector(0, cmp.Compare[int])
		sel.Add(1)
		assert.Empty(t, sel.Sorted())
	})
}
