package internal

import (
	"slices"
)

// pageSelector keeps the smallest items offered to it, in the order cmp defines, so a listing can cut a page without sorting every match
// It holds at most size items in a max-heap, so selecting a page out of N matches costs O(N log size) time and O(size) memory
// Each page still scans every entry of the map it lists, since the maps have no order of their own
type pageSelector[T any] struct {
	size int
	cmp  func(a, b T) int
	// heap is a max-heap by cmp, so its root is the largest item kept
	heap []T
}

// newPageSelector returns a selector that keeps the size smallest items
func newPageSelector[T any](size int, cmp func(a, b T) int) *pageSelector[T] {
	return &pageSelector[T]{
		size: size,
		cmp:  cmp,
		heap: make([]T, 0, max(size, 0)),
	}
}

// Add offers an item, which the selector keeps while it is among the size smallest ones offered
func (s *pageSelector[T]) Add(v T) {
	if s.size <= 0 {
		return
	}

	// Keep every item until the heap is full
	if len(s.heap) < s.size {
		s.heap = append(s.heap, v)
		s.up(len(s.heap) - 1)
		return
	}

	// Then an item is only kept when it sorts before the largest one kept, which it replaces
	if s.cmp(v, s.heap[0]) < 0 {
		s.heap[0] = v
		s.down(0)
	}
}

// Sorted returns the kept items in ascending order
// The selector must not be used afterwards
func (s *pageSelector[T]) Sorted() []T {
	slices.SortFunc(s.heap, s.cmp)
	return s.heap
}

// up moves the item at i towards the root until its parent is not smaller
func (s *pageSelector[T]) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if s.cmp(s.heap[i], s.heap[parent]) <= 0 {
			return
		}
		s.heap[i], s.heap[parent] = s.heap[parent], s.heap[i]
		i = parent
	}
}

// down moves the item at i away from the root until neither child is larger
func (s *pageSelector[T]) down(i int) {
	n := len(s.heap)
	for {
		largest := i
		left := 2*i + 1
		right := left + 1
		if left < n && s.cmp(s.heap[left], s.heap[largest]) > 0 {
			largest = left
		}
		if right < n && s.cmp(s.heap[right], s.heap[largest]) > 0 {
			largest = right
		}
		if largest == i {
			return
		}
		s.heap[i], s.heap[largest] = s.heap[largest], s.heap[i]
		i = largest
	}
}
