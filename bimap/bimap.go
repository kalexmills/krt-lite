package bimap

import (
	"iter"
	"maps"
)

// BiMultimap tracks a bidirectional one-to-many mapping between two sets, referred to as "Left" and "Right".
// BiMultimap is specialized for tracking dependencies between keys introduced by the use of Fetch. This BiMultimap can
// grow as large as O(mn).
//
// Not thread-safe.
type BiMultimap[L, R comparable] struct {
	leftToRight map[L]map[R]struct{}
	rightToLeft map[R]map[L]struct{}

	// totalCount tracks the number of associations in the multimap.
	totalCount int
}

// New constructs a new empty BiMultimap.
func New[L, R comparable]() *BiMultimap[L, R] {
	return &BiMultimap[L, R]{
		leftToRight: make(map[L]map[R]struct{}, 16),
		rightToLeft: make(map[R]map[L]struct{}, 16),
	}
}

// IsEmpty returns true if no items are contained in this BiMultimap.
func (b *BiMultimap[L, R]) IsEmpty() bool {
	return len(b.leftToRight) == 0 && len(b.rightToLeft) == 0
}

// Add associates l with r and r with l.
func (b *BiMultimap[L, R]) Add(l L, r R) {
	if _, ok := b.leftToRight[l]; !ok {
		b.leftToRight[l] = map[R]struct{}{r: {}}
	} else {
		b.leftToRight[l][r] = struct{}{}
	}

	if _, ok := b.rightToLeft[r]; !ok {
		b.rightToLeft[r] = map[L]struct{}{l: {}}
	} else {
		b.rightToLeft[r][l] = struct{}{}
	}

	b.totalCount++
}

func (b *BiMultimap[L, R]) Size() int {
	return b.totalCount
}

// RemoveLeft removes its argument from the "Left" set.
func (b *BiMultimap[L, R]) RemoveLeft(l L) {
	b.totalCount -= len(b.leftToRight[l])

	for r := range b.leftToRight[l] {
		delete(b.rightToLeft[r], l)
		if len(b.rightToLeft[r]) == 0 {
			delete(b.rightToLeft, r)
		}
	}

	delete(b.leftToRight, l)
}

// RemoveRight removes its argument from the "Right" set.
func (b *BiMultimap[L, R]) RemoveRight(r R) {
	b.totalCount -= len(b.rightToLeft[r])

	for l := range b.rightToLeft[r] {
		delete(b.leftToRight[l], r)
		if len(b.leftToRight[l]) == 0 {
			delete(b.leftToRight, l)
		}
	}

	delete(b.rightToLeft, r)
}

// GetRight returns an iterator over all items in the Right set which are mapped to by its argument.
func (b *BiMultimap[L, R]) GetRight(l L) iter.Seq[R] {
	return maps.Keys(b.leftToRight[l])
}

// GetLeft returns an iterator over all items in the Left set which are mapped to by its argument.
func (b *BiMultimap[L, R]) GetLeft(r R) iter.Seq[L] {
	return maps.Keys(b.rightToLeft[r])
}
