package bimap

import (
	"iter"
	"maps"
)

// A BiMap tracks a bidirectional mapping between two sets, referred to as "Left" and "Right".
type BiMap[L, R comparable] struct {
	leftToRight map[L]map[R]struct{}
	rightToLeft map[R]map[L]struct{}
}

// New constructs a new empty BiMap.
func New[L, R comparable]() *BiMap[L, R] {
	return &BiMap[L, R]{
		leftToRight: make(map[L]map[R]struct{}, 16),
		rightToLeft: make(map[R]map[L]struct{}, 16),
	}
}

// IsEmpty returns true if no items are contained in this BiMap.
func (b *BiMap[L, R]) IsEmpty() bool {
	return len(b.leftToRight) == 0 && len(b.rightToLeft) == 0
}

// Add associates l with r and r with l.
func (b *BiMap[L, R]) Add(l L, r R) {
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
}

// RemoveLeft removes its argument from the "Left" set.
func (b *BiMap[L, R]) RemoveLeft(l L) {
	for r := range b.leftToRight[l] {
		delete(b.rightToLeft[r], l)
		if len(b.rightToLeft[r]) == 0 {
			delete(b.rightToLeft, r)
		}
	}
	delete(b.leftToRight, l)
}

// RemoveRight removes its argument from the "Right" set.
func (b *BiMap[L, R]) RemoveRight(r R) {
	for l := range b.rightToLeft[r] {
		delete(b.leftToRight[l], r)
		if len(b.leftToRight[l]) == 0 {
			delete(b.leftToRight, l)
		}
	}

	delete(b.rightToLeft, r)
}

// GetVs returns an iterator over all items in the Right set which are mapped to by its argument.
func (b *BiMap[L, R]) GetVs(l L) iter.Seq[R] {
	return maps.Keys(b.leftToRight[l])
}

// GetUs returns an iterator over all items in the Left set which are mapped to by its argument.
func (b *BiMap[L, R]) GetUs(r R) iter.Seq[L] {
	return maps.Keys(b.rightToLeft[r])
}
