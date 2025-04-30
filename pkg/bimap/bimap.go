package bimap

import (
	"iter"
	"maps"
)

type BiMap[U, V comparable] struct {
	uv map[U]map[V]struct{}
	vu map[V]map[U]struct{}
}

func New[U, V comparable]() *BiMap[U, V] {
	return &BiMap[U, V]{
		uv: make(map[U]map[V]struct{}, 16),
		vu: make(map[V]map[U]struct{}, 16),
	}
}

func (b *BiMap[U, V]) IsEmpty() bool {
	return len(b.uv) == 0 && len(b.vu) == 0
}

// Add associates u with v and v with u.
func (b *BiMap[U, V]) Add(u U, v V) {
	if _, ok := b.uv[u]; !ok {
		b.uv[u] = map[V]struct{}{v: {}}
	} else {
		b.uv[u][v] = struct{}{}
	}

	if _, ok := b.vu[v]; !ok {
		b.vu[v] = map[U]struct{}{u: {}}
	} else {
		b.vu[v][u] = struct{}{}
	}
}

// RemoveU removes u from the bidirectional mapping.
func (b *BiMap[U, V]) RemoveU(u U) {
	for v := range b.uv[u] {
		delete(b.vu[v], u)
		if len(b.vu[v]) == 0 {
			delete(b.vu, v)
		}
	}
	delete(b.uv, u)
}

// RemoveV removes v from the bidirectional mapping.
func (b *BiMap[U, V]) RemoveV(v V) {
	for u := range b.vu[v] {
		delete(b.uv[u], v)
		if len(b.uv[u]) == 0 {
			delete(b.uv, u)
		}
	}
	delete(b.vu, v)
}

// GetVs returns an iterator over all Vs mapped by the provided U.
func (b *BiMap[U, V]) GetVs(u U) iter.Seq[V] {
	return maps.Keys(b.uv[u])
}

// GetUs returns an iterator over all Us mapped by the provided V.
func (b *BiMap[U, V]) GetUs(v V) iter.Seq[U] {
	return maps.Keys(b.vu[v])
}
