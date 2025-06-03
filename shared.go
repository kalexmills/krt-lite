package krtlite

import (
	"log/slog"
	"reflect"
	"sync/atomic"
	"time"
)

func getTypedKey[O any](o O) key[O] {
	return key[O](GetKey(o))
}

// collectionShared contains metadata and fields common to controllers.
type collectionShared struct {
	uid                 uint64
	name                string
	stop                <-chan struct{}
	pollInterval        *time.Duration
	wantSpuriousUpdates bool
	filter              *InformerFilter
}

func (c collectionShared) getStopCh() <-chan struct{} { //nolint: unused // implementing interface
	return c.stop
}

func (c collectionShared) getName() string { //nolint:unused // implementing interface
	return c.name
}

func (c collectionShared) getUID() uint64 { //nolint:unused // implementing interface
	return c.uid
}

func (c collectionShared) logger() *slog.Logger {
	return slog.With("uid", c.uid, "collectionName", c.name)
}

func newCollectionShared(options []CollectionOption) collectionShared {
	meta := &collectionShared{
		uid:  nextCollectionUID(),
		stop: make(chan struct{}),
	}
	for _, option := range options {
		option(meta)
	}
	return *meta
}

type key[O any] string

var globalCollectionUIDCounter = atomic.Uint64{}

func nextCollectionUID() uint64 {
	return globalCollectionUIDCounter.Add(1)
}

// equal compares two objects for equality. Uses Equaler if either a or b implement it, uses reflection otherwise.
func equal(a, b any) bool {
	if A, ok := a.(Equaler); ok {
		return A.Equal(b)
	}
	if B, ok := b.(Equaler); ok {
		return B.Equal(a)
	}
	return reflect.DeepEqual(a, b)
}
