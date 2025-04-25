package pkg

import (
	"fmt"
	"iter"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"log/slog"
	"sync/atomic"
)

type (
	// A Mapper maps an I to zero or one O.
	Mapper[I, O any] func(ctx Context, i I) *O

	// A FlatMapper maps an I to many O.
	FlatMapper[I, O any] func(ctx Context, i I) []O

	// A Merger merges two or more O into one. Must be a pure function.
	Merger[T any] func(ts []T) T

	// A KeyExtractor is used to extract mapIndex keys from an object.
	KeyExtractor[T any] func(t T) []string
)

type EventStream[T any] interface {
	Register(f func(o Event[T])) Syncer

	RegisterBatched(f func(o []Event[T]), runExistingState bool) Syncer

	WaitUntilSynced(stop <-chan struct{}) bool

	HasSynced() bool
}

type IndexableCollection[T any] interface {
	Collection[T]

	// Index returns an index built using the provided KeyExtractor.
	Index(extractor KeyExtractor[T]) Index[T]
}

// ComparableObject is used internally to track dependencies among runtime.Object.
type ComparableObject interface {
	// This interface is implemented by pointer-types that also implement runtime.Object. For example *corev1.Pod
	// implements ComparableObject, while not corev1.Pod does not.

	runtime.Object
	comparable
}

// Collection is a collection of objects whose changes can be subscribed to.
type Collection[T any] interface {
	EventStream[T]

	GetKey(key string) *T

	List() []T

	getName() string

	getUID() uint64

	logger() *slog.Logger
}

// Singleton is a Collection containing a single value which can change over time.
type Singleton[T any] interface {
	Collection[T]
	Get() *T
	Set(*T)
}

// collectionShared contains metadata and fields common to controllers.
type collectionShared struct {
	uid  uint64
	name string
	stop <-chan struct{}
}

//nolint:unused // TODO: remove if still unused
func (c collectionShared) getName() string {
	return c.name
}

//nolint:unused // TODO: remove if still unused
func (c collectionShared) getUID() uint64 {
	return c.uid
}

func (c collectionShared) logger() *slog.Logger {
	return slog.With("uid", c.uid, "collectionName", c.name)
}

func newCollectionShared(options []CollectionOption) collectionShared {
	meta := &collectionShared{
		uid:  nextUID(),
		stop: make(chan struct{}),
	}
	for _, option := range options {
		option(meta)
	}
	return *meta
}

type CollectionOption func(m *collectionShared)

// An Index allows subsets of items in a collection to be indexed.
type Index[T any] interface {
	Lookup(key string) []T
}

type EventType int

const (
	EventAdd EventType = iota
	EventUpdate
	EventDelete
)

func (e EventType) String() string {
	switch e {
	case EventAdd:
		return "add"
	case EventUpdate:
		return "update"
	case EventDelete:
		return "delete"
	}
	return "unknown"
}

type Event[T any] struct {
	Old   *T
	New   *T
	Event EventType
}

func (e Event[T]) String() string {
	return fmt.Sprintf("%v [old = %v, new = %v]", e.Event, e.Old, e.New)
}

func (e Event[T]) Latest() T {
	if e.New == nil {
		return *e.Old
	}
	return *e.New
}

func (e Event[T]) Items() []T {
	res := make([]T, 0, 2)
	if e.Old != nil {
		res = append(res, *e.Old)
	}
	if e.New != nil {
		res = append(res, *e.New)
	}
	return res
}

type HandlerContext interface { // TODO: will we use this?
	DiscardResult()
}

type key[O any] string

var globalUIDCounter = atomic.Uint64{}

func nextUID() uint64 {
	return globalUIDCounter.Add(1)
}

// GetKey infers a string key for the passed in value. Will panic if any type other than the following are passed.
// - runtime.Object
// - string
// - ResourceNamer
func GetKey[O any](o O) string {
	switch typed := any(o).(type) {
	case string:
		return typed
	case runtime.Object:
		str, _ := cache.MetaNamespaceKeyFunc(typed)
		return str
	case ResourceNamer:
		return typed.ResourceName()
	}
	panic(fmt.Sprintf("Cannot get key, got %T", o))
}

// ResourceNamer can be implemented
type ResourceNamer interface {
	ResourceName() string
}

func getTypedKey[O any](o O) key[O] {
	return key[O](GetKey(o))
}

// setFromSeq forms a set from an iter.Seq.
func setFromSeq[T comparable](seq iter.Seq[T]) map[T]struct{} {
	result := make(map[T]struct{})
	seq(func(t T) bool {
		result[t] = struct{}{}
		return true
	})
	return result
}

func castEvent[I, O any](o Event[I]) Event[O] {
	e := Event[O]{
		Event: o.Event,
	}
	if o.Old != nil {
		e.Old = ptr.To(any(*o.Old).(O))
	}
	if o.New != nil {
		e.New = ptr.To(any(*o.New).(O))
	}
	return e
}
