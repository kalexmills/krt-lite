package pkg

import (
	"fmt"
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"iter"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
)

type (
	// A Mapper maps an I to zero or one O.
	Mapper[I, O any] func(i I) *O

	// A FlatMapper maps an I to many O.
	FlatMapper[I, O any] func(i I) []O

	// A FlatSplitter maps an I into many O1 and many O2.
	FlatSplitter[I, O1, O2 any] func(i I) ([]O1, []O2)

	// A Joiner joins two or more O into one.
	Joiner[T any] func(ts []T) T
)

// Collection is a collection of objects that can change over time, and can be subscribed to.
type Collection[T any] interface {
	GetKey(k string) *T

	List() []T

	Register(f func(o Event[T])) cache.ResourceEventHandlerRegistration

	RegisterBatched(f func(o []Event[T]), runExistingState bool) cache.ResourceEventHandlerRegistration

	WaitUntilSynced(stop <-chan struct{}) bool

	HasSynced() bool
}

// Singleton is a Collection containing a single value which can change over time.
type Singleton[T any] interface {
	Collection[T]
	Get() *T
	Set(*T)
}

// NewSingleton creates an returns a new Singleton.
func NewSingleton[T any](initial *T, startSynced bool) Singleton[T] {
	result := newStatic[T]()
	result.Set(initial)
	if startSynced {
		result.MarkSynced()
	}
	return result
}

type EventType int

const (
	EventAdd EventType = iota
	EventUpdate
	EventDelete
	EventSynced // TODO: never sent or used
)

func (e EventType) String() string {
	switch e {
	case EventAdd:
		return "add"
	case EventUpdate:
		return "update"
	case EventDelete:
		return "delete"
	case EventSynced:
		return "synced"
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

type HandlerContext interface { // TODO: will we use this?
	DiscardResult()
}

type key[O any] string

// Map creates a new collection by mapping each I into an O. It starts a new goroutine.
//
// Will panic if unsupported type for I or O are used, see GetKey for details.
func Map[I, O any](c Collection[I], f Mapper[I, O]) Collection[O] {
	ff := func(i I) []O {
		res := f(i)
		if res == nil {
			return nil
		}
		return []O{*res}
	}
	return FlatMap(c, ff)
}

// FlatMap creates a new collection by mapping every I into zero or more O. It starts a new goroutine.
//
// Will panic if an unsupported type of I or O are used, see GetKey for details.
func FlatMap[I, O any](c Collection[I], f FlatMapper[I, O]) Collection[O] {
	result := &derivedCollection[I, O]{
		transformer: f,
		uid:         nextUID(),
		parent:      c,

		outputs:  make(map[key[O]]O),
		inputs:   make(map[key[I]]I),
		mappings: make(map[key[I]]map[key[O]]struct{}),
		mut:      &sync.Mutex{},

		registeredHandlers: make(map[*registrationHandler[Event[O]]]struct{}),
		processorWg:        &sync.WaitGroup{},

		stop:  make(chan struct{}),
		queue: fifo.NewQueue[[]Event[I]](1024),
	}

	go result.run()

	return result
}

// Join joins together a slice of collections. If any keys overlap, all overlapping key are joined using the provided
// Joiner. Joiner will always be called with at least two inputs.
func Join[T any](cs []Collection[T], j Joiner[T]) Collection[T] {
	return newJoinedCollection(cs, j)
}

// JoinDisjoint joins together a slice of collections whose keys do not overlap.
func JoinDisjoint[T any](cs []Collection[T]) Collection[T] {
	return newJoinedCollection(cs, nil)
}

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

type alwaysSynced struct{}

func (s alwaysSynced) HasSynced() bool { return true }
