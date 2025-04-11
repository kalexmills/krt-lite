package pkg

import (
	"errors"
	"fmt"
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"iter"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
)

type (
	// Mapper maps an I to an O.
	Mapper[I, O any] func(i I) *O

	// FlatMapper maps an I to many O.
	FlatMapper[I, O any] func(i I) []O

	// Splitter maps an I into an O1 and an O2.
	Splitter[I, O1, O2 any] func(i I) (*O1, *O2)

	// FlatSplitter maps an I into many O1 and many O2.
	FlatSplitter[I, O1, O2 any] func(i I) ([]O1, []O2)

	// Joiner joins two T into zero or one T.
	Joiner[T any] func(t1, t2 T) *T
)

// Collection is a collection of objects.
type Collection[T any] interface {
	GetKey(k string) *T

	List() []T

	Register(f func(o Event[T])) cache.ResourceEventHandlerRegistration

	RegisterBatched(f func(o []Event[T]), runExistingState bool) cache.ResourceEventHandlerRegistration

	WaitUntilSynced(stop <-chan struct{}) bool

	HasSynced() bool
}

type EventType int

const (
	EventAdd EventType = iota
	EventUpdate
	EventDelete
	EventSynced // TODO: never sent
)

type Event[T any] struct {
	Old   *T
	New   *T
	Event EventType
}

func (e Event[T]) Latest() T {
	if e.Old == nil {
		return *e.New
	}
	return *e.Old
}

func eventKey[T any](in any) (string, error) {
	ev, ok := in.(Event[T])
	if !ok {
		return "", errors.New("invalid event")
	}
	return fmt.Sprintf("%p-%p-%d", ev.Old, ev.New, ev.Event), nil
}

type HandlerContext interface {
	DiscardResult()
}

type Key[O any] string

// Map creates a new collection by mapping each I into an O.
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

// FlatMap creates a new collection by mapping every I into zero or more O.
func FlatMap[I, O any](c Collection[I], f FlatMapper[I, O]) Collection[O] {
	result := &derivedCollection[I, O]{
		transformer: f,
		uid:         nextUID(),
		parent:      c,

		outputs:  make(map[Key[O]]O),
		inputs:   make(map[Key[I]]I),
		mappings: make(map[Key[I]]map[Key[O]]struct{}),
		mut:      &sync.Mutex{},

		registeredHandlers: make(map[*processor[Event[O]]]struct{}),
		processorWg:        &sync.WaitGroup{},

		stop:  make(chan struct{}),
		queue: fifo.NewQueue[[]Event[I]](1024),
	}

	go result.run()

	return result
}

// Split splits a collection into two, mapping each I into zero or one O1 and O2
func Split[I, O1, O2 any](c Collection[I], spl Splitter[I, O1, O2]) (Collection[O1], Collection[O2]) {
	return nil, nil
}

// FlatSplit splits a collection into two, mapping each I to many O1 and many O2.
func FlatSplit[I, O1, O2 any](c Collection[I], spl FlatSplitter[I, O1, O2]) (Collection[O1], Collection[O2]) {
	return nil, nil
}

// Join joins together a slice of collections. If keys overlap, they are joined using the provided Joiner.
func Join[T any](cs []Collection[T], j Joiner[T]) Collection[T] {
	return nil
}

// JoinDisjoint joins together a slice of collections whose keys do not overlap.
func JoinDisjoint[T any](cs []Collection[T]) Collection[T] {
	return nil
}

var globalUIDCounter = atomic.Uint64{}

func nextUID() uint64 {
	return globalUIDCounter.Add(1)
}

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
	panic(fmt.Sprintf("Cannot get Key, got %T", o))
}

func GetTypedKey[O any](o O) Key[O] {
	return Key[O](GetKey(o))
}

type ResourceNamer interface {
	ResourceName() string
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

type chanSynced struct {
	ch <-chan struct{}
}

func (s chanSynced) HasSynced() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}
