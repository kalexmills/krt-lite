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
	// A Mapper maps its input to zero or one output values.
	Mapper[I, O any] func(ktx Context, i I) *O

	// A FlatMapper maps its input to zero or more output values.
	FlatMapper[I, O any] func(ktx Context, i I) []O

	// A KeyExtractor is used to extract mapIndex keys from an object.
	KeyExtractor[T any] func(t T) []string
)

// EventStream describes a source of state changes for items of type T. Components can register to an event stream to
// subscribe to updates.
//
// An EventStream is synced when it has received an EventAdd notification for every item
type EventStream[T any] interface {

	// Register subscribes to this EventStream, ensuring the handler is called exactly once for every event. The caller
	// will receive an add Events for each initial state.
	//
	// Upon registration, the event stream snapshots current state and generates an add event for every known item.
	// These additional events ensure the caller is synchronized with the initial state. The returned Syncer reports
	// when all initial add events have been sent.
	//
	// The following semantics govern handlers:
	//  * Update events which result in identical objects are suppressed.
	//  * On each event, all handlers are called.
	//  * Each handler has its own unbounded event queue. Slow handlers may cause items to accumulate but will not
	//    block other handlers.
	//  * Events are delivered exactly once, in order.
	Register(handler func(o Event[T])) Registration

	// RegisterBatched subscribes to this EventStream, the provided handler is called with batches of events. Events
	// in successive batches are delivered in the order they occur. Events within a batch may or may not be in the
	// order they occur, depending on the implementation.
	//
	// When runExistingState is true, the event stream snapshots current state and sends an add Event for every known
	// item. The returned Registration reports when all initial add events have been sent.
	RegisterBatched(handler func(o []Event[T]), runExistingState bool) Registration

	// WaitUntilSynced blocks until this EventStream has received all initial state from upstream. If the provided
	// channel is closed, this func returns false immediately. Returns true if and only if sync was successful.
	WaitUntilSynced(stop <-chan struct{}) bool

	// HasSynced returns true if this EventStream has received all its initial state from upstream.
	HasSynced() bool
}

// An IndexableCollection is a Collection which supports building additional Indexes on its contents.
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

type Registration interface {
	Syncer
	// Unregister unregisters this registration handler and cleans up any resources. Calling Unregister more than once
	// will panic.
	Unregister()
}

// Collection is a collection of objects whose changes can be subscribed to. Each item in a Collection is associated
// with a unique string key via GetKey. Items in a collection can be listed, or fetched individually by their key.
type Collection[T any] interface {
	EventStream[T]

	// GetKey fetches an item from this collection by its key.
	GetKey(key string) *T

	// List lists all items currently in this collection.
	List() []T

	getName() string

	getUID() uint64

	logger() *slog.Logger
}

// Singleton is a Collection containing a single value which can change over time.
type Singleton[T any] interface {
	Collection[T]

	// Get returns the current value.
	Get() *T
	// Set replaces the current value with its argument.
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
		uid:  nextCollectionUID(),
		stop: make(chan struct{}),
	}
	for _, option := range options {
		option(meta)
	}
	return *meta
}

// A CollectionOption specifies optional ways collections may behave.
type CollectionOption func(m *collectionShared)

// A FetchOption modifies how calls to Fetch work.
type FetchOption func(m *dependency)

// EventType describes an event which mutates a collection.
type EventType int

const (
	// EventAdd denotes an item was added to the collection.
	EventAdd EventType = iota
	// EventUpdate denotes an item in the collection was changed.
	EventUpdate
	// EventDelete denotes an item in the collection was removed.
	EventDelete
)

// String returns a string representation of the event.
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

// Event describes a mutation to a collection.
type Event[T any] struct {
	// Old denotes the old value of the item. Will be nil when Event == EventAdd.
	Old *T
	// New denotes the new value of the item. Will be nil when Event == EventDelete
	New *T
	// Event denotes the type of mutation which occurred.
	Event EventType
}

// String returns a string representation of this event.
func (e Event[T]) String() string {
	return fmt.Sprintf("%v [old = %v, new = %v]", e.Event, e.Old, e.New)
}

// Latest returns the most recent value of the item.
func (e Event[T]) Latest() T {
	if e.New == nil {
		return *e.Old
	}
	return *e.New
}

// Items returns a list of all non-nil items in this event.
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

type key[O any] string

var globalCollectionUIDCounter = atomic.Uint64{}

func nextCollectionUID() uint64 {
	return globalCollectionUIDCounter.Add(1)
}

var globalDependencyUIDCounter = atomic.Uint64{}

func nextDependencyUID() uint64 {
	return globalDependencyUIDCounter.Add(1)
}

// Keyer is implemented by any type which can have a key.
type Keyer interface {
	Key() string
}

// GetKey infers a string key for the passed in value. Panics if its argument is not one of the following types.
//   - runtime.Object
//   - string
//   - Keyer
func GetKey[O any](o O) string {
	switch typed := any(o).(type) {
	case string:
		return typed
	case runtime.Object:
		str, _ := cache.MetaNamespaceKeyFunc(typed)
		return str
	case Keyer:
		return typed.Key()
	}
	panic(fmt.Sprintf("Cannot get key, got %T", o))
}

// Labeler is implemented by any type which can have labels.
type Labeler interface {
	GetLabels() map[string]string
}

func getLabels(obj any) map[string]string {
	switch typed := obj.(type) {
	case Labeler:
		return typed.GetLabels()
	default:
		panic(fmt.Sprintf("Cannot get labels, got %T", obj))
	}
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
