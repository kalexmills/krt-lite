package krtlite

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"log/slog"
	"sync/atomic"
	"time"
)

type (
	// A Mapper maps its input to zero or one output values.
	Mapper[I, O any] func(ktx Context, i I) *O

	// A FlatMapper maps its input to zero or more output values.
	FlatMapper[I, O any] func(ktx Context, i I) []O

	// A KeyExtractor is used to index objects by additional keys. See [IndexableCollection.Index] for details.
	KeyExtractor[T any] func(t T) []string
)

// EventStream describes a source of state changes for items of type T. Components can register to an event stream to
// subscribe to updates.
//
// An EventStream is synced when it has received an EventAdd notification for every item.
type EventStream[T any] interface {

	// Register subscribes to this EventStream, ensuring the handler is called exactly once for every event.
	// Upon registration, the event stream snapshots current state and generates an add event for every item in the
	// collection. These additional events ensure the caller is synchronized with the initial state. The returned
	// Registration reports when all initial add events have been sent.
	//
	// The following semantics govern handlers:
	//  * Update events which result in identical objects are suppressed by default (see [WithSpuriousUpdates]).
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
	//
	// Handlers follow the same semantics as Register.
	RegisterBatched(handler func(o []Event[T]), runExistingState bool) Registration

	// HasSynced returns true if this EventStream has received all its initial state from upstream.
	HasSynced() bool

	// WaitUntilSynced blocks until this EventStream has received all initial state from upstream. Stops blocking and
	// returns false if the passed channel is closed. Returns true if and only if sync was successful.
	WaitUntilSynced(stop <-chan struct{}) bool
}

// An IndexableCollection is a Collection which supports building additional Indexes on its contents.
type IndexableCollection[T any] interface {
	Collection[T]

	// Index indexes items in this collection by keys produced by the provided KeyExtractor.
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
	// Unregister unregisters this registration handler and cleans up any resources.
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

	getStopCh() <-chan struct{}
}

// Singleton is a Collection containing a single value which can change over time.
type Singleton[T any] interface {
	Collection[T]

	// Get returns the current value.
	Get() *T
	// Set replaces the current value with its argument.
	Set(*T)
	// MarkSynced marks this collection as synced. Multiple calls to this method have no effect.
	MarkSynced()
}

// A CollectionOption specifies optional ways collections may behave.
type CollectionOption func(m *collectionShared)

// WithName provides a name to the collection for use in debugging and logging.
func WithName(name string) CollectionOption {
	return func(m *collectionShared) {
		m.name = name
	}
}

// WithStop ensures when the provided channel is closed, the collection is stopped.
func WithStop(stop <-chan struct{}) CollectionOption {
	return func(m *collectionShared) {
		m.stop = stop
	}
}

// WithContext ensures that when the provided context is cancelled, the collection is stopped.
func WithContext(ctx context.Context) CollectionOption {
	return WithStop(ctx.Done())
}

// WithPollInterval configures the poll interval used by Informers. Has no effect for other collections.
func WithPollInterval(interval time.Duration) CollectionOption {
	return func(m *collectionShared) {
		m.pollInterval = &interval
	}
}

// WithSpuriousUpdates configures collections from Map and FlatMap to send update events downstream even if old and new
// objects are identical. Has no effect for other collections.
func WithSpuriousUpdates() CollectionOption {
	return func(m *collectionShared) {
		m.wantSpuriousUpdates = true
	}
}

// collectionShared contains metadata and fields common to controllers.
type collectionShared struct {
	uid                 uint64
	name                string
	stop                <-chan struct{}
	pollInterval        *time.Duration
	wantSpuriousUpdates bool
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

// An Event describes a mutation to a collection.
type Event[T any] struct {
	// Old denotes the old value of the item. Will be nil when Type == EventAdd.
	Old *T
	// New denotes the new value of the item. Will be nil when Type == EventDelete
	New *T
	// Type denotes the type of mutation which occurred.
	Type EventType
}

// String returns a string representation of this event.
func (e Event[T]) String() string {
	return fmt.Sprintf("%v [old = %v, new = %v]", e.Type, e.Old, e.New)
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

// A Keyer is an object which can be identified by a unique key. It should be implemented by custom types which are
// placed in Collections. All items in a collection must have a unique key.
type Keyer interface {
	Key() string
}

// GetKey infers a string key for its argument. Will panic if its argument is not one of the following types.
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

func getTypedKey[O any](o O) key[O] {
	return key[O](GetKey(o))
}
