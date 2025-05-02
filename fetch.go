package krt_lite

import (
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"strings"
)

// Context is used to track dependencies between collections created via [FlatMap] and [Map], and collections accessed
// via [Fetch]. See [Fetch] for details.
type Context interface {
	// registerDependency registers the provided dependency and syncer with the parent collection. Returns the dependency
	// ID for the registered collection.
	registerDependency(d *dependency, s Syncer, register func(func([]Event[any])) Syncer) bool
	trackKeys(keys []string)
	resetTrackingForCollection(collID uint64)
}

// Fetch calls List on the provided Collection, and can be used to subscribe Collections created by [Map] or [FlatMap]
// to events from other Collections. Passing a non-nil [Context] to Fetch subscribes the collection to updates from
// Collection c. Updates to any item returned from fetch will trigger recalculation of the handler that called Fetch.
// When no Context is passed, Fetch is equivalent to c.List().
//
// Tracking dependencies among keys makes processing updates faster, but results in additional overhead. For large
// collections that routinely make Fetch calls that return many item, this may become prohibitive. Users can pass
// [WithMaxTrackCount] to limit the number of keys tracked. See documentation for [WithMaxTrackCount] for a discussion
// of trade-offs.
func Fetch[T any](ktx Context, c Collection[T], opts ...FetchOption) []T {
	if ktx == nil {
		return c.List()
	}

	d := &dependency{
		dependencyID:   nextDependencyUID(),
		collectionID:   c.getUID(),
		collectionName: c.getName(),
	}

	for _, opt := range opts {
		opt(d)
	}
	// we must register before we List() so as to not miss any events
	ktx.registerDependency(d, c, func(handler func([]Event[any])) Syncer {
		ff := func(ts []Event[T]) {
			// do type erasure to cast from T to any.
			anys := make([]Event[any], len(ts))
			for i, t := range ts {
				anys[i] = castEvent[T, any](t)
			}

			handler(anys)
		}

		return c.RegisterBatched(ff, false)
	})

	ts := c.List()
	var out []T
	for _, t := range ts {
		if d.Matches(t) {
			out = append(out, t)
		}
	}

	if d.maxTrackCount != nil {
		if uint(len(out)) <= *d.maxTrackCount {
			var keys []string
			for i := uint(0); i < min(*d.maxTrackCount, uint(len(out))); i++ {
				keys = append(keys, GetKey[any](out[i]))
			}
			ktx.trackKeys(keys)
		} else {
			// reset tracking to ensure updates are not missed due to an incomplete index
			ktx.resetTrackingForCollection(d.collectionID)
		}
	}

	return out
}

// dependency identifies a Fetch dependency between two collections.
type dependency struct {
	dependencyID   uint64
	collectionID   uint64
	collectionName string

	maxTrackCount *uint

	filterFunc              func(any) bool
	filterKeys              map[string]struct{}
	filterLabels            map[string]string
	filterSelectorExtractor SelectorExtractor
	filterSelectorLabels    map[string]string
	filterLabelSelector     labels.Selector
	filterNames             map[cache.ObjectName]struct{}
}

func (d *dependency) Matches(object any) bool {
	if d.filterKeys != nil {
		if _, ok := d.filterKeys[GetKey[any](object)]; !ok {
			return false
		}
	}

	if d.filterFunc != nil && !d.filterFunc(object) {
		return false
	}

	if d.filterNames != nil {
		if objName, err := cache.ObjectToName(object); err == nil {
			if _, ok := d.filterNames[objName]; !ok {
				return false
			}
		} else {
			panic("MatchNames used on a Collection whose contents are not k8s Objects")
		}
	}

	if d.filterLabels != nil {
		lblr, ok := object.(Labeler)
		if !ok {
			panic("MatchLabels used on a collection whose contents do not implement the Labeler interface")
		}
		lbls := lblr.GetLabels()
		if lbls == nil || !labels.SelectorFromSet(d.filterLabels).Matches(labels.Set(lbls)) {
			return false
		}
	}

	if d.filterSelectorExtractor != nil {
		selector := d.filterSelectorExtractor(object)
		if selector == nil {
			return false
		}

		if !selector.Matches(labels.Set(d.filterSelectorLabels)) {
			return false
		}
	}

	if d.filterLabelSelector != nil {
		lblr, ok := object.(Labeler)
		if !ok {
			panic("MatchLabelSelector used on a collection whose contents do not implement the Labeler interface")
		}
		if !d.filterLabelSelector.Matches(labels.Set(lblr.GetLabels())) {
			return false
		}
	}

	return true
}

// WithMaxTrackCount sets the maximum number of keys tracked via Fetch. Any fetch call which returns more keys than the
// configured will not be tracked. For keys without tracking information, a full scan of the collection is performed
// for each fetched item when updates occur.
func WithMaxTrackCount(maxKeys uint) FetchOption {
	return func(d *dependency) {
		d.maxTrackCount = ptr.To(maxKeys)
	}
}

// MatchKeys ensures [Fetch] only returns objects with matching keys. MatchKeys may be used multiple times in the same
// Fetch call.
func MatchKeys(k ...string) FetchOption {
	return func(d *dependency) {
		if d.filterKeys == nil {
			d.filterKeys = make(map[string]struct{}, len(k))
		}
		for _, k := range k {
			d.filterKeys[k] = struct{}{}
		}
	}
}

// MatchLabels ensures [Fetch] only returns Kubernetes objects with a matching set of labels.
//
// Panics will occur in case multiple MatchLabels options are passed to the same invocation of [Fetch].
func MatchLabels(labels map[string]string) FetchOption {
	return func(d *dependency) {
		if d.filterLabels != nil {
			panic("MatchLabels added twice to the same Fetch call.")
		}
		d.filterLabels = labels
	}
}

// MatchSelectsLabels ensures [Fetch] only returns objects that would select objects with the provided set of labels.
// Caller must provide an extractor which retrieves [labels.Selector] implementations from objects in the target
// collection. Returning nil from a SelectorExtractor ensures that object will never be returned from [Fetch].
//
// Panics will occur in case multiple MatchSelectsLabels options are passed to the same invocation of [Fetch].
func MatchSelectsLabels(labels map[string]string, extractor SelectorExtractor) FetchOption {
	return func(d *dependency) {
		if d.filterSelectorExtractor != nil {
			panic("MatchSelectedBy added twice to the same Fetch call.")
		}
		d.filterSelectorLabels = labels
		d.filterSelectorExtractor = extractor
	}
}

// MatchLabelSelector ensures Fetch only returns objects that would be selected by the provided label selector.
//
// Panics may occur:
//   - when used in the same Fetch call more than once
//   - when used with a collection containing types that do not implement [Labeler].
//
// Intended for use with Kubernetes objects that contain a label selector. See [metav1.LabelSelectorAsSelector] for
// details.
func MatchLabelSelector(selector labels.Selector) FetchOption {
	return func(d *dependency) {
		if d.filterLabelSelector != nil {
			panic("MatchLabelSelector added twice in the same Fetch call.")
		}
		d.filterLabelSelector = selector
	}
}

// MatchNames ensures Fetch only returns Kubernetes objects with a matching set of names and namespaces. Names must be
// formatted as "{namespace}/{name}", or "{name}", for non-namespaced objects. MatchNames may be used multiple times in
// the same Fetch call.
//
// Panics will occur when used in Collections that do not contain [metav1.Object].
func MatchNames(names ...string) FetchOption {
	return func(d *dependency) {
		if d.filterNames == nil {
			d.filterNames = make(map[cache.ObjectName]struct{}, len(names))
		}
		for _, name := range names {
			before, after, ok := strings.Cut(name, "/")
			if !ok {
				d.filterNames[cache.ObjectName{Name: before}] = struct{}{}
			} else {
				d.filterNames[cache.ObjectName{Name: after, Namespace: before}] = struct{}{}
			}
		}
	}
}

// MatchFilter ensures [Fetch] only returns objects which match the provided filter.
//
// Panics will occur:
//   - in case T does not match the type of the collection passed to Fetch.
//   - when multiple MatchFilter calls are passed to the same Fetch invocation.
//   - when both MatchFilter and [MatchIndex] are passed to the same Fetch invocation.
func MatchFilter[T any](filter func(T) bool) FetchOption {
	return func(d *dependency) {
		if d.filterFunc != nil {
			panic("MatchFilter cannot be used along with MatchIndex or a second call to MatchFilter")
		}
		d.filterFunc = func(a any) bool {
			return filter(a.(T))
		}
	}
}

// MatchIndex ensures Fetch only returns objects which match the provided index and key.
//
// Panics may occur:
//   - in case T does not match the type of the collection passed to Fetch.
//   - when multiple MatchIndex calls are passed to the same Fetch invocation.
//   - when both [MatchFilter] and MatchIndex are passed to the same Fetch invocation.
func MatchIndex[T any](index Index[T], key string) FetchOption {
	return func(d *dependency) {
		if d.filterFunc != nil {
			panic("MatchIndex cannot be used along with MatchFilter or a second call to MatchIndex")
		}
		d.filterFunc = func(a any) bool {
			return index.objectHasKey(a.(T), key)
		}
	}
}

// Labeler is implemented by any type which can have labels. Notably, any type embedding [metav1.Object] implements
// Labeler.
type Labeler interface {
	GetLabels() map[string]string
}

// LabelSelectorer is provided for use with [MatchLabelSelector]. Collections containing LabelSelectorers are valid
// Fetch targets. See also [ExtractPodSelector].
type LabelSelectorer interface {
	GetLabelSelector() map[string]string
}

// kontext is used to track dependencies via calls to Fetch made in a Mapper or FlatMapper.
type kontext[I, O any] struct {
	collection   *derivedCollection[I, O]
	key          key[I]
	dependencies []*dependency

	// trackedKeys contains a record of all filtered items fetched this fetch. The length of trackedKeys is upper bounded
	// by MaxFetchKeyTracking
	trackedKeys map[string]struct{}

	resetTracking map[uint64]struct{}
}

func newKontext[I, O any](collection *derivedCollection[I, O], iKey key[I]) *kontext[I, O] {
	return &kontext[I, O]{
		collection:    collection,
		key:           iKey,
		trackedKeys:   make(map[string]struct{}),
		resetTracking: make(map[uint64]struct{}),
	}
}

func (ktx *kontext[I, O]) registerDependency(d *dependency, syn Syncer, register func(func([]Event[any])) Syncer) bool {
	// register a watch on the parent collection, unless we already have one
	if _, ok := ktx.collection.collectionDependencies[d.collectionID]; !ok {
		l := ktx.collection.logger().With("fromCollectionName", d.collectionName, "fromCollectionID", d.collectionID)

		l.Debug("registering dependency")

		syn.WaitUntilSynced(ktx.collection.stop) // wait until passed collection is synced.
		ktx.collection.collectionDependencies[d.collectionID] = struct{}{}

		register(func(anys []Event[any]) { // register and wait for sync
			l.Debug("pushing fetch events", "count", len(anys))
			ktx.collection.pushFetchEvents(d, anys)
		}).WaitUntilSynced(ktx.collection.stop)

		l.Debug("dependency registration has synced")
	}

	ktx.dependencies = append(ktx.dependencies, d)
	ktx.collection.logger().Info("added dependency", "count", len(ktx.dependencies))
	return false
}

func (ktx *kontext[I, O]) trackKeys(keys []string) {
	for _, key := range keys {
		ktx.trackedKeys[key] = struct{}{}
	}
}

func (ktx *kontext[I, O]) resetTrackingForCollection(collID uint64) {
	ktx.resetTracking[collID] = struct{}{}
}

func castEvent[I, O any](o Event[I]) Event[O] {
	e := Event[O]{
		Type: o.Type,
	}
	if o.Old != nil {
		e.Old = ptr.To(any(*o.Old).(O))
	}
	if o.New != nil {
		e.New = ptr.To(any(*o.New).(O))
	}
	return e
}

// SelectorExtractor can extract a [labels.Selector] from objects in a collection. Intended for use with
// [MatchSelectsLabels].
type SelectorExtractor func(any) labels.Selector

// ExtractPodSelector knows how to retrieve pod label selectors from many common Kubernetes objects, and any
// LabelSelectorer. Panics if an unsupported object is used.
//
// Can fetch pod label selectors from *[corev1.Service], *[appsv1.DaemonSet], *[appsv1.Deployment],
// *[appsv1.ReplicaSet], *[appsv1.StatefulSet], *[batchv1.Job], and *[batchv1.CronJob].
func ExtractPodSelector(obj any) labels.Selector {
	var lblSelector *metav1.LabelSelector
	switch typed := obj.(type) {
	case LabelSelectorer:
		lblSelector = &metav1.LabelSelector{MatchLabels: typed.GetLabelSelector()}

	case *corev1.Service:
		lblSelector = &metav1.LabelSelector{MatchLabels: typed.Spec.Selector}

	case *appsv1.DaemonSet:
		lblSelector = typed.Spec.Selector

	case *appsv1.Deployment:
		lblSelector = typed.Spec.Selector

	case *appsv1.ReplicaSet:
		lblSelector = typed.Spec.Selector

	case *appsv1.StatefulSet:
		lblSelector = typed.Spec.Selector

	case *batchv1.Job:
		lblSelector = typed.Spec.Selector
	case *batchv1.CronJob:
		lblSelector = typed.Spec.JobTemplate.Spec.Selector

	default:
		panic(fmt.Sprintf("Could not fetch label selector for object of type %T", obj))
	}

	s, err := metav1.LabelSelectorAsSelector(lblSelector)
	if err != nil {
		return nil
	}
	return s
}
