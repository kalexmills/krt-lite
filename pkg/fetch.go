package pkg

import (
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"strings"
)

// Context is used to track dependencies between Collections via Fetch.
type Context interface {
	registerDependency(d *dependency, s Syncer, register func(func([]Event[any])) Syncer)
	trackKeys(keys []string)
}

// dependency identifies a dependency between two collections created via Fetch.
type dependency struct {
	depID          uint64
	collectionID   uint64
	collectionName string

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

// MatchKeys ensures Fetch only returns objects with matching keys. MatchKeys may be used multiple times in the same
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

// MatchLabels ensures Fetch only returns Kubernetes objects with a matching set of labels.
// Panics may occur:
//   - when used in the same Fetch call more than once.
func MatchLabels(labels map[string]string) FetchOption {
	return func(d *dependency) {
		if d.filterLabels != nil {
			panic("MatchLabels added twice to the same Fetch call.")
		}
		d.filterLabels = labels
	}
}

// MatchSelectsLabels ensures Fetch only returns objects that would select objects with the provided set of labels.
// Caller must provide an extractor which retrieves [labels.Selector] implementations from objects in the target
// collection. See
//
// Panics may occur:
//   - when used in the same Fetch call more than once.
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
// Panics may occur:
//   - when used in the same Fetch call more than once
//   - when used with a collection containing types that do not implement Labeler.
//
// Intended for use with k8s objects that have a label selector. See [metav1.LabelSelectorAsSelector] for details.
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
// Panics may occur when used in Collections that do not contain kubernetes [metav1.Object].
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

// MatchFilter ensures Fetch only returns objects which match the provided filter.
//
// Panics may occur:
//   - when T does not match the type of the collection passed to Fetch.
//   - when used in the same Fetch call as MatchIndex, or a separate call to MatchFilter.
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
//   - when T does not match the type of the collection passed to Fetch.
//   - when used in the same Fetch call as MatchIndex, or a separate call to MatchFilter.
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

// Labeler is implemented by any type which can have labels.
type Labeler interface {
	GetLabels() map[string]string
}

// LabelSelectorer is implemented by any type which has a LabelSelector.
type LabelSelectorer interface {
	GetLabelSelector() map[string]string
}

// kontext is used to track dependencies via calls to Fetch made in a Mapper or FlatMapper.
type kontext[I, O any] struct {
	collection   *derivedCollection[I, O]
	key          key[I]
	dependencies []*dependency

	// trackedKeys contains a record of all filtered items fetched this fetch. The length of trackedKeys should always be
	// T(1).
	trackedKeys []string
}

func (ktx *kontext[I, O]) registerDependency(d *dependency, syn Syncer, register func(func([]Event[any])) Syncer) {
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
}

func (ktx *kontext[I, O]) trackKeys(keys []string) {
	ktx.trackedKeys = keys
}

// MaxTrackKeys constrains memory usage by placing a maximum cap on the number of keys tracked between Fetch
// dependencies. Can be increased to trade-off memory for faster processing of events triggered via Fetch.
var MaxTrackKeys = 10

// Fetch calls List on the provided Collection, and can be used to subscribe Collections created by Map or FlatMap to
// updates from additional collections. Passing a non-nil Context to Fetch subscribes the collection to updates from
// Collection c. When no Context is passed, Fetch is equivalent to c.List().
func Fetch[T any](ktx Context, c Collection[T], opts ...FetchOption) []T {
	if ktx == nil {
		return c.List()
	}

	d := &dependency{
		depID:          nextDependencyUID(),
		collectionID:   c.getUID(),
		collectionName: c.getName(),
	}

	for _, opt := range opts {
		opt(d)
	}

	ts := c.List()
	var out []T
	for _, t := range ts {
		if d.Matches(t) {
			out = append(out, t)
		}
	}

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

	for i := 0; i < min(MaxTrackKeys, len(out)); i++ {
		var keys []string
		for _, t := range out {
			keys = append(keys, GetKey[any](t))
		}
		ktx.trackKeys(keys)
	}

	return out
}

// SelectorExtractor fetches labels.Selectors from objects in a collection. If nil is returned for any object, it
// will always match nothing.
type SelectorExtractor func(any) labels.Selector

// LabelSelectorExtractor knows how to retrieve label selectors from many common Kubernetes objects, and any
// LabelSelectorer. Panics if an unsupported object is used.
//
// Supports fetching pod label selectors from *[corev1.Service], *[appsv1.DaemonSet], *[appsv1.Deployment],
// *[appsv1.ReplicaSet], *[appsv1.StatefulSet], *[batchv1.Job], and *[batchv1.CronJob].
func LabelSelectorExtractor(obj any) labels.Selector {
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
