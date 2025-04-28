package pkg

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// Context is used to track dependencies between Collections via Fetch.
type Context interface {
	registerDependency(d *dependency, s Syncer, register func(func([]Event[any])) Syncer)
	trackKeys(keys []string)
}

// MatchKeys ensures Fetch only returns objects with matching keys. MatchKeys may be included multiple times in the
// same fetch. Multiple MatchKeys are OR'd.
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
// Panics if used in the same Fetch call twice.
func MatchLabels(lbls map[string]string) FetchOption {
	return func(d *dependency) {
		if d.filterLabels == nil {
			panic("MatchLabels added twice to the same call to Fetch.")
		}
		d.filterLabels = lbls
	}
}

// MatchNames ensures Fetch only returns Kubernetes objects with a matching set of names and namespaces. MatchNames may
// be included multiple times in the same Fetch call. Multiple MatchNames are OR'd.
func MatchNames(names ...types.NamespacedName) FetchOption {
	return func(d *dependency) {
		if d.filterNames == nil {
			d.filterNames = make(map[cache.ObjectName]struct{}, len(names))
		}
		for _, name := range names {
			d.filterNames[cache.ObjectName{Name: name.Name, Namespace: name.Namespace}] = struct{}{}
		}
	}
}

// dependency identifies a dependency between two collections created via Fetch.
type dependency struct {
	depID          uint64
	collectionID   uint64
	collectionName string

	filterKeys   map[string]struct{}
	filterLabels map[string]string
	filterNames  map[cache.ObjectName]struct{}
}

func (d *dependency) Matches(object any) bool {
	if d.filterKeys != nil {
		if _, ok := d.filterKeys[GetKey[any](object)]; !ok {
			return false
		}
	}
	if d.filterNames != nil {
		if objName, err := cache.ObjectToName(object); err == nil {
			if _, ok := d.filterNames[objName]; !ok {
				return false
			}
		}
	}
	if d.filterLabels != nil {
		lbls := getLabels(object)
		if lbls != nil && !labels.SelectorFromSet(d.filterLabels).Matches(labels.Set(lbls)) {
			return false
		}
	}
	return true
}

type kontext[I, O any] struct {
	collection   *derivedCollection[I, O]
	key          key[I]
	dependencies []*dependency

	// trackedKeys contains a record of all filtered items fetched this fetch. The length of trackedKeys should always be
	// O(1).
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

	for i := 0; i < min(maxTrackKeys, len(out)); i++ {
		var keys []string
		for _, t := range out {
			keys = append(keys, GetKey[any](t))
		}
		ktx.trackKeys(keys)
	}

	return out
}

// maxTrackKeys constrains memory usage by placing a maximum cap on the number of keys tracked between dependencies. Most
// cases in which indexes or filters limit the number of dependencies will be caught.
const maxTrackKeys = 100
