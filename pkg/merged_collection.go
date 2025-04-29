package pkg

// MergeDisjoint merges multiple collections into one. Items with duplicate keys will race for their presence in the
// collection. No guarantee is made regarding -- relying on this behavior is strongly discouraged.
func MergeDisjoint[T any](cs []Collection[T], opts ...CollectionOption) Collection[T] {
	return newMergedCollection(cs, opts)
}

// MergeIndexableDisjoint is identical to MergeDisjoint, except that the resulting collection is Indexable. Relies on
// all parent collections being indexable.
func MergeIndexableDisjoint[T any](cs []IndexableCollection[T], opts ...CollectionOption) IndexableCollection[T] {
	castCs := make([]Collection[T], 0, len(cs))
	for _, c := range cs {
		castCs = append(castCs, c)
	}
	return newMergedCollection(castCs, opts)
}

// mergedCollection merges the results of several collections, all of which have the same type.
type mergedCollection[T any] struct {
	collectionShared
	collections []Collection[T]
	syncer      *multiSyncer
	synced      chan struct{}
	stop        chan struct{}
}

var _ Collection[any] = &mergedCollection[any]{}

func newMergedCollection[O any](cs []Collection[O], opts []CollectionOption) *mergedCollection[O] {
	j := &mergedCollection[O]{
		collectionShared: newCollectionShared(opts),
		collections:      cs,
		stop:             make(chan struct{}),
		synced:           make(chan struct{}),
	}
	j.syncer = newMultiSyncer(channelSyncer{synced: j.synced})

	go j.init()
	return j
}

// init registers this mergedCollection with its parents and handles sync.
func (m *mergedCollection[T]) init() {
	defer close(m.synced)

	// wait for all parent collections to sync
	m.logger().Debug("waiting for parents to sync")
	for _, c := range m.collections {
		if !c.WaitUntilSynced(m.stop) {
			return
		}
	}

	m.logger().Debug("synced")
}

func (m *mergedCollection[T]) GetKey(k string) *T {
	// if collections are disjoint we can be lazy and stop early
	for _, c := range m.collections {
		if r := c.GetKey(k); r != nil {
			return r
		}
	}
	return nil
}

func (m *mergedCollection[T]) List() []T {
	var res []T
	first := true
	for _, c := range m.collections {
		objs := c.List()
		// As an optimization, take the first (non-empty) result as-is without copying
		if len(objs) > 0 && first {
			res = objs
			first = false
		} else {
			// After the first, safely merge into the result
			res = append(res, objs...)
		}
	}
	return res
}

func (m *mergedCollection[T]) Register(f func(o Event[T])) Registration {
	return m.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (m *mergedCollection[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Registration {
	r := &mergedRegistration{}
	// register with each parent collection, and add to our list of collections waiting for sync.
	for _, c := range m.collections {
		r.registrations = append(r.registrations, c.RegisterBatched(f, runExistingState))
	}
	return r
}

func (m *mergedCollection[T]) HasSynced() bool {
	return m.syncer.HasSynced()
}

func (m *mergedCollection[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return m.syncer.WaitUntilSynced(stop)
}

func (m *mergedCollection[T]) Index(e KeyExtractor[T]) Index[T] {
	ji := mergedIndexer[T]{indexers: make([]Index[T], 0, len(m.collections))}
	for _, c := range m.collections {
		cidxd, _ := c.(IndexableCollection[T])

		ji.indexers = append(ji.indexers, cidxd.Index(e))
	}
	return nil
}

type mergedIndexer[T any] struct {
	indexers []Index[T]
}

func (m mergedIndexer[T]) Lookup(key string) []T {
	var res []T
	first := true
	for _, i := range m.indexers {
		l := i.Lookup(key)
		if len(l) > 0 && first {
			res = l
			first = false
		} else {
			res = append(res, l...)
		}
	}
	return res
}

func (m mergedIndexer[T]) objectHasKey(t T, key string) bool { //nolint: unused // for interface
	for _, i := range m.indexers {
		if i.objectHasKey(t, key) {
			return true
		}
	}
	return false
}

type mergedRegistration struct {
	registrations []Registration // must not be modified after first use.
	syncer        *multiSyncer   // syncer formed from registrations; lazily initialized.
}

func (m *mergedRegistration) Unregister() {
	for _, r := range m.registrations {
		r.Unregister()
	}
}

func (m *mergedRegistration) WaitUntilSynced(stop <-chan struct{}) bool {
	return m.getSyncer().WaitUntilSynced(stop)
}

func (m *mergedRegistration) HasSynced() bool {
	return m.getSyncer().HasSynced()
}

func (m *mergedRegistration) getSyncer() *multiSyncer {
	if m.syncer == nil {
		m.syncer = newMultiSyncer()
		for _, r := range m.registrations {
			m.syncer.syncers = append(m.syncer.syncers, r)
		}
	}
	return m.syncer
}
