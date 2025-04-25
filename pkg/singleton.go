package pkg

import (
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
)

// NewSingleton creates a Collection containing a single item.
func NewSingleton[T any](initial *T, startSynced bool, opts ...CollectionOption) Singleton[T] {
	result := newSingleton[T](opts)
	result.Set(initial)
	if startSynced {
		result.MarkSynced()
	}
	return result
}

type singleton[T any] struct {
	collectionShared
	val     atomic.Pointer[T]
	synced  atomic.Bool
	currKey string

	mut      *sync.RWMutex
	handlers []func(o []Event[T])
}

func newSingleton[T any](opts []CollectionOption) *singleton[T] {
	return &singleton[T]{
		collectionShared: newCollectionShared(opts),
		val:              atomic.Pointer[T]{},
		synced:           atomic.Bool{},
		mut:              new(sync.RWMutex),
	}
}

var _ Collection[any] = &singleton[any]{}

func (s *singleton[T]) GetKey(k string) *T {
	if s.currKey == k {
		return s.val.Load()
	}
	return nil
}

func (s *singleton[T]) List() []T {
	v := s.val.Load()
	if v == nil {
		return nil
	}
	return []T{*v}
}

func (s *singleton[T]) Register(f func(ev Event[T])) Syncer {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *singleton[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Syncer {
	if runExistingState {
		v := s.val.Load()
		if v != nil {
			f([]Event[T]{{
				New:   v,
				Event: EventAdd,
			}})
		}
	}

	s.mut.Lock()
	defer s.mut.Unlock()
	s.handlers = append(s.handlers, f)
	return alwaysSynced{}
}

func (s *singleton[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, s.HasSynced) // TODO: remove WaitForCacheSync
}

func (s *singleton[T]) HasSynced() bool {
	return s.synced.Load()
}

func (s *singleton[T]) Get() *T {
	return s.val.Load()
}

func (s *singleton[T]) Set(now *T) {
	old := s.val.Swap(now)
	if old == now {
		return
	}
	// update handlers
	var ev Event[T]
	if old == nil {
		ev = Event[T]{
			New:   now,
			Event: EventAdd,
		}
		s.currKey = GetKey(*now)
	} else if now == nil {
		ev = Event[T]{
			Old:   old,
			Event: EventDelete,
		}
		s.currKey = ""
	} else {
		ev = Event[T]{
			New:   now,
			Old:   old,
			Event: EventUpdate,
		}
		s.currKey = GetKey(*now)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()
	for _, h := range s.handlers {
		h([]Event[T]{ev})
	}
}

func (s *singleton[T]) MarkSynced() {
	s.synced.Store(true)
}
