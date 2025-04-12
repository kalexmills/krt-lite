package pkg

import (
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
)

type static[T any] struct { // TODO: test
	val     atomic.Pointer[T]
	synced  atomic.Bool
	id      uint64
	currKey string

	mut      *sync.RWMutex
	handlers []func(o []Event[T])
}

func newStatic[T any]() *static[T] {
	return &static[T]{
		val:    atomic.Pointer[T]{},
		synced: atomic.Bool{},
		id:     nextUID(),
		mut:    new(sync.RWMutex),
	}
}

var _ Collection[any] = &static[any]{}

func (s *static[T]) GetKey(k string) *T {
	if s.currKey == k {
		return s.val.Load()
	}
	return nil
}

func (s *static[T]) List() []T {
	v := s.val.Load()
	if v == nil {
		return nil
	}
	return []T{*v}
}

func (s *static[T]) Register(f func(ev Event[T])) cache.ResourceEventHandlerRegistration {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *static[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) cache.ResourceEventHandlerRegistration {
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

func (s *static[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, s.HasSynced)
}

func (s *static[T]) HasSynced() bool {
	return s.synced.Load()
}

func (s *static[T]) Get() *T {
	return s.val.Load()
}

func (s *static[T]) Set(now *T) {
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
	for _, h := range s.handlers {
		h([]Event[T]{ev})
	}
}

func (s *static[T]) MarkSynced() {
	s.synced.Store(true)
}
