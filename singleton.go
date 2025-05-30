package krtlite

import (
	"sync"
	"sync/atomic"
)

// NewSingleton creates a Collection containing a single item. Updates to this item must be made manually. Intended
// use-cases include config which may be live-updated.
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
	val atomic.Pointer[T]

	closeSynced *sync.Once
	syncedCh    chan struct{}

	currKey string

	mut        *sync.RWMutex
	handlers   map[uint64]*func(o []Event[T])
	handlerIdx uint64
}

func newSingleton[T any](opts []CollectionOption) *singleton[T] {
	return &singleton[T]{
		collectionShared: newCollectionShared(opts),
		val:              atomic.Pointer[T]{},
		syncedCh:         make(chan struct{}),
		closeSynced:      &sync.Once{},
		mut:              new(sync.RWMutex),
		handlers:         make(map[uint64]*func([]Event[T])),
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

func (s *singleton[T]) Register(f func(ev Event[T])) Registration {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *singleton[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Registration {
	if runExistingState {
		v := s.val.Load()
		if v != nil {
			go f([]Event[T]{{ // call handler on another thread to avoid deadlocks while updating handlers
				New:  v,
				Type: EventAdd,
			}})
		}
	}

	s.mut.Lock()
	defer s.mut.Unlock()
	unreg := s.unregister(s.handlerIdx)
	s.handlers[s.handlerIdx] = &f
	s.handlerIdx++
	return &singletonRegistration{unregister: unreg}
}

func (s *singleton[T]) unregister(idx uint64) func() {
	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()
		delete(s.handlers, idx)
	}
}

func (s *singleton[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	select {
	case <-s.syncedCh:
		return true
	case <-stop:
		return false
	}
}

func (s *singleton[T]) HasSynced() bool {
	select {
	case <-s.syncedCh:
		return true
	default:
		return false
	}
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
			New:  now,
			Type: EventAdd,
		}
		s.currKey = GetKey(*now)
	} else if now == nil {
		ev = Event[T]{
			Old:  old,
			Type: EventDelete,
		}
		s.currKey = ""
	} else {
		ev = Event[T]{
			New:  now,
			Old:  old,
			Type: EventUpdate,
		}
		s.currKey = GetKey(*now)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()
	for _, h := range s.handlers {
		(*h)([]Event[T]{ev})
	}
}

func (s *singleton[T]) MarkSynced() {
	s.closeSynced.Do(func() {
		close(s.syncedCh)
	})
}

type singletonRegistration struct {
	unregister func()
}

func (s singletonRegistration) Unregister() {
	s.unregister()
}

func (s singletonRegistration) WaitUntilSynced(stopCh <-chan struct{}) bool {
	return true
}

func (s singletonRegistration) HasSynced() bool {
	return true
}
