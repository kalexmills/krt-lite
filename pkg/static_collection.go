package pkg

import (
	"maps"
	"slices"
	"sync"
)

// A StaticCollection contains elements which are not derived from other Collections. It is intended to be used to tie
// events from other systems into KRT.
type StaticCollection[T any] interface {
	IndexableCollection[T]

	// Update adds or updates an object.
	Update(obj T)
	// Delete deletes an object from the collection.
	Delete(key string)

	// DeleteFunc deletes all objects matching the filter.
	DeleteFunc(filter func(T) bool)
}

type staticList[T any] struct {
	collectionShared
	mu     sync.RWMutex
	vals   map[string]T
	stop   <-chan struct{}
	syncer Syncer

	indices []*mapIndex[T]

	handlers map[*registrationHandler[T]]struct{}
}

var _ StaticCollection[any] = &staticList[any]{}

// NewStaticCollection creates and returns a new StaticCollection containing the provided list of values. If no Syncer
// is provided, the collection starts as synced. Otherwise the Syncer provided is used to determine when this
// StaticCollection has seen all of its initial state. Caller is responsible for ensuring the Syncer passed eventually
// syncs -- failing to do so will prevent downstream Collections from starting.
func NewStaticCollection[T any](synced Syncer, vals []T, opts ...CollectionOption) StaticCollection[T] {
	if synced == nil {
		synced = alwaysSynced{}
	}
	res := &staticList[T]{
		collectionShared: newCollectionShared(opts),
		vals:             make(map[string]T, len(vals)),
		syncer:           synced,
		stop:             make(chan struct{}),
		handlers:         make(map[*registrationHandler[T]]struct{}),
	}
	for _, t := range vals {
		k := GetKey(t)
		res.vals[k] = t
	}
	return res
}

func (s *staticList[T]) Register(f func(o Event[T])) Registration {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *staticList[T]) unregisterFunc(h *registrationHandler[T]) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.handlers, h)
	}
}

func (s *staticList[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Registration {
	handler := newRegistrationHandler[T](s, f)
	handler.unregister = s.unregisterFunc(handler)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[handler] = struct{}{}

	if !runExistingState {
		handler.markSynced()
		go handler.run()
		return handler
	}

	go func() {
		s.syncer.WaitUntilSynced(s.stop)
		handler.send(s.snapshotInitialState(), true)
	}()

	go handler.run()

	return handler
}

func (s *staticList[T]) snapshotInitialState() []Event[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	evs := make([]Event[T], 0, len(s.vals))
	for _, v := range s.vals {
		evs = append(evs, Event[T]{
			New:   &v,
			Event: EventAdd,
		})
	}
	return evs
}

func (s *staticList[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return s.syncer.WaitUntilSynced(stop)
}

func (s *staticList[T]) HasSynced() bool {
	return s.syncer.HasSynced()
}

func (s *staticList[T]) GetKey(key string) *T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.vals[key]
	if !ok {
		return nil
	}
	return &val
}

func (s *staticList[T]) List() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Values(s.vals))
}

func (s *staticList[T]) Index(extractor KeyExtractor[T]) Index[T] { // TODO: test indexing.
	idx := newMapIndex[T](s, extractor, func(m map[key[T]]struct{}) []T {
		res := make([]T, 0, len(m))
		s.mu.RLock()
		defer s.mu.RUnlock()
		for key := range m {
			if t, ok := s.vals[string(key)]; ok {
				res = append(res, t)
			}
		}
		return res
	})
	s.indices = append(s.indices, idx)
	return idx
}

func (s *staticList[T]) Update(obj T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := GetKey(obj)
	old, ok := s.vals[k]
	s.vals[k] = obj
	if ok {
		s.distributeEvents([]Event[T]{{
			Old:   &old,
			New:   &obj,
			Event: EventUpdate,
		}})
	} else {
		s.distributeEvents([]Event[T]{{
			New:   &obj,
			Event: EventAdd,
		}})
	}
}

func (s *staticList[T]) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.vals[key]
	if ok {
		delete(s.vals, key)
		s.distributeEvents([]Event[T]{{
			Old:   &old,
			Event: EventDelete,
		}})
	}
}

func (s *staticList[T]) DeleteFunc(filter func(T) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var removed []Event[T]
	for k, v := range s.vals {
		if filter(v) {
			delete(s.vals, k)
			removed = append(removed, Event[T]{
				Old:   &v,
				Event: EventDelete,
			})
		}
	}
	if len(removed) > 0 {
		s.distributeEvents(removed)
	}
}

func (s *staticList[T]) distributeEvents(evs []Event[T]) {
	for _, h := range s.indices {
		h.handleEvents(evs)
	}
	for h := range s.handlers {
		h.send(evs, false)
	}
}
