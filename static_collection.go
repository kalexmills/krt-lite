package krtlite

import (
	"maps"
	"slices"
	"sync"
)

// A StaticCollection contains elements which are not derived from other Collections, or from an Informer. It is
// intended to be used to allow events from other systems to be tied into krt-lite.
type StaticCollection[T any] interface {
	IndexableCollection[T]

	// Update adds or updates an object.
	Update(obj T)
	// Delete deletes an object from the collection.
	Delete(key string)

	// DeleteFunc deletes all objects matching the informerFilter.
	DeleteFunc(filter func(T) bool)
}

type staticList[T any] struct {
	collectionShared
	mu     sync.RWMutex
	vals   map[string]T
	syncer Syncer

	indices []*mapIndex[T]

	regIdx   uint64
	handlers map[uint64]*registrationHandler[T]
}

var _ StaticCollection[any] = &staticList[any]{}

// NewStaticCollection returns a StaticCollection containing the provided list of values. If a nil Syncer
// is passed, the collection is considered synced on creation. Otherwise, the provided Syncer is used to determine when
// this StaticCollection has seen all of its initial state. Caller is responsible for ensuring the Syncer passed
// eventually syncs -- failing to do so will prevent downstream Collections from starting.
func NewStaticCollection[T any](synced Syncer, vals []T, opts ...CollectionOption) StaticCollection[T] {
	if synced == nil {
		synced = alwaysSynced{}
	}
	res := &staticList[T]{
		collectionShared: newCollectionShared(opts),
		vals:             make(map[string]T, len(vals)),
		syncer:           synced,
		handlers:         make(map[uint64]*registrationHandler[T]),
	}
	for _, t := range vals {
		k := GetKey(t)
		res.vals[k] = t
	}
	go func() {
		<-res.stop
		for _, h := range res.copyHandlerList() {
			h.Unregister() // Unregister grabs the lock as well, so we make a defensive copy of handlers before unregistering
		}
	}()

	return res
}

func (s *staticList[T]) copyHandlerList() []*registrationHandler[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Values(s.handlers))
}

func (s *staticList[T]) Register(f func(o Event[T])) Registration {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *staticList[T]) unregisterFunc(idx uint64) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.handlers, idx)
	}
}

func (s *staticList[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Registration {
	handler := newRegistrationHandler[T](s, f)

	s.mu.Lock()
	defer s.mu.Unlock()
	handler.unregister = s.unregisterFunc(s.regIdx)
	s.handlers[s.regIdx] = handler
	s.regIdx++

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
			New:  &v,
			Type: EventAdd,
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
			Old:  &old,
			New:  &obj,
			Type: EventUpdate,
		}})
	} else {
		s.distributeEvents([]Event[T]{{
			New:  &obj,
			Type: EventAdd,
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
			Old:  &old,
			Type: EventDelete,
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
				Old:  &v,
				Type: EventDelete,
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
	for _, h := range s.handlers {
		h.send(evs, false)
	}
}
