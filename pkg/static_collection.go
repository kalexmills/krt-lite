package pkg

import (
	"maps"
	"slices"
	"sync"
)

type StaticCollection[T any] interface {
	Collection[T]

	// Update adds or updates an object from the collection.
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

	handlers map[*registrationHandler[Event[T]]]struct{}
}

// NewStaticCollection creates and returns a new static collection, initialized with the provided list of values. If
// synced is nil, the collection will start synced.
func NewStaticCollection[T any](synced Syncer, vals []T, opts ...CollectionOption) StaticCollection[T] {
	if synced == nil {
		synced = alwaysSynced{}
	}
	res := &staticList[T]{
		collectionShared: newCollectionShared(opts),
		vals:             make(map[string]T, len(vals)),
		syncer:           synced,
		stop:             make(chan struct{}),
		handlers:         make(map[*registrationHandler[Event[T]]]struct{}),
	}
	for _, t := range vals {
		k := GetKey(t)
		res.vals[k] = t
	}
	return res
}

func (s *staticList[T]) Register(f func(o Event[T])) Syncer {
	return s.RegisterBatched(func(evs []Event[T]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (s *staticList[T]) RegisterBatched(f func(o []Event[T]), runExistingState bool) Syncer {
	handler := newRegistrationHandler[T](s, f)

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
	var evs []Event[T]
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
	for h := range s.handlers {
		h.send(evs, false)
	}
}
