package pkg

import (
	"k8s.io/client-go/tools/cache"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// Join joins together a slice of collections. If any keys overlap, all overlapping key are joined using the provided
// Joiner. Joiner will always be called with at least two inputs.
func Join[T any](cs []Collection[T], j Joiner[T], opts ...CollectorOption) IndexableCollection[T] {
	return newJoinedCollection(cs, j, opts)
}

// JoinDisjoint joins together a slice of collections whose keys do not overlap.
func JoinDisjoint[T any](cs []Collection[T], opts ...CollectorOption) IndexableCollection[T] {
	return newJoinedCollection(cs, nil, opts)
}

// joinedCollection joins together the results of several collections, all of which have the same type.
type joinedCollection[O any] struct {
	collectorMeta
	collections []Collection[O]
	stop        chan struct{}
	joiner      Joiner[O]
	syncer      *multiSyncer
	idSyncer    *idSyncer

	// TODO: this should be a good use-case for wrapping up a type-safe sync.Map instead of using global locks

	// inputs tracks inputs for the same key and the collection they came from by index.
	// e.g. if collections[3] associates "key" with "value", then inputs["key"][3] == "value"
	inputs map[string]map[int]O
	inMut  *sync.Mutex // inMut protects inputs

	outMut  *sync.RWMutex // outMut protects outputs
	outputs map[string]O

	idxMut  *sync.RWMutex // idxMut protects indices
	indices []*mapIndex[O]

	regHandlerMut      *sync.RWMutex // regHandlerMut protects registeredHandlers.
	registeredHandlers map[*registrationHandler[Event[O]]]struct{}
}

var _ Collection[any] = &joinedCollection[any]{}

func newJoinedCollection[O any](cs []Collection[O], joiner Joiner[O], opts []CollectorOption) *joinedCollection[O] {
	j := &joinedCollection[O]{
		collectorMeta: newCollectorMeta(opts),
		collections:   cs,
		stop:          make(chan struct{}),
		syncer:        &multiSyncer{},
		joiner:        joiner,

		inMut:   &sync.Mutex{},
		outMut:  &sync.RWMutex{},
		inputs:  make(map[string]map[int]O),
		outputs: make(map[string]O),

		idxMut:  &sync.RWMutex{},
		indices: nil,

		regHandlerMut:      &sync.RWMutex{},
		registeredHandlers: make(map[*registrationHandler[Event[O]]]struct{}),
	}

	for idx, c := range cs {
		j.syncer.syncers = append(j.syncer.syncers, c.HasSynced)

		if joiner != nil {
			// register with all our parents so we can track which inputs come from which collection.
			reg := c.RegisterBatched(j.handleEvents(idx), true)

			j.syncer.syncers = append(j.syncer.syncers, reg.HasSynced)
		}
	}
	if joiner != nil {
		j.idSyncer = newIDSyncer(len(cs))
		j.syncer.syncers = append(j.syncer.syncers, j.idSyncer.HasSynced)
	}
	return j
}

// handleEvents handles events coming from the collection with the provided index.
func (j *joinedCollection[O]) handleEvents(idx int) func(events []Event[O]) {
	return func(events []Event[O]) {
		j.inMut.Lock()
		defer j.inMut.Unlock()

		// TODO: there are spurious add events being emitted with old = nil and new = nil... Add a tracker test and get them out.

		outEvents := make([]Event[O], 0, len(events))
		for _, ev := range events {
			o := ev.Latest()
			k := GetKey(o)

			inputMap := j.getInputMap(k)
			switch ev.Event {
			case EventAdd, EventUpdate:
				inputMap[idx] = o
			case EventDelete:
				delete(inputMap, idx)
			}
		}

		j.outMut.Lock()
		defer j.outMut.Unlock()
		for _, ev := range events {
			o := ev.Latest()
			k := GetKey(o)

			inputMap := j.getInputMap(k)
			if len(inputMap) == 0 {
				old, oldOk := j.outputs[k]
				delete(j.outputs, k)

				if oldOk {
					outEvents = append(outEvents, Event[O]{
						Event: EventDelete,
						Old:   &old,
					})
				}
			} else {
				oldO, oldOk := j.outputs[k]

				if len(inputMap) == 1 {
					j.outputs[k] = inputMap[idx]
				} else {
					j.outputs[k] = j.joiner(slices.Collect(maps.Values(inputMap)))
				}

				newO := j.outputs[k]

				if !oldOk {
					continue
				}

				outEv := Event[O]{
					Event: EventAdd,
					New:   &newO,
				}
				if oldOk {
					if reflect.DeepEqual(oldO, newO) { // TODO: avoid reflection if possible
						continue
					}
					outEv.Old = &oldO
					outEv.Event = EventUpdate
				}
				outEvents = append(outEvents, outEv)
			}
		}

		if !j.idSyncer.HasSynced() {
			slog.Info("marking as synced", "idx", idx, "name", j.name)
			j.idSyncer.MarkSynced(idx)
		} else if j.HasSynced() {
			j.distributeEvents(outEvents, false)
		}
	}
}

func (j *joinedCollection[O]) distributeEvents(events []Event[O], isInInitialList bool) {
	slog.Info("distributing output events", "events", events, "isInitialList", isInInitialList, "name", j.name)
	j.idxMut.RLock()
	for _, idx := range j.indices {
		idx.handleEvents(events)
	}
	j.idxMut.RUnlock()

	j.regHandlerMut.RLock()
	for h := range j.registeredHandlers {
		h.send(events, isInInitialList)
	}
	defer j.regHandlerMut.RUnlock()
}

// getInputMap fetches and lazily initializes the output map associated with the provided key. Must only be called while
// inMut is held
func (j *joinedCollection[O]) getInputMap(key string) map[int]O {
	if result, ok := j.inputs[key]; ok {
		return result
	}
	j.inputs[key] = map[int]O{}
	return j.inputs[key]
}

func (j *joinedCollection[O]) GetKey(k string) *O {
	if j.joiner == nil {
		// if collections are disjoint we can be lazy and stop early
		for _, c := range j.collections {
			if r := c.GetKey(k); r != nil {
				return r
			}
		}
		return nil
	}

	// if collections are not disjoint, we must merge all matching inputs
	j.outMut.RLock()
	defer j.outMut.RUnlock()
	if out, ok := j.outputs[k]; ok {
		return &out
	}
	return nil
}

func (j *joinedCollection[O]) List() []O {
	var res []O

	// if no joiner was provided, collect and return your result on-the-fly.
	if j.joiner == nil {
		first := true
		for _, c := range j.collections {
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

	j.outMut.RLock()
	defer j.outMut.RUnlock()
	return slices.Collect(maps.Values(j.outputs))
}

func (j *joinedCollection[O]) Register(f func(o Event[O])) cache.ResourceEventHandlerRegistration {
	return j.RegisterBatched(func(evs []Event[O]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (j *joinedCollection[O]) RegisterBatched(f func(o []Event[O]), runExistingState bool) cache.ResourceEventHandlerRegistration {
	if j.joiner == nil {
		s := multiSyncer{}
		// TODO: handle deregistration
		// register with each parent collection, and add to our of collections waiting for sync.
		for _, c := range j.collections {
			reg := c.RegisterBatched(f, runExistingState)
			s.syncers = append(s.syncers, reg.HasSynced)
		}
	}

	p := newRegistrationHandler(j, f)

	j.regHandlerMut.Lock()
	defer j.regHandlerMut.Unlock()
	j.registeredHandlers[p] = struct{}{}

	if !runExistingState {
		p.markSynced()
		go p.run()
		return p
	}

	go func() {
		slog.Info("waiting until synced", "syncerCount", len(j.syncer.syncers), "name", j.name)
		j.syncer.WaitUntilSynced(j.stop) // wait for all parents to sync before snapshotting and sending
		slog.Info("synced!", "syncerCount", len(j.syncer.syncers), "name", j.name)
		p.send(j.snapshotInitialState(), true)
	}()

	go p.run()

	return p
}

func (j *joinedCollection[O]) snapshotInitialState() []Event[O] {
	j.outMut.RLock()
	defer j.outMut.RUnlock()

	events := make([]Event[O], 0, len(j.outputs))
	for _, o := range j.outputs {
		events = append(events, Event[O]{
			New:   &o,
			Event: EventAdd,
		})
	}
	return events
}

func (j *joinedCollection[O]) HasSynced() bool {
	return j.syncer.HasSynced()
}

func (j *joinedCollection[O]) WaitUntilSynced(stop <-chan struct{}) bool {
	return j.syncer.WaitUntilSynced(stop)
}

func (j *joinedCollection[O]) Index(e KeyExtractor[O]) Index[O] {
	idx := newIndex(j, e, func(oKeys map[key[O]]struct{}) []O {
		j.outMut.RLock()
		defer j.outMut.RUnlock()
		result := make([]O, 0, len(oKeys))
		for oKey := range oKeys {
			result = append(result, j.outputs[string(oKey)])
		}
		return result
	})

	j.idxMut.Lock()
	defer j.idxMut.Unlock()

	j.indices = append(j.indices, idx)
	return idx
}
