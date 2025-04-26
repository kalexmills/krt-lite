package pkg

import (
	"maps"
	"reflect"
	"slices"
	"sync"
)

// merge merges multiple collections into one. Items with duplicate keys are combined using the provided merger. merger
// will always be called with at least two inputs.
//
// TODO: currently broken -- does not pass conformance tests.
func merge[T any](cs []Collection[T], handler merger[T], opts ...CollectionOption) IndexableCollection[T] {
	return newMergedCollection(cs, handler, opts)
}

// MergeDisjoint merges multiple collections into one. Items with duplicate keys will race for their presence in the
// collection -- relying on this behavior is strongly discouraged.
func MergeDisjoint[T any](cs []Collection[T], opts ...CollectionOption) IndexableCollection[T] {
	return newMergedCollection(cs, nil, opts)
}

// mergedCollection merges the results of several collections, all of which have the same type.
type mergedCollection[O any] struct {
	collectionShared
	collections []Collection[O]
	merger      merger[O]
	syncer      *multiSyncer
	synced      chan struct{}
	stop        chan struct{}

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

var _ Collection[any] = &mergedCollection[any]{}

func newMergedCollection[O any](cs []Collection[O], merger merger[O], opts []CollectionOption) *mergedCollection[O] {
	j := &mergedCollection[O]{
		collectionShared: newCollectionShared(opts),
		collections:      cs,
		merger:           merger,
		stop:             make(chan struct{}),
		synced:           make(chan struct{}),

		inMut:   &sync.Mutex{},
		outMut:  &sync.RWMutex{},
		inputs:  make(map[string]map[int]O),
		outputs: make(map[string]O),

		idxMut:  &sync.RWMutex{},
		indices: nil,

		regHandlerMut:      &sync.RWMutex{},
		registeredHandlers: make(map[*registrationHandler[Event[O]]]struct{}),
	}
	j.syncer = newMultiSyncer(channelSyncer{synced: j.synced})

	go j.init()
	return j
}

// init registers this mergedCollection with its parents and handles sync
func (j *mergedCollection[O]) init() {
	defer close(j.synced)

	// wait for all parent collections to sync
	j.logger().Debug("waiting for parents to sync")
	for _, c := range j.collections {
		if !c.WaitUntilSynced(j.stop) {
			return
		}
	}

	if j.merger == nil {
		j.logger().Debug("merger is nil, synced")
		return
	}

	syn := make([]Syncer, 0, len(j.collections))

	j.logger().Debug("registering with parents")
	// register with all parent collections
	for idx, c := range j.collections {
		// register with all our parents, tracking which inputs come from which collection.
		reg := c.RegisterBatched(j.handleEvents(idx), true)
		syn = append(syn, reg)
	}

	// wait for all parents to be synced
	newMultiSyncer(syn...).WaitUntilSynced(j.stop)
	j.logger().Info("parent registrations are finished; synced")
}

// handleEvents handles events coming from the collection with the provided index.
func (j *mergedCollection[O]) handleEvents(idx int) func(events []Event[O]) {
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
					j.outputs[k] = j.merger(slices.Collect(maps.Values(inputMap)))
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

		if j.HasSynced() {
			j.distributeEvents(outEvents, false)
		} else {
			j.logger().Debug("throwing away events because we weren't synced")
		}
	}
}

func (j *mergedCollection[O]) distributeEvents(events []Event[O], isInInitialList bool) {
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

// getInputMap fetches and lazily initializes the output map associated with the provided key. Caller must hold inMut.
func (j *mergedCollection[O]) getInputMap(key string) map[int]O {
	if result, ok := j.inputs[key]; ok {
		return result
	}
	j.inputs[key] = map[int]O{}
	return j.inputs[key]
}

func (j *mergedCollection[O]) GetKey(k string) *O {
	if j.merger == nil {
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

func (j *mergedCollection[O]) List() []O {
	var res []O

	// if no merger was provided, collect and return results on-the-fly.
	if j.merger == nil {
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

func (j *mergedCollection[O]) Register(f func(o Event[O])) Syncer {
	return j.RegisterBatched(func(evs []Event[O]) {
		for _, ev := range evs {
			f(ev)
		}
	}, true)
}

func (j *mergedCollection[O]) RegisterBatched(f func(o []Event[O]), runExistingState bool) Syncer {
	if j.merger == nil {
		s := newMultiSyncer()
		// TODO: handle deregistration
		// register with each parent collection, and add to our list of collections waiting for sync.
		for _, c := range j.collections {
			reg := c.RegisterBatched(f, runExistingState)
			s.syncers = append(s.syncers, reg)
		}
		return s
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
		j.syncer.WaitUntilSynced(j.stop) // wait for all parents to sync before snapshotting and sending
		p.send(j.snapshotInitialState(), true)
	}()

	go p.run()

	return p
}

func (j *mergedCollection[O]) snapshotInitialState() []Event[O] {
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

func (j *mergedCollection[O]) HasSynced() bool {
	return j.syncer.HasSynced()
}

func (j *mergedCollection[O]) WaitUntilSynced(stop <-chan struct{}) bool {
	return j.syncer.WaitUntilSynced(stop)
}

func (j *mergedCollection[O]) Index(e KeyExtractor[O]) Index[O] {
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
