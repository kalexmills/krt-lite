package pkg

import (
	"k8s.io/client-go/tools/cache"
	"maps"
	"slices"
	"sync"
)

// joinedCollection joins together the results of several collections, all of which have the same type.
type joinedCollection[O any] struct {
	id          uint64
	collections []Collection[O]
	joiner      Joiner[O]
	syncer      *multiSyncer

	// TODO: this should be a good use-case for wrapping up a type-safe sync.Map instead of using global locks

	inMut *sync.Mutex

	// inputs tracks inputs for the same key and the collection they came from by index.
	// e.g. if collections[3] associates "key" with "value", then inputs["key"][3] == "value"
	inputs map[string]map[int]O

	outMut  *sync.RWMutex
	outputs map[string]O

	idxMut  *sync.RWMutex
	indices []*mapIndex[O]
}

var _ Collection[any] = &joinedCollection[any]{}

func newJoinedCollection[O any](cs []Collection[O], joiner Joiner[O]) *joinedCollection[O] {
	j := &joinedCollection[O]{
		id:          nextUID(),
		collections: cs,
		syncer:      &multiSyncer{},
		joiner:      joiner,

		inMut:   &sync.Mutex{},
		outMut:  &sync.RWMutex{},
		inputs:  make(map[string]map[int]O),
		outputs: make(map[string]O),

		idxMut:  &sync.RWMutex{},
		indices: nil,
	}

	for idx, c := range cs {
		j.syncer.syncers = append(j.syncer.syncers, c.HasSynced)

		if joiner != nil {
			// register with all our parents so we can track which inputs come from which collection.
			reg := c.RegisterBatched(func(events []Event[O]) {
				j.inMut.Lock()
				defer j.inMut.Unlock()

				j.outMut.Lock()
				defer j.outMut.Unlock()

				for _, ev := range events {
					o := ev.Latest()
					k := GetKey(o)

					// update the appropriate valmap with the most recent item

					inputMap := j.getInputMap(k)
					switch ev.Event {
					case EventAdd:
						inputMap[idx] = o
					case EventUpdate:
						inputMap[idx] = o
					case EventDelete:
						delete(inputMap, idx)
					}

					if len(inputMap) == 0 {
						delete(j.outputs, k)
					} else if len(inputMap) == 1 {
						j.outputs[k] = inputMap[idx]
					} else {
						j.outputs[k] = j.joiner(slices.Collect(maps.Values(inputMap)))
					}
				}

				j.idxMut.RLock()
				defer j.idxMut.RUnlock()

				for _, idx := range j.indices {
					idx.handleEvents(events)
				}
			}, true)

			j.syncer.syncers = append(j.syncer.syncers, reg.HasSynced)
		}
	}

	return j
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
	s := multiSyncer{}
	// TODO: handle deregistration
	// register with each parent collection, and to our list of collections waiting for sync.
	for _, c := range j.collections {
		reg := c.RegisterBatched(f, runExistingState)
		s.syncers = append(s.syncers, reg.HasSynced)
	}
	return s
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
