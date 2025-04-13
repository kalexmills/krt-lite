package pkg

import (
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"k8s.io/client-go/tools/cache"
	"maps"
	"slices"
	"sync"
)

// derivedCollection implements a collection whose contents are computed based on the contents of other collections.
// Dependencies between collections are tracked to ensure updates are propagated properly.
type derivedCollection[I, O any] struct {
	uid    uint64
	parent Collection[I]

	transformer FlatMapper[I, O]

	mut      *sync.RWMutex // mut protects inputs, outputs, and mappings
	inputs   map[key[I]]I
	outputs  map[key[O]]O
	mappings map[key[I]]map[key[O]]struct{}

	idxMut  *sync.RWMutex // idxMut protects indices.
	indices []*index[O]

	registeredHandlers map[*registrationHandler[Event[O]]]struct{}
	registrationWg     *sync.WaitGroup // TODO: use this for graceful shutdown?

	stop      chan struct{}
	parentReg cache.ResourceEventHandlerRegistration

	registrantsSynced cache.ResourceEventHandlerRegistration // TODO: multiple registers will break this.

	queue *fifo.Queue[[]Event[I]]
}

var _ Collection[any] = &derivedCollection[int, any]{}

func (c *derivedCollection[I, O]) run() {
	if !c.parent.WaitUntilSynced(c.stop) {
		return
	}
	c.parentReg = c.parent.RegisterBatched(c.handleEvents, true)

	// wait for parent to sync before running.
	if !cache.WaitForCacheSync(c.stop, c.parentReg.HasSynced) {
		return
	}

	c.queue.Run(c.stop)
}

// handleEvents handles all input events by computing the corresponding output and dispatching downstream events based
// on any changes to the current state of the collection.
func (c *derivedCollection[I, O]) handleEvents(inputs []Event[I]) {
	var outputEvents []Event[O]
	c.mut.Lock()
	defer c.mut.Unlock()

	recomputed := make([]map[key[O]]O, len(inputs))
	for idx, input := range inputs {
		if input.Event == EventDelete {
			continue
		}

		os := c.transformer(input.Latest())
		outmap := make(map[key[O]]O, len(os))
		for _, o := range os {
			outmap[getTypedKey(o)] = o
		}
		recomputed[idx] = outmap
	}

	for idx, input := range inputs {
		i := input.Latest()
		iKey := getTypedKey(i)

		// plumb input events to output events

		if input.Event == EventDelete {
			for oKey := range c.mappings[iKey] {
				old, ok := c.outputs[oKey]
				if !ok {
					continue
				}
				outputEvents = append(outputEvents, Event[O]{
					Event: EventDelete,
					Old:   &old,
				})
				delete(c.outputs, oKey)
			}
			delete(c.mappings, iKey)
			delete(c.inputs, iKey)
		} else {
			results := recomputed[idx]

			newKeys := setFromSeq(maps.Keys(results))
			oldKeys := c.mappings[iKey]
			c.mappings[iKey] = newKeys
			c.inputs[iKey] = i

			allKeys := make(map[key[O]]struct{})
			maps.Copy(allKeys, newKeys)
			maps.Copy(allKeys, oldKeys)

			for key := range allKeys {
				newRes, newOK := results[key]
				oldRes, oldOK := c.outputs[key]

				ev := Event[O]{}
				if newOK && oldOK {
					// TODO: test equivalence of old and new and skip.

					ev.Event = EventUpdate
					ev.New = &newRes
					ev.Old = &oldRes
					c.outputs[key] = newRes
				} else if newOK {
					ev.Event = EventAdd
					ev.New = &newRes
					c.outputs[key] = newRes
				} else {
					ev.Event = EventDelete
					ev.Old = &oldRes
					delete(c.outputs, key)
				}
				outputEvents = append(outputEvents, ev)
			}
		}
	}
	if len(outputEvents) == 0 {
		return
	}

	for _, idx := range c.indices {
		idx.handleEvents(outputEvents)
	}

	c.distributeEvents(outputEvents, !c.HasSynced())
}

// distributeEvents sends the provided events to all registered handlers.
func (c *derivedCollection[I, O]) distributeEvents(events []Event[O], initialSync bool) {
	for h := range c.registeredHandlers {
		h.send(events, initialSync)
	}
}

func (c *derivedCollection[I, O]) GetKey(k string) *O {
	c.mut.RLock()
	defer c.mut.RUnlock()
	result, ok := c.outputs[key[O](k)]
	if !ok {
		return nil
	}
	return &result
}

func (c *derivedCollection[I, O]) List() []O {
	c.mut.RLock()
	defer c.mut.RUnlock()
	return slices.Collect(maps.Values(c.outputs))
}

func (c *derivedCollection[I, O]) Register(f func(o Event[O])) cache.ResourceEventHandlerRegistration {
	return c.RegisterBatched(func(events []Event[O]) {
		for _, ev := range events {
			f(ev)
		}
	}, true)
}

func (c *derivedCollection[I, O]) RegisterBatched(f func(o []Event[O]), runExistingState bool) cache.ResourceEventHandlerRegistration {
	p := newRegistrationHandler(f)

	c.registrationWg.Add(1)
	go func() {
		defer c.registrationWg.Done()
		p.run()
	}()

	c.registeredHandlers[p] = struct{}{}

	if !runExistingState {
		return alwaysSynced{}
	}

	// block any event processing so we can collect a consistent snapshot of outputs representing our 'parentReg' state.
	c.mut.Lock()
	defer c.mut.Unlock()

	events := make([]Event[O], 0, len(c.outputs))
	for _, o := range c.outputs {
		events = append(events, Event[O]{
			New:   &o,
			Event: EventAdd,
		})
	}

	p.send(events, true)

	return p
}

func (c *derivedCollection[I, O]) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, c.HasSynced)
}

func (c *derivedCollection[I, O]) HasSynced() bool {
	if c.registrantsSynced == nil {
		return c.parent.HasSynced()
	}
	return c.registrantsSynced.HasSynced()
}

func (c *derivedCollection[I, O]) index(e KeyExtractor[O]) indexer[O] {
	idx := newIndex(c, e, func(oKeys map[key[O]]struct{}) []O {
		c.mut.RLock()
		defer c.mut.RUnlock()
		result := make([]O, 0, len(oKeys))
		for oKey := range oKeys {
			result = append(result, c.outputs[oKey])
		}
		return result
	})

	c.idxMut.Lock()
	defer c.idxMut.Unlock()
	c.indices = append(c.indices, idx)
	return idx
}
