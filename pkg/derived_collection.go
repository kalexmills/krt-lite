package pkg

import (
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"k8s.io/client-go/tools/cache"
	"maps"
	"slices"
	"sync"
)

// Map creates a new collection by mapping each I into an O.
//
// Panics will occur if an unsupported type for I or O are used, see GetKey for details.
func Map[I, O any](c Collection[I], f Mapper[I, O], opts ...CollectorOption) IndexableCollection[O] {
	ff := func(i I) []O {
		res := f(i)
		if res == nil {
			return nil
		}
		return []O{*res}
	}
	return FlatMap(c, ff, opts...)
}

// FlatMap creates a new collection by mapping every I into zero or more O.
//
// Panics will occur if an unsupported type of I or O are used, see GetKey for details.
func FlatMap[I, O any](c Collection[I], f FlatMapper[I, O], opts ...CollectorOption) IndexableCollection[O] {
	res := newDerivedCollection(c, f, opts)
	go res.run()
	return res
}

// derivedCollection implements a collection whose contents are computed based on the contents of other collections.
// Dependencies between collections are tracked to ensure updates are propagated properly.
type derivedCollection[I, O any] struct {
	collectorMeta

	parent Collection[I]

	transformer FlatMapper[I, O]

	mut      *sync.RWMutex // mut protects inputs, outputs, and mappings
	inputs   map[key[I]]I
	outputs  map[key[O]]O
	mappings map[key[I]]map[key[O]]struct{}

	idxMut  *sync.RWMutex // idxMut protects indices.
	indices []*mapIndex[O]

	regHandlerMut      *sync.RWMutex // regHandlerMut protects registeredHandlers.
	registeredHandlers map[*registrationHandler[Event[O]]]struct{}

	stop      chan struct{}
	parentReg cache.ResourceEventHandlerRegistration

	markSynced *sync.Once
	syncedCh   chan struct{}
	syncer     *multiSyncer

	inputQueue *fifo.Queue[any] // inputQueue entries will always be either eventParentIsSynced or []Event[I]
}

func newDerivedCollection[I, O any](c Collection[I], f FlatMapper[I, O], opts []CollectorOption) *derivedCollection[I, O] {
	result := &derivedCollection[I, O]{
		collectorMeta: newCollectorMeta(opts),
		parent:        c,
		transformer:   f,

		outputs:  make(map[key[O]]O),
		inputs:   make(map[key[I]]I),
		mappings: make(map[key[I]]map[key[O]]struct{}),
		mut:      &sync.RWMutex{},
		idxMut:   &sync.RWMutex{},

		regHandlerMut:      &sync.RWMutex{},
		registeredHandlers: make(map[*registrationHandler[Event[O]]]struct{}),

		markSynced: &sync.Once{},
		stop:       make(chan struct{}),
		inputQueue: fifo.NewQueue[any](1024),

		syncedCh: make(chan struct{}),
	}
	result.syncer = &multiSyncer{
		syncers: []cache.InformerSynced{
			c.HasSynced,
			channelSyncer{synced: result.syncedCh}.HasSynced,
		}}
	return result
}

var _ Collection[any] = &derivedCollection[int, any]{}

func (c *derivedCollection[I, O]) run() {
	if !c.parent.WaitUntilSynced(c.stop) {
		return
	}
	c.parentReg = c.parent.RegisterBatched(c.pushInputEvents, true)

	// wait for parent to sync before running.
	if !cache.WaitForCacheSync(c.stop, c.parent.HasSynced) {
		return // TODO: a noisy error
	}

	go c.inputQueue.Run(c.stop)

	c.processInputQueue()
}

func (c *derivedCollection[I, O]) pushInputEvents(events []Event[I]) {
	c.inputQueue.In() <- events
	c.markSynced.Do(func() {
		c.inputQueue.In() <- eventParentIsSynced{}
	})
}

func (c *derivedCollection[I, O]) processInputQueue() {
	for {
		select {
		case <-c.stop:
			return
		case input := <-c.inputQueue.Out():
			if _, ok := input.(eventParentIsSynced); ok {
				close(c.syncedCh)
				continue
			}
			events := input.([]Event[I])
			c.handleEvents(events)
		}
	}
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
					//if reflect.DeepEqual(newRes, oldRes) { // TODO: avoid reflection if possible
					//	continue
					//}
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

	// note; we still hold the lock, which guarantees events are distributed in order.
	c.distributeEvents(outputEvents, !c.HasSynced())
}

// distributeEvents sends the provided events to all downstream listeners.
func (c *derivedCollection[I, O]) distributeEvents(events []Event[O], initialSync bool) {
	// update indexes before handlers, so handlers can rely on indexes being computed.
	c.idxMut.RLock()
	for _, idx := range c.indices {
		idx.handleEvents(events)
	}
	c.idxMut.RUnlock()

	c.regHandlerMut.RLock()
	defer c.regHandlerMut.RUnlock()
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
	p := newRegistrationHandler(c, f)

	c.regHandlerMut.Lock()
	defer c.regHandlerMut.Unlock()
	c.registeredHandlers[p] = struct{}{}

	if !runExistingState {
		p.markSynced()
		go p.run()
		return p
	}

	go func() {
		c.WaitUntilSynced(c.stop) // wait for parent to sync before snapshotting and sending initial state
		p.send(c.snapshotInitialState(), true)
	}()

	go p.run()

	return p
}

func (c *derivedCollection[I, O]) snapshotInitialState() []Event[O] {
	c.mut.RLock()
	defer c.mut.RUnlock()

	events := make([]Event[O], 0, len(c.outputs))
	for _, o := range c.outputs {
		events = append(events, Event[O]{
			New:   &o,
			Event: EventAdd,
		})
	}
	return events
}

func (c *derivedCollection[I, O]) WaitUntilSynced(stop <-chan struct{}) bool {
	return c.syncer.WaitUntilSynced(stop)
}

func (c *derivedCollection[I, O]) HasSynced() bool {
	return c.syncer.HasSynced()
}

func (c *derivedCollection[I, O]) Index(e KeyExtractor[O]) Index[O] {
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

// newRegistrationHandler returns a registration handler, starting up the internal inputQueue.
func newRegistrationHandler[O any](parent Collection[O], f func(o []Event[O])) *registrationHandler[Event[O]] {
	h := &registrationHandler[Event[O]]{
		parentName:     parent.getName(),
		handler:        f,
		queue:          fifo.NewQueue[any](1024),
		stopCh:         make(chan struct{}),
		syncedCh:       make(chan struct{}),
		sendSyncedOnce: &sync.Once{},
	}
	h.syncer = &multiSyncer{
		syncers: []cache.InformerSynced{
			parent.HasSynced,
			channelSyncer{synced: h.syncedCh}.HasSynced,
		},
	}

	return h
}

// eventParentIsSynced is sent when a queue has processed all of its initial input events.
type eventParentIsSynced struct{}

// registrationHandler handles a fifo.Queue of batched output events which are being sent to a registered component.
type registrationHandler[T any] struct {
	parentName string

	handler func(o []T)
	// each entry will be either []Event[O] or eventParentIsSynced{}
	queue *fifo.Queue[any]

	stopCh chan struct{}

	sendSyncedOnce *sync.Once
	syncedCh       chan struct{}
	syncer         *multiSyncer
}

func (p *registrationHandler[T]) markSynced() {
	close(p.syncedCh)
}

// HasSynced is true when the registrationHandler is synced.
func (p *registrationHandler[T]) HasSynced() bool {
	return p.syncer.HasSynced()
}

func (p *registrationHandler[T]) send(os []T, isInInitialList bool) {
	select {
	case <-p.stopCh:
		return
	case p.queue.In() <- os:
	}
	if !isInInitialList {
		return
	}

	select {
	case <-p.syncedCh:
		return
	default:
		select {
		case <-p.stopCh:
			return
		case p.queue.In() <- eventParentIsSynced{}:
		}
	}
}

func (p *registrationHandler[O]) run() {
	go p.queue.Run(p.stopCh)
	for {
		select {
		case <-p.stopCh:
			return
		case fromQueue, ok := <-p.queue.Out():
			if !ok {
				return
			}
			if _, ok := fromQueue.(eventParentIsSynced); ok {
				p.markSynced()
				continue
			}
			next := fromQueue.([]O)
			if len(next) > 0 {
				p.handler(next)
			}
		}
	}
}
