package pkg

import (
	"github.com/kalexmills/krt-lite/pkg/bimap"
	"github.com/kalexmills/krt-lite/pkg/fifo"
	"k8s.io/utils/ptr"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// Map creates a new Collection by calling the provided Mapper on each item in C. The returned Collection will be kept
// in sync with c -- every event from c triggers the handler to update the corresponding item in the returned
// Collection.
//
// Panics will occur if an unsupported type for I or T is used, see GetKey for details.
func Map[I, O any](c Collection[I], handler Mapper[I, O], opts ...CollectionOption) IndexableCollection[O] {
	ff := func(ctx Context, i I) []O {
		res := handler(ctx, i)
		if res == nil {
			return nil
		}
		return []O{*res}
	}
	return FlatMap(c, ff, opts...)
}

// FlatMap creates a new Collection by calling the provided FlatMapper on each item in C. Unlike Map, each item in
// Collection c may result in zero or more items in the returned Collection. The returned Collection is kept in sync
// with c. See Map for details.
//
// Panics will occur if an unsupported type of I or T are used, see GetKey for details.
func FlatMap[I, O any](c Collection[I], f FlatMapper[I, O], opts ...CollectionOption) IndexableCollection[O] {
	res := newDerivedCollection(c, f, opts)
	return res
}

// derivedCollection implements a collection whose contents are computed based on the contents of other collections.
// Dependencies between collections are tracked to ensure updates are propagated properly.
type derivedCollection[I, O any] struct {
	collectionShared

	parent Collection[I]

	transformer FlatMapper[I, O]

	mut      *sync.RWMutex // mut protects inputs, outputs, and mappings
	inputs   map[key[I]]I
	outputs  map[key[O]]O
	mappings map[key[I]]map[key[O]]struct{}

	idxMut  *sync.RWMutex // idxMut protects indices.
	indices []*mapIndex[O]

	regHandlerMut      *sync.RWMutex // regHandlerMut protects registeredHandlers.
	registeredHandlers map[*registrationHandler[O]]struct{}

	parentReg Syncer

	markSynced *sync.Once
	syncedCh   chan struct{}
	syncer     *multiSyncer

	collectionDependencies map[uint64]struct{}      // keyed list of collections w/ dependencies added via fetch
	dependencies           map[key[I]][]*dependency // dependencies by input key

	depMap *bimap.BiMap[key[I], depKey] // depMap maps between input keys and keys from Fetch dependencies.

	inputQueue *fifo.Queue[inputEvent[I]]
}

type depKey struct {
	depID uint64
	key   string
}

func newDerivedCollection[I, O any](parent Collection[I], f FlatMapper[I, O], opts []CollectionOption) *derivedCollection[I, O] {
	c := &derivedCollection[I, O]{
		collectionShared: newCollectionShared(opts),
		parent:           parent,
		transformer:      f,

		outputs:  make(map[key[O]]O),
		inputs:   make(map[key[I]]I),
		mappings: make(map[key[I]]map[key[O]]struct{}),
		mut:      &sync.RWMutex{},
		idxMut:   &sync.RWMutex{},

		regHandlerMut:      &sync.RWMutex{},
		registeredHandlers: make(map[*registrationHandler[O]]struct{}),

		markSynced: &sync.Once{},
		inputQueue: fifo.NewQueue[inputEvent[I]](1024),

		collectionDependencies: make(map[uint64]struct{}),
		dependencies:           make(map[key[I]][]*dependency),
		depMap:                 bimap.New[key[I], depKey](),

		syncedCh: make(chan struct{}),
	}
	c.syncer = newMultiSyncer(
		parent,
		&channelSyncer{synced: c.syncedCh},
	)
	go c.run()
	return c
}

var _ Collection[any] = &derivedCollection[int, any]{}

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

func (c *derivedCollection[I, O]) Register(f func(o Event[O])) Registration {
	return c.RegisterBatched(func(events []Event[O]) {
		for _, ev := range events {
			f(ev)
		}
	}, true)
}

func (c *derivedCollection[I, O]) RegisterBatched(f func(o []Event[O]), runExistingState bool) Registration {
	p := newRegistrationHandler(c, f)
	p.unregister = c.unregisterFunc(p)

	c.regHandlerMut.Lock()
	defer c.regHandlerMut.Unlock()
	c.registeredHandlers[p] = struct{}{}

	if !runExistingState {
		p.markSynced()
		go p.run()
		return p
	}

	go func() {
		c.WaitUntilSynced(c.stop) // wait for collection to sync before snapshotting and sending initial state
		p.send(c.snapshotInitialState(), true)
	}()

	go p.run()

	return p
}

func (c *derivedCollection[I, O]) Index(e KeyExtractor[O]) Index[O] {
	idx := newMapIndex(c, e, func(oKeys map[key[O]]struct{}) []O {
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

// run registers this derivedCollection with its parent, starts up the inputQueue, and blocks to process the input
// queue.
func (c *derivedCollection[I, O]) run() {
	c.logger().Debug("waiting for parent to sync")
	if !c.parent.WaitUntilSynced(c.stop) {
		return
	}
	c.logger().Debug("parent synced")

	c.parentReg = c.parent.RegisterBatched(func(evs []Event[I]) {
		c.inputQueue.In() <- inputEvents(evs)
	}, true)

	// parent registration will push to the queue, so it must be running before we wait for registration to sync.
	go c.inputQueue.Run(c.stop)

	if !c.parentReg.WaitUntilSynced(c.stop) {
		return
	}
	c.logger().Debug("parent registration synced")

	// parent is synced so they must have pushed everything -- mark ourselves as synced once everything has processed
	c.markSynced.Do(func() {
		c.inputQueue.In() <- inputEventParentIsSynced[I]()
	})
	c.processInputQueue()
}

func (c *derivedCollection[I, O]) pushFetchEvents(d *dependency, events []Event[any]) {
	c.inputQueue.In() <- fetchEvents[I](d, events)
}

func (c *derivedCollection[I, O]) processInputQueue() {
	for {
		select {
		case <-c.stop:
			return
		case input := <-c.inputQueue.Out():
			if input.IsParentIsSynced() {
				c.logger().Debug("parent has synced", "parentName", c.parent.getName())
				close(c.syncedCh)
				continue
			}
			if input.IsFetchEvents() {
				c.logger().Debug("received fetch events", "fromCollectionID", input.dependency.collectionID)
				c.handleFetchEvents(input.dependency, input.fetchEvents)
				continue
			}

			c.handleEvents(input.events)
			c.logger().Debug("handled events", "count", len(input.events))
		}
	}
}

// handleEvents handles all input events by computing the corresponding output and dispatching downstream events based
// on any changes to the current state of the collection.
//
// handleEvents does not need to lock for access to several derivedCollection fields, as all calls are executed
// sequentially via the inputQueue.
func (c *derivedCollection[I, O]) handleEvents(inputs []Event[I]) {
	var outputEvents []Event[O]

	recomputed := make([]map[key[O]]O, len(inputs))
	pendingContexts := make(map[key[I]]*kontext[I, O], len(inputs))
	for idx, input := range inputs {
		if input.Event == EventDelete {
			continue
		}
		i := input.Latest()
		iKey := getTypedKey(i)

		pendingContexts[iKey] = &kontext[I, O]{collection: c, key: iKey}
		os := c.transformer(pendingContexts[iKey], input.Latest())
		outmap := make(map[key[O]]O, len(os))
		for _, o := range os {
			outmap[getTypedKey(o)] = o
		}
		recomputed[idx] = outmap
	}

	c.mut.Lock()
	defer c.mut.Unlock()

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
			c.dependencyDelete(iKey)
		} else {
			results := recomputed[idx]
			c.dependencyUpdate(iKey, pendingContexts[iKey])

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
					if reflect.DeepEqual(newRes, oldRes) { // TODO: avoid reflection if possible
						continue
					}
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

func (c *derivedCollection[I, O]) handleFetchEvents(dependency *dependency, events []Event[any]) {
	changedKeys := c.changedKeys(dependency, events)
	if len(changedKeys) == 0 {
		return
	}

	// generate fake events based on updates.

	res := make([]Event[I], 0, len(events))

	deletions := make([]key[I], 0, len(events))
	for iKey := range changedKeys {
		iObj := c.parent.GetKey(string(iKey))
		if iObj == nil {
			// object was deleted
			deletions = append(deletions, iKey)
		} else {
			// we let handleEvents fetch Old for us.
			res = append(res, Event[I]{
				Event: EventUpdate,
				New:   iObj,
			})
		}
	}

	for _, iKey := range deletions {
		for oKey := range c.mappings[iKey] {
			_, ok := c.outputs[oKey]
			if !ok {
				continue
			}
			e := Event[I]{
				Event: EventDelete,
				Old:   ptr.To(c.inputs[iKey]),
			}
			res = append(res, e)
		}
	}
	c.logger().Debug("added fetch events", "updateCount", len(events), "deletionCount", len(deletions), "events", events)
	c.handleEvents(res)
}

func (c *derivedCollection[I, O]) changedKeys(dep *dependency, events []Event[any]) map[key[I]]struct{} {
	// search through the depMap mapping for an index... if none, check everything.
	result := make(map[key[I]]struct{})
	for _, ev := range events {
		found := false
		for _, item := range ev.Items() {
			key := GetKey[any](item)
			for iKey := range c.depMap.GetUs(depKey{depID: dep.depID, key: key}) {
				found = true
				c.logger().Debug("found a match")
				result[iKey] = struct{}{}
			}
		}
		if !found {
			for iKey, depsForKey := range c.dependencies {
				for _, depForKey := range depsForKey {
					for _, item := range ev.Items() {
						if depForKey.Matches(item) {
							result[iKey] = struct{}{}
						}
					}
				}
			}
		}
	}
	c.logger().Debug("changedKeys", "result", result)
	return result
}

// dependencyUpdate updates dependency state. Caller must hold mut.
func (c *derivedCollection[I, O]) dependencyUpdate(iKey key[I], ktx *kontext[I, O]) {
	c.dependencies[iKey] = ktx.dependencies
	for _, dep := range ktx.dependencies {
		// update tracking by sourceKey
		for _, k := range ktx.trackedKeys {
			c.depMap.Add(iKey, depKey{depID: dep.depID, key: k})
		}
	}
}

// dependencyDelete deletes a dependency. Caller must hold mut.
func (c *derivedCollection[I, O]) dependencyDelete(iKey key[I]) {
	delete(c.dependencies, iKey)
	c.depMap.RemoveU(iKey)
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

func (c *derivedCollection[I, O]) unregisterFunc(reg *registrationHandler[O]) func() {
	return func() {
		c.regHandlerMut.Lock()
		defer c.regHandlerMut.Unlock()
		delete(c.registeredHandlers, reg)
	}
}

// newRegistrationHandler returns a registration handler nad starts up the internal inputQueue.
func newRegistrationHandler[O any](parent Collection[O], handler func(o []Event[O])) *registrationHandler[O] {
	h := &registrationHandler[O]{
		parent:          parent,
		handler:         handler,
		queue:           fifo.NewQueue[any](1024),
		stopCh:          make(chan struct{}),
		syncedCh:        make(chan struct{}),
		closeSyncedOnce: &sync.Once{},
	}
	h.syncer = newMultiSyncer(
		parent,
		channelSyncer{synced: h.syncedCh},
	)

	return h
}

// eventParentIsSynced is sent when a queue has processed all of its initial input events.
type eventParentIsSynced struct{}

// registrationHandler handles a fifo.Queue of batched output events which are being sent to a registered component.
type registrationHandler[T any] struct {
	parent Collection[T]

	handler func(o []Event[T])
	// each entry will be either []Event[T] or eventParentIsSynced{}
	queue *fifo.Queue[any]

	stopCh chan struct{}

	closeSyncedOnce *sync.Once
	syncedCh        chan struct{}
	syncer          *multiSyncer

	unregister func()
}

func (p *registrationHandler[T]) Unregister() {
	p.unregister()
	close(p.stopCh)
}

func (p *registrationHandler[T]) markSynced() {
	p.closeSyncedOnce.Do(func() {
		close(p.syncedCh)
	})
}

func (p *registrationHandler[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return p.syncer.WaitUntilSynced(stop)
}

func (p *registrationHandler[T]) HasSynced() bool {
	return p.syncer.HasSynced()
}

func (p *registrationHandler[T]) send(os []Event[T], isInInitialList bool) {
	select {
	case <-p.stopCh:
		return
	case p.queue.In() <- os:
	}
	if !isInInitialList {
		return
	}

	select { // if we're already synced then return
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
			next := fromQueue.([]Event[O])
			if len(next) > 0 {
				p.handler(next)
			}
		}
	}
}

type inputEvent[I any] struct {
	header      byte
	events      []Event[I]
	dependency  *dependency
	fetchEvents []Event[any] // TODO(perf): lower memory footprint by making it hold thunks.
}

func inputEventParentIsSynced[I any]() inputEvent[I] {
	return inputEvent[I]{header: parentIsSynced}
}

func inputEvents[I any](events []Event[I]) inputEvent[I] {
	return inputEvent[I]{events: events}
}

func fetchEvents[I any](d *dependency, events []Event[any]) inputEvent[I] {
	return inputEvent[I]{header: isFetchEvents, fetchEvents: events, dependency: d}
}

// IsParentIsSynced means this event indicates the parent is synced. No events accompany this message.
func (e *inputEvent[I]) IsParentIsSynced() bool {
	return (e.header & parentIsSynced) > 0
}

// IsFetchEvents means this event contains only fetch events.
func (e *inputEvent[I]) IsFetchEvents() bool {
	return (e.header & isFetchEvents) > 0
}

const (
	parentIsSynced = 1 << iota
	isFetchEvents
)
