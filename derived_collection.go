package krtlite

import (
	"github.com/kalexmills/krt-lite/bimap"
	"github.com/kalexmills/krt-lite/fifo"
	"iter"
	"k8s.io/utils/ptr"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// BufferSize is used to preallocate buffers for all unbounded queues used in krt-lite. Take care to set this
// once before creating any collections to avoid data races. Must be a power of 2.
var BufferSize = 1024

// Map creates a new Collection by calling the provided [Mapper] on each item in c. The result Collection will be kept
// in sync with c -- every event from c triggers the handler to update the corresponding item in the result.
// Collection.
//
// By default, if running the Mapper results in an identical object, no update event will be sent to downstream
// collections. Passing [WithSpuriousUpdates] disables this behavior.
//
// Panics will occur if an unsupported type for I or O is used, see [GetKey] for details.
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

// FlatMap creates a new Collection by calling the provided [FlatMapper] on each item in c. FlatMap allows mapping each
// item in a collection to zero or more items. The result Collection is kept in sync with c. See [Map] for details.
//
// By default, update events are not retriggered when FlatMapper produces identical objects for the same key.
// Passing [WithSpuriousUpdates] disables this behavior.
//
// Panics will occur if an unsupported type of I or O are used, see [GetKey] for details.
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
	regIdx             uint64
	registeredHandlers map[uint64]*registrationHandler[O]

	syncedCh chan struct{}
	syncer   *multiSyncer

	collectionDependencies map[uint64]struct{}      // keyed list of collections w/ dependencies added via fetch
	dependencies           map[key[I]][]*dependency // dependencies by input key

	depMaps map[uint64]*bimap.BiMap[key[I], string] // depMaps stores dependency maps for each dependency.

	taskQueue *fifo.Queue[task]
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
		registeredHandlers: make(map[uint64]*registrationHandler[O]),

		taskQueue: fifo.NewQueue[task](BufferSize),

		collectionDependencies: make(map[uint64]struct{}),
		dependencies:           make(map[key[I]][]*dependency),
		depMaps:                make(map[uint64]*bimap.BiMap[key[I], string]),

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

type task func()

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

	// add registration handler
	c.regHandlerMut.Lock()
	defer c.regHandlerMut.Unlock()
	p.unregister = c.unregisterFunc(c.regIdx)
	c.registeredHandlers[c.regIdx] = p
	c.regIdx++

	// start syncer + cleanup handler
	go func() {
		if runExistingState {
			c.WaitUntilSynced(c.stop) // wait for collection to sync before snapshotting and sending initial state
			p.send(c.snapshotInitialState(), true)
		}

		// wait until stopped and unregister all registered handlers, so they can clean up
		<-c.stop

		// reg.Unregister also locks c.registeredHandlers in a callback. To avoid deadlocks and data races, we iterate over
		// a copy.
		for _, reg := range c.copyHandlerList() {
			reg.Unregister()
		}
	}()

	if !runExistingState {
		p.markSynced()
	}

	go p.run()

	return p
}

// handlers provides a copy of all registered handlers.
func (c *derivedCollection[I, O]) copyHandlerList() []*registrationHandler[O] {
	c.regHandlerMut.RLock()
	defer c.regHandlerMut.RUnlock()
	return slices.Collect(maps.Values(c.registeredHandlers))
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
		c.logger().Error("parent registration failed to sync, this collection will never sync")
		return
	}
	c.logger().Debug("parent synced")

	parentReg := c.parent.RegisterBatched(func(evs []Event[I]) {
		c.submitTask(func() {
			c.handleEvents(evs)
		})
	}, true)

	// parent registration will push to the queue, so the queue must be running before we wait for registration to sync.
	go c.taskQueue.Run(c.stop)

	if !parentReg.WaitUntilSynced(c.stop) {
		c.logger().Error("parent registration never synced, this collection will never sync")
		return
	}
	c.logger().Debug("parent registration synced")

	// registration is synced so parent has sent all initial events -- mark ourselves as synced after processing all input
	// items
	c.submitTask(func() {
		close(c.syncedCh)
		c.logger().Debug("collection has synced", "parentName", c.parent.getName())
	})
	c.processTaskQueue()
}

func (c *derivedCollection[I, O]) submitTask(task task) {
	c.taskQueue.In() <- task
}

func (c *derivedCollection[I, O]) pushFetchEvents(d *dependency, events []Event[any]) {
	c.submitTask(func() {
		c.handleFetchEvents(d, events)
	})
}

// processTaskQueue processes items submitted to the task queue in order.
func (c *derivedCollection[I, O]) processTaskQueue() {
	for {
		select {
		case <-c.stop:
			c.logger().Info("stopping task queue")
			return
		case t := <-c.taskQueue.Out():
			t()
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
		if input.Type == EventDelete {
			continue
		}
		i := input.Latest()
		iKey := getTypedKey(i)

		pendingContexts[iKey] = newKontext[I, O](c, iKey)
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
		if input.Type == EventDelete {
			for oKey := range c.mappings[iKey] {
				old, ok := c.outputs[oKey]
				if !ok {
					continue
				}
				outputEvents = append(outputEvents, Event[O]{
					Type: EventDelete,
					Old:  &old,
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
					if !c.wantSpuriousUpdates && reflect.DeepEqual(newRes, oldRes) { // TODO: avoid reflection if possible
						continue
					}
					ev.Type = EventUpdate
					ev.New = &newRes
					ev.Old = &oldRes
					c.outputs[key] = newRes
				} else if newOK {
					ev.Type = EventAdd
					ev.New = &newRes
					c.outputs[key] = newRes
				} else {
					ev.Type = EventDelete
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
	for _, h := range c.registeredHandlers {
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
				Type: EventUpdate,
				New:  iObj,
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
				Type: EventDelete,
				Old:  ptr.To(c.inputs[iKey]),
			}
			res = append(res, e)
		}
	}
	c.handleEvents(res)
}

func (c *derivedCollection[I, O]) changedKeys(dep *dependency, events []Event[any]) map[key[I]]struct{} {
	result := make(map[key[I]]struct{})
	depMap := c.depMaps[dep.dependencyID]
	for _, ev := range events {
		// search through the depMap for a match to avoid searching the entire collection.
		found := false
		for _, item := range ev.Items() {
			for iKey := range depMap.GetLeft(GetKey[any](item)) {
				found = true
				result[iKey] = struct{}{}
			}
		}
		if found {
			continue
		}

		// if no match is found, check all dependencies across all keys for a match.
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

	return result
}

// dependencyUpdate updates dependency tracking. Caller must hold mut.
func (c *derivedCollection[I, O]) dependencyUpdate(iKey key[I], ktx *kontext[I, O]) {
	c.dependencies[iKey] = ktx.dependencies
	for _, dep := range ktx.dependencies {
		if _, ok := c.depMaps[dep.dependencyID]; !ok {
			c.depMaps[dep.dependencyID] = bimap.New[key[I], string]()
		}
		depMap := c.depMaps[dep.dependencyID]

		// if this key exceeded its max track count, reset tracking information for this key so all dependencies are
		// checked during updates.
		if _, ok := ktx.resetTracking[dep.dependencyID]; ok {
			depMap.RemoveLeft(iKey)
			continue
		}

		for k := range ktx.trackedKeys {
			depMap.Add(iKey, k)
		}
	}
}

// dependencyDelete deletes all dependency tracking for the provided iKey. Caller must hold mut.
func (c *derivedCollection[I, O]) dependencyDelete(iKey key[I]) {
	for _, dep := range c.dependencies[iKey] {
		depID := dep.dependencyID

		c.depMaps[depID].RemoveLeft(iKey)
		if c.depMaps[depID].IsEmpty() {
			delete(c.depMaps, depID)
		}
	}
	delete(c.dependencies, iKey)
}

func (c *derivedCollection[I, O]) snapshotInitialState() []Event[O] {
	c.mut.RLock()
	defer c.mut.RUnlock()

	events := make([]Event[O], 0, len(c.outputs))
	for _, o := range c.outputs {
		events = append(events, Event[O]{
			New:  &o,
			Type: EventAdd,
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

// unregisterFunc must always be called with the c.regHandlerMut held.
func (c *derivedCollection[I, O]) unregisterFunc(idx uint64) func() {
	return func() {
		c.regHandlerMut.Lock()
		defer c.regHandlerMut.Unlock()
		delete(c.registeredHandlers, idx)
	}
}

// registrationHandler handles a [fifo.Queue] of batched output events which are being sent to a registered component.
type registrationHandler[T any] struct {
	parent Collection[T]

	handler func(o []Event[T])
	queue   *fifo.Queue[regQueueItem[T]]

	closeStopOnce *sync.Once
	stopCh        chan struct{}

	closeSyncedOnce *sync.Once
	syncedCh        chan struct{}
	syncer          *multiSyncer

	unregister func()
}

// newRegistrationHandler returns a registration handler nad starts up the internal taskQueue.
func newRegistrationHandler[O any](parent Collection[O], handler func(o []Event[O])) *registrationHandler[O] {
	h := &registrationHandler[O]{
		parent:          parent,
		handler:         handler,
		queue:           fifo.NewQueue[regQueueItem[O]](BufferSize),
		stopCh:          make(chan struct{}),
		syncedCh:        make(chan struct{}),
		closeSyncedOnce: &sync.Once{},
		closeStopOnce:   &sync.Once{},
	}

	h.syncer = newMultiSyncer(
		parent,
		channelSyncer{synced: h.syncedCh},
	)

	return h
}

type regQueueItem[T any] struct {
	initialEventsSent bool
	events            []Event[T]
}

func (p *registrationHandler[T]) Unregister() {
	p.unregister()
	p.closeStopOnce.Do(func() {
		close(p.stopCh)
	})
	p.parent.logger().Debug("unregistered registration handler")
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
	case p.queue.In() <- regQueueItem[T]{events: os}:
	case <-p.stopCh:
		p.parent.logger().Info("stopping registration handler")
		return
	}
	if !isInInitialList {
		return
	}

	// signal that we've received all initial events
	select {
	case <-p.syncedCh: // if syncedCh is closed then we're synced -- there's nothing else to do.
		return
	default:
		select {
		// parent is synced once we have received our initial set of events.
		case p.queue.In() <- regQueueItem[T]{initialEventsSent: true}:
		case <-p.stopCh:
			p.parent.logger().Info("stopping registration handler")
			return
		}
	}
}

func (p *registrationHandler[T]) run() {
	go p.queue.Run(p.stopCh)
	for {
		select {
		case fromQueue, ok := <-p.queue.Out():
			if !ok {
				return
			}
			if fromQueue.initialEventsSent {
				p.markSynced()
				continue
			}

			if len(fromQueue.events) > 0 {
				p.handler(fromQueue.events)
			}
		case <-p.stopCh:
			p.parent.logger().Debug("stopping registration handler")
			return
		}
	}
}

// setFromSeq forms a set from an iter.Seq.
func setFromSeq[T comparable](seq iter.Seq[T]) map[T]struct{} {
	result := make(map[T]struct{})
	seq(func(t T) bool {
		result[t] = struct{}{}
		return true
	})
	return result
}
